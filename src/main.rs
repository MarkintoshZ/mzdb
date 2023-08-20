use std::cmp;
use std::net::SocketAddr;
use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use tokio::{select, signal, sync::Mutex};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use mzdb::node_client::NodeClient;
use mzdb::node_server::{Node, NodeServer};
use mzdb::{
    get_response, GetRequest, GetResponse, LookupRequest, LookupResponse, SetRequest, SetResponse,
    WhothisRequest,
};

/// MZBD server
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// ip address of the node
    addr: SocketAddr,

    /// ip address of the successor node
    successor: SocketAddr,

    /// node number
    number: u64,

    /// 2^m number hash slots
    #[arg(default_value_t = 8)]
    m: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct NodeInfo {
    key: u64,
    addr: SocketAddr,
}

#[derive(Debug, Default)]
struct Storage {
    table: HashMap<String, String>,
}

#[derive(Debug)]
struct Chord {
    info: NodeInfo,
    fingers: Vec<Option<NodeInfo>>,
    sockets: HashMap<SocketAddr, NodeClient<Channel>>,
}

impl Chord {
    fn new(node: NodeInfo, m: u64) -> Self {
        Self {
            info: node,
            fingers: (0..m).map(|_| None).collect(),
            sockets: HashMap::new(),
        }
    }

    fn lookup(&self, key: u64) -> Option<NodeInfo> {
        assert!(self.fingers.first().is_some());
        assert!(key < 2u64.pow(self.fingers.len() as u32));

        // calculate the successor node index
        let m = self.fingers.len() as u32;
        let dist = (key - self.info.key).rem_euclid(2u64.pow(m)) as usize;
        let mut index = cmp::min(dist.ilog2(), m - 1);

        // find the farthest node that precedes the index
        loop {
            if let Some(node) = self.fingers.get(index as usize).unwrap() {
                return Some(node.clone());
            }
            if index == 0 {
                break;
            }
            index -= 1;
        }
        None
    }

    fn lookup_conn(&self, key: u64) -> Option<NodeClient<Channel>> {
        self.lookup(key)
            .and_then(|node| self.sockets.get(&node.addr).cloned())
    }

    fn successor_node(&self) -> Option<&NodeInfo> {
        self.fingers.first().unwrap().as_ref()
    }

    fn set_finger(&mut self, i: usize, node: NodeInfo, client: NodeClient<Channel>) {
        let addr = node.addr.clone();
        self.fingers[i] = Some(node);
        self.sockets.insert(addr, client);
    }

    async fn create_conn(addr: &String) -> Option<NodeClient<Channel>> {
        let endpoint = format!("http://{}", addr);
        match NodeClient::connect(endpoint).await {
            Ok(client) => Some(client),
            Err(e) => {
                println!("Client Request Error: {}", e);
                None
            }
        }
    }
}

impl Into<NodeInfo> for LookupResponse {
    fn into(self) -> NodeInfo {
        let addr = self.addr.parse().unwrap();
        NodeInfo {
            key: self.key,
            addr,
        }
    }
}

pub mod mzdb {
    tonic::include_proto!("mzdb");
}

#[derive(Debug)]
pub struct NodeService {
    info: NodeInfo,
    storage: Arc<Mutex<Storage>>,
    chord: Arc<Mutex<Chord>>,
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn whothis(
        &self,
        request: Request<WhothisRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = LookupResponse {
            key: self.info.key,
            addr: self.info.addr.to_string(),
        };
        Ok(Response::new(reply))
    }

    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        println!("Got a request: {:?}", request);

        let request = request.into_inner();
        let reply = if request.relay {
            let mut chord_guard = self.chord.lock().await;
            let next_hop_node = chord_guard.lookup(request.key).unwrap();
            let relay_req = LookupRequest {
                key: request.key,
                relay: next_hop_node != *chord_guard.successor_node().unwrap(),
            };
            chord_guard
                .sockets
                .get_mut(&next_hop_node.addr)
                .unwrap()
                .lookup(relay_req)
                .await
                .expect("failed to get successor")
                .into_inner()
        } else {
            LookupResponse {
                key: self.info.key,
                addr: self.info.addr.to_string(),
            }
        };
        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = GetResponse {
            response: Some(get_response::Response::Value("".as_bytes().into())),
        };

        Ok(Response::new(reply))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = SetResponse { success: true };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // parse cmd args
    let args = Args::parse();
    let self_info = NodeInfo {
        key: args.number,
        addr: args.addr,
    };
    let storage = Arc::new(Mutex::new(Storage::default()));
    let chord = Arc::new(Mutex::new(Chord::new(self_info.clone(), args.m)));

    println!("Starting node: {:?}", self_info);

    // start main process
    {
        let chord = chord.clone();
        tokio::task::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            // connect to successor
            let mut successor = Chord::create_conn(&args.successor.to_string())
                .await
                .expect("failed to connect to successor");
            let successor_node = successor.whothis(WhothisRequest {}).await.unwrap();
            chord
                .lock()
                .await
                .set_finger(0, successor_node.into_inner().into(), successor);
            dbg!(&chord.lock().await.fingers);

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            // build finger table
            let finger_idx = (0..args.m)
                .map(|i| 2u64.pow(i as u32))
                .map(|i| (i + self_info.key) % 2u64.pow(args.m as u32));

            for (i, curr_key) in finger_idx.enumerate().skip(1) {
                let mut chord_guard = chord.lock().await;
                if let Some(suc) = chord_guard.lookup(curr_key) {
                    let prev_key = curr_key / 2;
                    if (prev_key < suc.key && curr_key <= suc.key)
                        || (prev_key > suc.key && (curr_key <= suc.key || curr_key > prev_key))
                    {
                        chord_guard.fingers[i] = Some(suc);
                    } else {
                        let mut successor = chord_guard.lookup_conn(curr_key).unwrap();
                        let successor_node = successor
                            .lookup(LookupRequest {
                                key: curr_key,
                                relay: true,
                            })
                            .await
                            .unwrap()
                            .into_inner();
                        let successor_conn =
                            Chord::create_conn(&successor_node.addr).await.unwrap();
                        chord_guard.set_finger(i, successor_node.into(), successor_conn);
                    }
                }
                drop(chord_guard);
            }
            chord
                .lock()
                .await
                .fingers
                .iter()
                .enumerate()
                .for_each(|(i, f)| {
                    println!("{}: {:?}", i, f);
                });
        });
    }

    // start gRPC server
    let node = NodeService {
        info: self_info,
        storage: storage.clone(),
        chord: chord.clone(),
    };
    if let Err(e) = Server::builder()
        .add_service(NodeServer::new(node))
        .serve(args.addr)
        .await
    {
        println!("Error: {}", e);
    }

    Ok(())
}
