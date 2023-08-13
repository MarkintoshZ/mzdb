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

#[derive(Debug)]
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
    fingers: Vec<Option<NodeInfo>>,
    sockets: HashMap<SocketAddr, NodeClient<Channel>>,
}

impl Chord {
    fn new(m: u64) -> Self {
        Self {
            fingers: (0..m).map(|_| None).collect(),
            sockets: HashMap::new(),
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

        let reply = LookupResponse {
            key: self.info.key,
            addr: self.info.addr.to_string(),
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
    let chord = Arc::new(Mutex::new(Chord::new(args.m)));

    println!("Starting node: {:?}", self_info);

    // start main process
    tokio::task::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        match NodeClient::connect(dbg!(format!("http://{}", args.successor))).await {
            Ok(mut successor) => {
                let request = tonic::Request::new(WhothisRequest {});
                let response = successor.whothis(request).await;
                println!("Response = {:?}", response);
            }
            Err(e) => {
                println!("Client Request Error: {}", e);
            }
        }
    });

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
