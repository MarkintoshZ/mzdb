use std::collections::HashMap;
use std::net::SocketAddr;

use clap::Parser;
use tonic::{transport::Server, Request, Response, Status};

use mzdb::node_server::{Node, NodeServer};
use mzdb::{
    get_response, GetRequest, GetResponse, LookupRequest, LookupResponse, SetRequest, SetResponse,
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

    /// 2^k number hash slots
    #[arg(default_value_t = 8)]
    k: u64,
}

#[derive(Debug)]
struct NodeInfo {
    number: u64,
    addr: SocketAddr,
}

#[derive(Debug, Default)]
struct Storage {
    table: HashMap<String, String>,
}

#[derive(Debug)]
struct NodeState {
    successor: SocketAddr,
    fingers: Vec<NodeInfo>,
    sockets: HashMap<i64, String>,
    storage: Storage,
}

pub mod mzdb {
    tonic::include_proto!("mzdb");
}

#[derive(Debug)]
pub struct DBNode {
    state: NodeState,
}

impl DBNode {
    fn new(state: NodeState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Node for DBNode {
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = LookupResponse {
            key: "hi".to_string(),
            addr: "".to_string(),
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
    let node_state = NodeState {
        successor: args.successor,
        fingers: Vec::new(),
        sockets: HashMap::default(),
        storage: Storage::default(),
    };

    // start gRPC server
    let node = DBNode::new(node_state);

    Server::builder()
        .add_service(NodeServer::new(node))
        .serve(args.addr)
        .await?;

    Ok(())
}
