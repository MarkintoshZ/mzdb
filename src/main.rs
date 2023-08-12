use tonic::{transport::Server, Request, Response, Status};

use mzdb::node_server::{Node, NodeServer};
use mzdb::{
    get_response, GetRequest, GetResponse, LookupRequest, LookupResponse, SetRequest, SetResponse,
};

pub mod mzdb {
    tonic::include_proto!("mzdb");
}

#[derive(Debug, Default)]
pub struct DBNode {}

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
    let addr = "[::1]:50051".parse()?;
    let greeter = DBNode::default();

    Server::builder()
        .add_service(NodeServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
