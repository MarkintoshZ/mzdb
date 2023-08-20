use crate::chord::Chord;
use crate::mzdb::node_server::Node;
use crate::mzdb::{
    get_response, GetRequest, GetResponse, LookupRequest, LookupResponse, SetRequest, SetResponse,
    WhothisRequest,
};
use crate::node_info::NodeInfo;
use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct NodeService {
    pub info: NodeInfo,
    pub storage: Arc<Mutex<Storage>>,
    pub chord: Arc<Mutex<Chord>>,
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
