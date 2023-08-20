use crate::chord::Chord;
use crate::mzdb::node_server::Node;
use crate::mzdb::{
    get_response, GetRequest, GetResponse, LookupRequest, LookupResponse, SetRequest, SetResponse,
    WhoisRequest,
};
use crate::node_info::NodeInfo;
use crate::storage::Storage;

use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug)]
pub struct NodeService {
    pub m: u64,
    pub info: NodeInfo,
    pub storage: Arc<Mutex<Storage>>,
    pub chord: Arc<Mutex<Chord>>,
}

#[tonic::async_trait]
impl Node for NodeService {
    async fn whothis(
        &self,
        request: Request<WhoisRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        debug!("Received request: {:?}", request);

        let reply = LookupResponse {
            key_slot: self.info.key_slot,
            addr: self.info.addr.to_string(),
        };
        Ok(Response::new(reply))
    }

    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        debug!("Received request: {:?}", request);

        let request = request.into_inner();
        let reply = if request.relay {
            let mut chord_guard = self.chord.lock().await;
            let next_hop_node = chord_guard.lookup(request.key_slot).unwrap();
            let relay_req = LookupRequest {
                key_slot: request.key_slot,
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
                key_slot: self.info.key_slot,
                addr: self.info.addr.to_string(),
            }
        };
        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        debug!("Received request: {:?}", request);

        let request = request.into_inner();
        let key_slot = hash(&request.key, self.m);
        let reply = if request.relay {
            let mut chord_guard = self.chord.lock().await;
            let next_hop_node = chord_guard.lookup(key_slot).unwrap();
            let relay_req = GetRequest {
                key: request.key,
                relay: next_hop_node != *chord_guard.successor_node().unwrap(),
            };
            chord_guard
                .sockets
                .get_mut(&next_hop_node.addr)
                .unwrap()
                .get(relay_req)
                .await
                .expect("failed to get successor")
                .into_inner()
        } else {
            match self.storage.lock().await.table.get(&request.key) {
                Some(value) => GetResponse {
                    response: Some(get_response::Response::Value(value.clone())),
                },
                None => GetResponse {
                    response: Some(get_response::Response::Error("Key not found".to_string())),
                },
            }
        };
        Ok(Response::new(reply))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        debug!("Received request: {:?}", request);

        let request = request.into_inner();
        let key_slot = hash(&request.key, self.m);
        let reply = if request.relay {
            let mut chord_guard = self.chord.lock().await;
            let next_hop_node = chord_guard.lookup(key_slot).unwrap();
            let relay_req = SetRequest {
                key: request.key,
                value: request.value,
                relay: next_hop_node != *chord_guard.successor_node().unwrap(),
            };
            chord_guard
                .sockets
                .get_mut(&next_hop_node.addr)
                .unwrap()
                .set(relay_req)
                .await
                .expect("failed to get successor")
                .into_inner()
        } else {
            self.storage
                .lock()
                .await
                .table
                .insert(request.key, request.value);
            SetResponse { success: true }
        };
        Ok(Response::new(reply))
    }
}

fn hash<T: Hash>(t: &T, m: u64) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() % 2u64.pow(m as u32)
}
