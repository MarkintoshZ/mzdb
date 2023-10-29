use std::cmp;
use std::collections::HashMap;
use std::net::SocketAddr;

use tonic::transport::Channel;

use crate::mzdb::node_client::NodeClient;
use crate::node_info::NodeInfo;

#[derive(Debug)]
pub struct Chord {
    info: NodeInfo,
    pub fingers: Vec<Option<NodeInfo>>,
    pub sockets: HashMap<SocketAddr, NodeClient<Channel>>,
}

impl Chord {
    pub fn new(node: NodeInfo, m: u64) -> Self {
        Self {
            info: node,
            fingers: (0..m).map(|_| None).collect(),
            sockets: HashMap::new(),
        }
    }

    pub fn lookup(&self, key: u64) -> Option<NodeInfo> {
        assert!(self.fingers.first().is_some());
        assert!(key < 2u64.pow(self.fingers.len() as u32));

        // calculate the successor node index
        let m = self.fingers.len() as u32;
        let dist: u64 = if key > self.info.key_slot {
            key - self.info.key_slot
        } else {
            2u64.pow(m) - self.info.key_slot - key
        };
        if dist == 0 {
            Some(self.info.clone())
        } else {
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
    }

    pub fn lookup_conn(&self, key: u64) -> Option<NodeClient<Channel>> {
        self.lookup(key)
            .and_then(|node| self.sockets.get(&node.addr).cloned())
    }

    pub fn successor_node(&self) -> Option<&NodeInfo> {
        self.fingers.first().unwrap().as_ref()
    }

    pub fn set_finger(&mut self, i: usize, node: NodeInfo, client: NodeClient<Channel>) {
        let addr = node.addr;
        self.fingers[i] = Some(node);
        self.sockets.insert(addr, client);
    }

    pub async fn create_conn(addr: &String) -> Option<NodeClient<Channel>> {
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

