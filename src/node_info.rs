use crate::mzdb::LookupResponse;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub key: u64,
    pub addr: SocketAddr,
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
