use crate::mzdb::LookupResponse;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub key_slot: u64,
    pub addr: SocketAddr,
}

impl From<LookupResponse> for NodeInfo {
    fn from(val: LookupResponse) -> Self {
        let addr = val.addr.parse().unwrap();
        NodeInfo {
            key_slot: val.key_slot,
            addr,
        }
    }
}
