use clap::Parser;
use std::net::SocketAddr;

/// MZBD server
#[derive(Parser, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// ip address of the node
    pub addr: SocketAddr,

    /// ip address of the successor node
    pub successor: SocketAddr,

    /// node number
    pub number: u64,

    /// 2^m number hash slots
    #[arg(default_value_t = 8)]
    pub m: u64,
}
