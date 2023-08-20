#[macro_use]
extern crate log;

mod args;
mod chord;
mod node_info;
mod node_service;
mod routine;
mod storage;
mod mzdb {
    #![allow(non_snake_case)]
    tonic::include_proto!("mzdb");
}

use args::Args;
use chord::Chord;
use mzdb::node_server::NodeServer;
use node_info::NodeInfo;
use node_service::NodeService;
use storage::Storage;

use clap::Parser;
use tokio::sync::Mutex;
use tonic::transport::Server;

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // parse cmd args
    let args = Args::parse();
    let self_info = NodeInfo {
        key: args.number,
        addr: args.addr,
    };
    let storage = Arc::new(Mutex::new(Storage::default()));
    let chord = Arc::new(Mutex::new(Chord::new(self_info.clone(), args.m)));

    // start main process
    info!("Starting main routine");
    {
        tokio::task::spawn(routine::start(
            self_info.clone(),
            chord.clone(),
            args.clone(),
        ));
    }

    // start gRPC server
    info!("Starting gRPC server");
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
