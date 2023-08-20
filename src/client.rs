mod mzdb {
    #![allow(non_snake_case)]
    tonic::include_proto!("mzdb");
}

use mzdb::get_response;
use mzdb::node_client::NodeClient;
use mzdb::{GetRequest, LookupRequest, SetRequest, WhoisRequest};

use clap::Parser;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tonic::Request;

use std::net::SocketAddr;
use std::str;

/// MZDB shell client
#[derive(Parser)]
#[clap(name = "mzdb-client", version = "0.1.0")]
struct Args {
    /// Address of the node to connect to
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let url = format!("http://{}", args.addr);
    let client = NodeClient::connect(url).await?;
    println!("Node Connected");

    shell(client).await;

    Ok(())
}

async fn shell(mut client: NodeClient<tonic::transport::Channel>) {
    let msg = {
        let request = Request::new(WhoisRequest {});
        let node = client.whothis(request).await.unwrap().into_inner();
        format!("mzdb {} {} > ", node.key_slot, node.addr)
    };

    let mut reader = BufReader::new(stdin());
    let mut writer = BufWriter::new(stdout());
    let mut buf = String::new();
    loop {
        writer.write(msg.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();
        let bytes = reader.read_line(&mut buf).await.unwrap();
        if bytes == 1 {
            buf.clear();
            continue;
        }

        // parse line into request
        let mut tokens = buf.split_whitespace().map(|v| v.to_string());

        if let Some(command) = tokens.next() {
            match command.to_uppercase().as_str() {
                "GET" => {
                    let key = tokens.next().unwrap();
                    let request = Request::new(GetRequest { key, relay: true });
                    let response = client.get(request).await.unwrap();
                    let response = response.into_inner().response.unwrap();
                    match response {
                        get_response::Response::Value(value) => {
                            println!("GET: {:?}", str::from_utf8(&value).unwrap());
                        }
                        get_response::Response::Error(error) => {
                            println!("GET failed: {:?}", error);
                        }
                    }
                }
                "SET" => {
                    let key = tokens.next().unwrap();
                    let value = tokens.next().unwrap();
                    let request = Request::new(SetRequest {
                        key,
                        value: value.bytes().collect(),
                        relay: true,
                    });
                    let response = client.set(request).await.unwrap();
                    let response = response.into_inner().success;
                    println!("SET: {:?}", response);
                }
                "WHOIS" => {
                    let request = Request::new(WhoisRequest {});
                    let response = client.whothis(request).await.unwrap();
                    let response = response.into_inner();
                    println!("WHOIS: {:?}", response);
                }
                "LOOKUP" => {
                    let key_slot = tokens.next().unwrap().parse().unwrap();
                    let request = Request::new(LookupRequest {
                        key_slot,
                        relay: true,
                    });
                    let response = client.lookup(request).await.unwrap();
                    let response = response.into_inner();
                    println!("LOOKUP: {:?}", response);
                }
                _ => {
                    println!("Unknown command");
                    println!("Available commands: GET, SET, WHOIS, LOOKUP");
                }
            }
        }

        buf.clear();
    }
}
