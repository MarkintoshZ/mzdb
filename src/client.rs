mod mzdb {
    #![allow(non_snake_case)]
    tonic::include_proto!("mzdb");
}

use mzdb::get_response;
use mzdb::node_client::NodeClient;
use mzdb::{GetRequest, LookupRequest, SetRequest, WhoisRequest};

use clap::Parser;
use tokio::io::{stdin, stdout, AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader, BufWriter};
use tonic::Request;

use std::net::SocketAddr;
use std::path::Path;
use std::process::exit;
use std::str;

/// MZDB shell client
#[derive(Parser)]
#[clap(name = "mzdb-client", version = "0.1.0")]
struct Args {
    /// Address of the node to connect to
    addr: SocketAddr,

    /// Path of input file to execute line by line
    #[arg(short, long, default_value=None)]
    file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let url = format!("http://{}", args.addr);
    let client = NodeClient::connect(url).await?;
    println!("Node Connected");

    if let Some(filepath) = args.file {
        let path = Path::new(&filepath);

        if !path.exists() {
            println!("Input file does not exist");
            exit(1);
        }

        if !path.is_file() {
            println!("Input file is not a file");
            exit(1);
        }

        let file = tokio::fs::File::open(path).await.expect("Cannot open file");
        shell(client, BufReader::new(file), false).await;
    } else {
        shell(client, BufReader::new(stdin()), true).await;
    }

    Ok(())
}

async fn shell<R>(mut client: NodeClient<tonic::transport::Channel>, mut reader: R, is_shell: bool)
where
    R: AsyncRead + AsyncBufReadExt + std::marker::Unpin,
{
    let msg = {
        let request = Request::new(WhoisRequest {});
        let node = client.whothis(request).await.unwrap().into_inner();
        format!("mzdb {} {} > ", node.key_slot, node.addr)
    };

    let mut writer = BufWriter::new(stdout());
    let mut buf = String::new();
    loop {
        if is_shell {
            writer.write(msg.as_bytes()).await.unwrap();
        }
        writer.flush().await.unwrap();
        let bytes = reader.read_line(&mut buf).await.unwrap();
        if bytes == 1 {
            buf.clear();
            continue;
        } else if bytes == 0 {
            break;
        }

        // parse line into request
        let mut tokens = buf.split_whitespace().map(|v| v.to_string());

        if let Some(command) = tokens.next() {
            let output = match command.to_uppercase().as_str() {
                "GET" => {
                    let key = tokens.next().unwrap();
                    let request = Request::new(GetRequest { key, relay: true });
                    let response = client.get(request).await.unwrap();
                    let response = response.into_inner().response.unwrap();
                    match response {
                        get_response::Response::Value(value) => {
                            format!("GET: {:?}\n", str::from_utf8(&value).unwrap())
                        }
                        get_response::Response::Error(error) => {
                            format!("GET failed: {:?}\n", error)
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
                    format!("SET: {:?}\n", response)
                }
                "WHOIS" => {
                    let request = Request::new(WhoisRequest {});
                    let response = client.whothis(request).await.unwrap();
                    let response = response.into_inner();
                    format!("WHOIS: {:?}\n", response)
                }
                "LOOKUP" => {
                    let key_slot = tokens.next().unwrap().parse().unwrap();
                    let request = Request::new(LookupRequest {
                        key_slot,
                        relay: true,
                    });
                    let response = client.lookup(request).await.unwrap();
                    let response = response.into_inner();
                    format!("LOOKUP: {:?}\n", response)
                }
                _ => "Unknown command\nAvailable commands: GET, SET, WHOIS, LOOKUP\n".to_owned(),
            };

            writer
                .write(output.as_bytes())
                .await
                .expect("Unable to write output");
        }

        buf.clear();
    }
}
