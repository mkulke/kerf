use anyhow::Result;
use raft::Empty;
use std::env;
use tonic::Request;

pub mod raft {
    tonic::include_proto!("raft");
}

use raft::client::RaftClient;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut client = RaftClient::connect("http://[::1]:10000")?;
    let arg = &args[1];
    match arg.as_str() {
        "append_entries" => {
            let res = client.append_entries(Request::new(Empty {})).await?;
            println!("res: {:?}", res);
        }
        "request_vote" => {
            let res = client.request_vote(Request::new(Empty {})).await?;
            println!("res: {:?}", res);
        }
        _ => unimplemented!(),
    };
    Ok(())
}
