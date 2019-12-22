use anyhow::Result;
use std::env;
use tonic::Request;

pub mod raft {
    tonic::include_proto!("raft");
}

use raft::raft_client::RaftClient;
use raft::Term;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut client = RaftClient::connect("http://[::1]:10000").await?;
    let arg = &args[1];
    let term = Term { term: 0 };
    match arg.as_str() {
        "append_entries" => {
            let res = client.append_entries(Request::new(term)).await?;
            println!("res: {:?}", res);
        }
        "request_vote" => {
            let res = client.request_vote(Request::new(term)).await?;
            println!("res: {:?}", res);
        }
        _ => unimplemented!(),
    };
    Ok(())
}
