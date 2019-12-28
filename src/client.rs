use anyhow::Result;
use std::env;
use tonic::Request;

pub mod raft {
    tonic::include_proto!("raft");
}

use raft::raft_client::RaftClient;
use raft::{AppendEntriesRequest, RequestVoteRequest};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let mut client = RaftClient::connect("http://[::1]:10000").await?;
    let arg = &args[1];
    match arg.as_str() {
        "append_entries" => {
            let req = AppendEntriesRequest { term: 0 };
            let res = client.append_entries(Request::new(req)).await?;
            println!("res: {:?}", res);
        }
        "request_vote" => {
            let req = RequestVoteRequest {
                candidate_id: "mock".to_string(),
                term: 0,
            };
            let res = client.request_vote(Request::new(req)).await?;
            println!("res: {:?}", res);
        }
        _ => unimplemented!(),
    };
    Ok(())
}
