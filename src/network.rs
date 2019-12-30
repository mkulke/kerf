use crate::raft::{Member, Reply};
use anyhow::Result;
use proto::raft_client::RaftClient;
use proto::RequestVoteRequest;
use std::net::SocketAddrV4;
use tonic::Request;

pub mod proto {
    tonic::include_proto!("raft");
}

pub type Address = SocketAddrV4;

pub trait Addressable {
    fn to_url(&self) -> String;
}

impl Addressable for Address {
    fn to_url(&self) -> String {
        format!("http://{ip}:{port}", ip = self.ip(), port = self.port())
    }
}

pub async fn request_vote(peer: Member, term: i32, candidate_id: String) -> Result<Reply> {
    let url = peer.addr().to_url();
    let mut client = RaftClient::connect(url).await?;
    let req = RequestVoteRequest { candidate_id, term };
    let reply = client.request_vote(Request::new(req)).await?.into_inner();
    Ok((reply.term, reply.vote_granted))
}
