use crate::raft::Reply;
use anyhow::Result;
use async_trait::async_trait;
use proto::raft_client::RaftClient;
use proto::RequestVoteRequest;
use tonic::Request;

pub mod proto {
    tonic::include_proto!("raft");
}

#[async_trait]
pub trait Addressable {
    fn to_url(&self) -> String;
    async fn request_vote(&self, term: i32, candidate_id: String) -> Result<Reply>;
}

#[derive(Clone, Default)]
pub struct Member;

#[async_trait]
impl Addressable for Member {
    fn to_url(&self) -> String {
        // format!("http://{ip}:{port}", ip = self.ip(), port = self.port())
        todo!();
    }

    async fn request_vote(&self, term: i32, candidate_id: String) -> Result<Reply> {
        let url = self.to_url();
        let req = RequestVoteRequest { candidate_id, term };
        let mut client = RaftClient::connect(url).await?;
        let reply = client.request_vote(Request::new(req)).await?.into_inner();
        Ok((reply.term, reply.vote_granted))
    }
}
