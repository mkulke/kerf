use anyhow::Result;
use futures::future::join;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

pub mod raft;
use raft::{message_loop, Message, Reply, Rpc};

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{
    raft_server, AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
};

#[derive(Debug)]
pub struct Service {
    tx: Sender<Message>,
}

impl Service {
    async fn send_append_entries(mut tx: Sender<Message>, term: i32) -> Result<Reply> {
        let (resp_tx, resp_rx) = oneshot::channel::<Reply>();
        let rpc = Rpc::AppendEntries(resp_tx);
        let message = Message::Rpc { rpc, term };
        tx.send(message).await?;
        let resp = resp_rx.await?;
        Ok(resp)
    }

    async fn send_vote_request(
        mut tx: Sender<Message>,
        term: i32,
        candidate_id: String,
    ) -> Result<Reply> {
        let (resp_tx, resp_rx) = oneshot::channel::<Reply>();
        let rpc = Rpc::VoteRequest((candidate_id, resp_tx));
        let message = Message::Rpc { rpc, term };
        tx.send(message).await?;
        let resp = resp_rx.await?;
        Ok(resp)
    }
}

#[tonic::async_trait]
impl raft_server::Raft for Service {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let term = request.into_inner().term;
        Service::send_append_entries(self.tx.clone(), term)
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|(term, success)| Response::new(AppendEntriesReply { success, term }))
    }

    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let inner = request.into_inner();
        let term = inner.term;
        let candidate_id = inner.candidate_id;
        Service::send_vote_request(self.tx.clone(), term, candidate_id)
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|(term, vote_granted)| Response::new(RequestVoteReply { term, vote_granted }))
    }
}

async fn listen(tx: Sender<Message>) {
    let addr = "[::1]:10000".parse().unwrap();

    println!("Listening on: {}", addr);

    let raft = Service { tx };
    let svc = raft_server::RaftServer::new(raft);
    Server::builder()
        .add_service(svc)
        .serve(addr)
        .await
        .expect("server could not start");
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(100);
    join(listen(tx.clone()), message_loop(rx, tx.clone())).await;
}
