use anyhow::Result;
use futures::future::join;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

pub mod raft;
use raft::{message_loop, Message};

pub mod proto {
    tonic::include_proto!("raft");
}

use proto::{raft_server, Term, VoteReply};

#[derive(Debug)]
pub struct Service {
    tx: Sender<Message>,
}

impl Service {
    async fn send_append_entries(mut tx: Sender<Message>, term: i32) -> Result<i32> {
        let (resp_tx, resp_rx) = oneshot::channel::<i32>();
        tx.send(Message::AppendEntries((resp_tx, term))).await?;
        let term = resp_rx.await?;
        Ok(term)
    }

    async fn send_vote_request(mut tx: Sender<Message>, term: i32) -> Result<(i32, bool)> {
        let (resp_tx, resp_rx) = oneshot::channel::<(i32, bool)>();
        tx.send(Message::VoteRequest((resp_tx, term))).await?;
        let resp = resp_rx.await?;
        Ok(resp)
    }
}

#[tonic::async_trait]
impl raft_server::Raft for Service {
    async fn append_entries(&self, request: Request<Term>) -> Result<Response<Term>, Status> {
        let term = request.into_inner().term;
        Service::send_append_entries(self.tx.clone(), term)
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|term| Response::new(Term { term }))
    }

    async fn request_vote(&self, request: Request<Term>) -> Result<Response<VoteReply>, Status> {
        let term = request.into_inner().term;
        Service::send_vote_request(self.tx.clone(), term)
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|(term, yes)| Response::new(VoteReply { term, yes }))
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
