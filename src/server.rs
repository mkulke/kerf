use anyhow::Result;
use futures::future::join;
use futures_util::future::{abortable, AbortHandle};
use std::time::Duration;
use tokio::clock::now;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::timer::delay;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

const NUM_NODES: u8 = 3;
const QUORUM: u8 = (NUM_NODES / 2) + 1;

pub mod raft {
    tonic::include_proto!("raft");
}

use raft::{server, Empty, VoteReply};

#[derive(Debug)]
pub struct Raft {
    tx: Sender<Message>,
}

#[derive(Debug)]
enum Message {
    AppendEntries(oneshot::Sender<()>),
    VoteRequest(oneshot::Sender<bool>),
    TimerElapsed,
    Vote(bool),
}

#[derive(Debug)]
struct Timeout {
    abort_handle: AbortHandle,
}

struct Machine {
    tx: Sender<Message>,
    state: State,
}

#[derive(Debug)]
struct Votes {
    yes: u8,
    no: u8,
}

#[derive(Debug)]
enum State {
    Follower { timeout: Timeout, voted_yet: bool },
    Candidate { votes: Votes },
    Leader,
}

impl Raft {
    async fn send_append_entries(mut tx: Sender<Message>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(Message::AppendEntries(resp_tx)).await?;
        resp_rx.await?;
        Ok(())
    }

    async fn send_vote_request(mut tx: Sender<Message>) -> Result<bool> {
        let (resp_tx, resp_rx) = oneshot::channel::<bool>();
        tx.send(Message::VoteRequest(resp_tx)).await?;
        let resp = resp_rx.await?;
        Ok(resp)
    }
}

#[tonic::async_trait]
impl server::Raft for Raft {
    async fn append_entries(&self, _request: Request<Empty>) -> Result<Response<Empty>, Status> {
        Raft::send_append_entries(self.tx.clone())
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|_| Response::new(Empty {}))
    }

    async fn request_vote(&self, _request: Request<Empty>) -> Result<Response<VoteReply>, Status> {
        Raft::send_vote_request(self.tx.clone())
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|yes| Response::new(VoteReply { yes }))
    }
}

fn spawn_timer(mut tx: Sender<Message>) -> Timeout {
    let when = now() + Duration::from_secs(10);
    let delay = delay(when);
    let (abortable_delay, abort_handle) = abortable(delay);
    tokio::spawn(async move {
        if abortable_delay.await.is_ok() {
            tx.send(Message::TimerElapsed).await.expect("channel error");
        }
    });
    Timeout { abort_handle }
}

fn request_votes(tx: Sender<Message>) {
    for n in 1..NUM_NODES {
        let mut vote_tx = tx.clone();
        let when = tokio::clock::now() + Duration::from_secs(1 + n as u64);
        let delay = delay(when);
        tokio::spawn(async move {
            delay.await;
            vote_tx
                .send(Message::Vote(true))
                .await
                .expect("channel error");
        });
    }
}

fn respond<T>(tx: oneshot::Sender<T>, response: T)
where
    T: std::fmt::Debug,
{
    tx.send(response).expect("oneshot error");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_machine_times_out_after_10s() {
        let (tx, mut rx) = mpsc::channel(1);
        let _ = Machine::new(tx);
        let message = rx.recv().await.unwrap();
        assert!(match message {
            Message::TimerElapsed => true,
            _ => false,
        });
    }
}

impl Machine {
    fn new(tx: Sender<Message>) -> Self {
        let timeout = spawn_timer(tx.clone());
        let voted_yet = false;
        let state = State::Follower { timeout, voted_yet };
        Machine { state, tx }
    }

    fn as_follower(&mut self) {
        let timeout = spawn_timer(self.tx.clone());
        let voted_yet = false;
        self.state = State::Follower { timeout, voted_yet }
    }

    fn as_candidate(&mut self) {
        let votes = Votes { yes: 1, no: 0 };
        self.state = State::Candidate { votes };
        request_votes(self.tx.clone());
    }

    fn as_leader(&mut self) {
        self.state = State::Leader;
    }

    fn transition(&mut self, message: Message) {
        use Message::*;
        use State::*;

        match self.state {
            Follower {
                ref mut timeout,
                ref mut voted_yet,
            } => match message {
                TimerElapsed => self.as_candidate(),
                AppendEntries(resp_tx) => {
                    timeout.abort_handle.abort();
                    *timeout = spawn_timer(self.tx.clone());
                    respond(resp_tx, ());
                }
                VoteRequest(resp_tx) => {
                    let vote = !*voted_yet;
                    *voted_yet = true;
                    respond(resp_tx, vote);
                }
                Vote(_) => {}
            },
            Candidate { ref mut votes, .. } => match message {
                VoteRequest(resp_tx) => respond(resp_tx, false),
                AppendEntries(resp_tx) => {
                    self.as_follower();
                    respond(resp_tx, ());
                }
                Vote(yes) => {
                    if yes {
                        votes.yes += 1
                    } else {
                        votes.no += 1
                    }
                    let all = votes.yes + votes.no;
                    if all >= QUORUM {
                        if votes.yes > votes.no {
                            self.as_leader();
                        } else {
                            self.as_follower();
                        }
                    }
                }
                TimerElapsed => {}
            },
            Leader => match message {
                Vote(_) => {}
                _ => unimplemented!(),
            },
        };
        println!("state: {:?}", self.state);
    }
}

async fn message_loop(mut rx: Receiver<Message>, tx: Sender<Message>) {
    let mut state = Machine::new(tx.clone());

    while let Some(message) = rx.recv().await {
        state.transition(message);
    }
}

async fn listen(tx: Sender<Message>) {
    let addr = "[::1]:10000".parse().unwrap();

    println!("Listening on: {}", addr);

    let raft = Raft { tx };
    let svc = server::RaftServer::new(raft);
    Server::builder()
        .serve(addr, svc)
        .await
        .expect("server could not start");
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(100);
    join(listen(tx.clone()), message_loop(rx, tx.clone())).await;
}
