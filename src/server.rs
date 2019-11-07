use anyhow::Result;
use futures::future::join;
use futures::Future;
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

use raft::{server, Empty, Term, VoteReply};

#[derive(Debug)]
pub struct Raft {
    tx: Sender<Message>,
}

#[derive(Debug)]
enum Message {
    AppendEntries((oneshot::Sender<()>, i32)),
    VoteRequest((oneshot::Sender<bool>, i32)),
    TimerElapsed,
    Vote(bool),
}

#[derive(Debug)]
struct Timeout {
    abort_handle: AbortHandle,
}

impl Drop for Timeout {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
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
enum Role {
    Follower { timeout: Timeout, voted_yet: bool },
    Candidate { votes: Votes },
    Leader,
}

#[derive(Debug)]
struct State {
    role: Role,
    term: i32,
}

impl Raft {
    async fn send_append_entries(mut tx: Sender<Message>, term: i32) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        tx.send(Message::AppendEntries((resp_tx, term))).await?;
        resp_rx.await?;
        Ok(())
    }

    async fn send_vote_request(mut tx: Sender<Message>, term: i32) -> Result<bool> {
        let (resp_tx, resp_rx) = oneshot::channel::<bool>();
        tx.send(Message::VoteRequest((resp_tx, term))).await?;
        let resp = resp_rx.await?;
        Ok(resp)
    }
}

#[tonic::async_trait]
impl server::Raft for Raft {
    async fn append_entries(&self, request: Request<Term>) -> Result<Response<Empty>, Status> {
        let term = request.into_inner().term;
        Raft::send_append_entries(self.tx.clone(), term)
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|_| Response::new(Empty {}))
    }

    async fn request_vote(&self, request: Request<Term>) -> Result<Response<VoteReply>, Status> {
        let term = request.into_inner().term;
        Raft::send_vote_request(self.tx.clone(), term)
            .await
            .map_err(|_| Status::new(Code::Internal, "channel error"))
            .map(|yes| Response::new(VoteReply { yes }))
    }
}

fn create_timer(mut tx: Sender<Message>) -> (impl Future<Output = ()>, AbortHandle) {
    let when = now() + Duration::from_secs(10);
    let delay = delay(when);
    let (abortable_delay, abort_handle) = abortable(delay);
    let task = async move {
        if abortable_delay.await.is_ok() {
            tx.send(Message::TimerElapsed).await.expect("channel error");
        }
    };
    (task, abort_handle)
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

// TODO: the individual operations can fail, is it a 'no' vote?
fn request_votes(tx: Sender<Message>, _term: i32) {
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

async fn request_votes_as(tx: Sender<Message>) {
    for n in 1..NUM_NODES {
        let mut vote_tx = tx.clone();
        let when = tokio::clock::now() + Duration::from_secs(1 + n as u64);
        delay(when).await;
        vote_tx
            .send(Message::Vote(true))
            .await
            .expect("channel error");
    }
}

impl Machine {
    fn new(tx: Sender<Message>) -> Self {
        let timeout = spawn_timer(tx.clone());
        let voted_yet = false;
        let role = Role::Follower { timeout, voted_yet };
        let term = 0;
        let state = State { role, term };
        Machine { state, tx }
    }

    fn to_candidate(&mut self) -> (State, impl Future<Output = ()>) {
        let votes = Votes { yes: 1, no: 0 };
        let role = Role::Candidate { votes };
        let state = State { role, ..self.state };
        let request_votes = request_votes_as(self.tx.clone());
        (state, request_votes)
    }

    fn to_follower(&mut self) -> (State, impl Future<Output = ()>) {
        let voted_yet = false;
        let (delay, abort_handle) = create_timer(self.tx.clone());
        let timeout = Timeout { abort_handle };
        let role = Role::Follower { timeout, voted_yet };
        let term = self.state.term + 1;
        let state = State { role, term };
        (state, delay)
    }

    fn to_leader(&mut self) -> (State, impl Future<Output = ()>) {
        let role = Role::Leader;
        let state = State { role, ..self.state };
        let task = async {
            unimplemented!();
        };
        (state, task)
    }

    fn as_follower(&mut self) {
        let timeout = spawn_timer(self.tx.clone());
        let voted_yet = false;
        self.state.role = Role::Follower { timeout, voted_yet };
        self.state.term += 1;
    }

    fn as_candidate(&mut self) {
        let votes = Votes { yes: 1, no: 0 };
        self.state.role = Role::Candidate { votes };
        request_votes(self.tx.clone(), self.state.term);
    }

    fn as_leader(&mut self) {
        self.state.role = Role::Leader;
    }

    fn transition_as(&mut self, message: Message) {
        use Message::*;
        use Role::*;

        println!("message: {:?}", message);
        let role = &self.state.role;
        let (state, task) = match (role, message) {
            (Follower { .. }, TimerElapsed) => self.to_candidate(),
            _ => unimplemented!(),
        };
        tokio::spawn(task);
        self.state = state;
        println!("state: {:?}", self.state);
    }

    fn transition(&mut self, message: Message) {
        use Message::*;
        use Role::*;

        println!("message: {:?}", message);
        match self.state.role {
            Follower {
                ref mut timeout,
                ref mut voted_yet,
            } => match message {
                TimerElapsed => self.as_candidate(),
                AppendEntries((resp_tx, _)) => {
                    *timeout = spawn_timer(self.tx.clone());
                    respond(resp_tx, ());
                }
                VoteRequest((resp_tx, _)) => {
                    let vote = !*voted_yet;
                    *voted_yet = true;
                    respond(resp_tx, vote);
                }
                Vote(_) => {}
            },
            Candidate { ref mut votes, .. } => match message {
                VoteRequest((resp_tx, _)) => respond(resp_tx, false),
                AppendEntries((resp_tx, _)) => {
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
        // state.transition(message);
        state.transition_as(message);
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
