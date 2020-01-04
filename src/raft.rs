// Code inspired by:
// https://yoric.github.io/post/rust-typestate
// https://github.com/rustic-games/sm

// use crate::network::{request_vote, Address};
use crate::transport::Addressable;
use crate::transport::Member;
use futures_util::future::{abortable, AbortHandle};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::delay_for;

const ELECTION_TIMEOUT: u64 = 2000;
const HEARTBEAT_TIMEOUT: u64 = 200;

enum Timer {
    Election,
    Heartbeat,
}

type Oneshot<T> = oneshot::Sender<T>;
pub type Reply = (i32, bool);

#[derive(Debug)]
pub enum Message {
    Rpc { term: i32, rpc: Rpc },
    TimerElapsed,
    Vote(Reply),
}

#[derive(Debug)]
pub enum Rpc {
    AppendEntries(Oneshot<Reply>),
    VoteRequest((String, Oneshot<Reply>)),
}

trait AsEnum {
    type Enum;

    fn into_enum(self) -> Self::Enum;
}

enum Variant {
    Follower(Raft<Follower>),
    Candidate(Raft<Candidate>),
    Leader(Raft<Leader>),
}

impl Variant {
    fn into_follower(self, term: i32) -> Self {
        let mut inner = match self {
            Variant::Follower(f) => f.inner,
            Variant::Candidate(c) => c.inner,
            Variant::Leader(l) => l.inner,
        };

        inner.term = term;

        let timeout = inner.spawn_timer(Timer::Election);
        let state = Follower { timeout };
        let raft = Raft { inner, state };

        raft.into_enum()
    }

    fn term_lt(&self, term: i32) -> bool {
        let inner = match &self {
            Variant::Follower(f) => &f.inner,
            Variant::Candidate(c) => &c.inner,
            Variant::Leader(l) => &l.inner,
        };
        term > inner.term
    }

    fn handle_rpc(self, rpc: Rpc, term: i32) -> Self {
        use Rpc::*;
        use Variant::*;

        match (self, rpc) {
            (Follower(f), AppendEntries(o)) => f.append_entries(term, o),
            (Follower(f), VoteRequest((i, o))) => f.vote(term, i, o),
            (Candidate(c), AppendEntries(o)) => c.append_entries(term, o),
            (Candidate(c), VoteRequest((i, o))) => c.vote(term, i, o),
            (Leader(l), AppendEntries(_)) => l.into_enum(),
            (Leader(l), VoteRequest(_)) => l.into_enum(),
        }
    }

    fn transition(self, message: Message) -> Self {
        use Message::*;
        use Variant::*;

        match message {
            Rpc { term, rpc } => {
                if self.term_lt(term) {
                    self.into_follower(term)
                } else {
                    self.handle_rpc(rpc, term)
                }
            }
            TimerElapsed => match self {
                Follower(f) => f.timeout(),
                Candidate(c) => c.timeout(),
                Leader(l) => l.timeout(),
            },
            Vote(v) => match self {
                Follower(f) => f.into_enum(),
                Candidate(c) => c.receive_vote(v),
                Leader(l) => l.into_enum(),
            },
        }
    }
}

impl AsEnum for Raft<Follower> {
    type Enum = Variant;

    fn into_enum(self) -> Self::Enum {
        Variant::Follower(self)
    }
}

impl AsEnum for Raft<Candidate> {
    type Enum = Variant;

    fn into_enum(self) -> Self::Enum {
        Variant::Candidate(self)
    }
}

impl AsEnum for Raft<Leader> {
    type Enum = Variant;

    fn into_enum(self) -> Self::Enum {
        Variant::Leader(self)
    }
}

// #[derive(Clone)]
// pub struct Member {
//     addr: Address,
//     id_mgns: String,
// }

// impl Member {
//     pub fn new(addr: Address, id: String) -> Self {
//         Member { addr, id_mgns: id }
//     }
//     pub fn addr(&self) -> Address {
//         self.addr
//     }
// }

struct RaftInner {
    peers: Vec<Member>,
    id: String,
    tx: Sender<Message>,
    term: i32,
    voted_for: Option<String>,
}

impl RaftInner {
    fn new_term(&mut self) {
        self.term += 1;
        self.voted_for = None;
    }

    fn start_election(mut self) -> Raft<Candidate> {
        self.new_term();
        self.voted_for = Some(self.id.clone());
        let votes = 1;
        let timeout = self.spawn_timer(Timer::Election);
        let candidate = Candidate { votes, timeout };
        let raft = Raft {
            inner: self,
            state: candidate,
        };
        raft.request_votes();
        raft
    }

    fn spawn_timer(&self, timer: Timer) -> Timeout {
        let ms = match timer {
            Timer::Election => ELECTION_TIMEOUT,
            Timer::Heartbeat => HEARTBEAT_TIMEOUT,
        };
        let delay = delay_for(Duration::from_millis(ms));
        let (abortable_delay, abort_handle) = abortable(delay);
        let mut tx = self.tx.clone();
        tokio::spawn(async move {
            if abortable_delay.await.is_ok() {
                tx.send(Message::TimerElapsed).await.expect("channel error");
            }
        });
        Timeout { abort_handle }
    }

    fn vote(&mut self, term: i32, candidate_id: String, oneshot: Oneshot<Reply>) {
        let current_term = self.term;
        let vote_granted = {
            if term < current_term {
                false
            } else {
                match self.voted_for {
                    None => true,
                    Some(ref c_id) => &candidate_id == c_id,
                }
            }
        };
        if vote_granted {
            self.voted_for = Some(candidate_id);
        }
        oneshot
            .send((current_term, vote_granted))
            .expect("oneshot error");
    }

    fn append_entries(&mut self, term: i32, oneshot: Oneshot<Reply>) -> bool {
        let current_term = self.term;
        let success = term >= current_term;
        oneshot
            .send((current_term, success))
            .expect("oneshot error");
        success
    }
}

struct Raft<S> {
    inner: RaftInner,
    // 0-sized field, doesn't exist at runtime.
    state: S,
}

impl Raft<Follower> {
    fn into_candidate(self) -> Raft<Candidate> {
        self.inner.start_election()
    }

    fn timeout(self) -> Variant {
        self.into_candidate().into_enum()
    }

    fn vote(mut self, term: i32, candidate_id: String, oneshot: Oneshot<Reply>) -> Variant {
        self.inner.vote(term, candidate_id, oneshot);
        self.into_enum()
    }

    fn append_entries(mut self, term: i32, oneshot: Oneshot<Reply>) -> Variant {
        self.inner.append_entries(term, oneshot);
        self.state.timeout = self.inner.spawn_timer(Timer::Election);
        self.into_enum()
    }
}

impl Raft<Candidate> {
    fn into_leader(self) -> Raft<Leader> {
        let timeout = self.inner.spawn_timer(Timer::Heartbeat);
        let state = Leader { timeout };
        Raft {
            inner: self.inner,
            state,
        }
    }

    fn into_follower(self) -> Raft<Follower> {
        let timeout = self.inner.spawn_timer(Timer::Election);
        let follower = Follower { timeout };
        Raft {
            inner: self.inner,
            state: follower,
        }
    }

    fn request_votes(&self) {
        for peer in &self.inner.peers {
            let mut vote_tx = self.inner.tx.clone();
            let term = self.inner.term;
            let candidate_id = self.inner.id.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                let result = peer.request_vote(term, candidate_id).await;
                if let Ok(reply) = result {
                    let msg = Message::Vote(reply);
                    let _ = vote_tx.send(msg).await;
                }
            });
        }
    }

    fn append_entries(mut self, term: i32, oneshot: Oneshot<Reply>) -> Variant {
        let success = self.inner.append_entries(term, oneshot);
        if success {
            self.into_follower().into_enum()
        } else {
            self.into_enum()
        }
    }

    fn timeout(mut self) -> Variant {
        self.state.votes = 1;
        self.state.timeout = self.inner.spawn_timer(Timer::Election);
        self.request_votes();
        self.into_enum()
    }

    fn receive_vote(mut self, vote: (i32, bool)) -> Variant {
        let (_term, yes) = vote;
        let votes = &mut self.state.votes;
        if yes {
            *votes += 1;
        }

        let num_nodes = self.inner.peers.len() as u8 + 1;
        let quorum = (num_nodes / 2) + 1;

        if *votes >= quorum {
            self.into_leader().into_enum()
        } else {
            self.into_enum()
        }
    }

    fn vote(mut self, term: i32, candidate_id: String, oneshot: Oneshot<Reply>) -> Variant {
        self.inner.vote(term, candidate_id, oneshot);
        self.into_enum()
    }
}

impl Raft<Leader> {
    fn timeout(mut self) -> Variant {
        self.state.timeout = self.inner.spawn_timer(Timer::Heartbeat);
        self.into_enum()
    }
}

struct Follower {
    timeout: Timeout,
}

struct Candidate {
    timeout: Timeout,
    votes: u8,
}

struct Leader {
    timeout: Timeout,
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

#[cfg(test)]
mod follower {
    use super::*;
    use enum_extract::let_extract;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    // fn create_membiers() -> Vec<Member> {
    //     (0..2)
    //         .into_iter()
    //         .map(|i| {
    //             let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 10000 + i);
    //             let id = format!("test-{}", i);
    //             Member::new(addr, id)
    //         })
    //         .collect()
    // }

    fn new_follower(tx: Sender<Message>) -> Raft<Follower> {
        let voted_for = None;
        let term = 0;
        let id = "test".to_string();
        let inner = RaftInner {
            peers: vec![],
            id,
            term,
            tx,
            voted_for,
        };
        let timeout = inner.spawn_timer(Timer::Election);
        let state = Follower { timeout };
        Raft { inner, state }
    }

    #[tokio::test]
    async fn react_to_append_entries() {
        let (tx, mut rx) = mpsc::channel(1);
        let follower = new_follower(tx.clone());
        let (resp_tx, _resp_rx) = oneshot::channel();
        delay_for(Duration::from_millis(100)).await;
        let _variant = follower.append_entries(0, resp_tx);
        let result = timeout(Duration::from_millis(ELECTION_TIMEOUT - 100), rx.recv()).await;
        assert!(result.is_err(), "there should be no election timeout");
    }

    #[tokio::test]
    async fn promotion_to_candidate_after_timeout() {
        let (tx, _) = mpsc::channel(1);
        let follower = new_follower(tx);
        let mut variant = follower.into_enum();
        variant = variant.transition(Message::TimerElapsed);
        let_extract!(Variant::Candidate(_candidate), variant, panic!());
    }
}

#[cfg(test)]
mod inner {
    use super::*;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;
    const TIMEOUT: u64 = 5000;

    fn new_inner(tx: Sender<Message>) -> RaftInner {
        RaftInner {
            peers: vec![],
            id: "test".to_string(),
            voted_for: Some("test".to_string()),
            term: 42,
            tx,
        }
    }

    #[tokio::test]
    async fn vote() {
        let (tx, _) = mpsc::channel(1);
        let mut inner = new_inner(tx);
        inner.voted_for = None;
        let (resp_tx, resp_rx) = oneshot::channel();
        inner.vote(42, "test".to_string(), resp_tx);
        let (term, vote_granted) = timeout(Duration::from_millis(TIMEOUT - 100), resp_rx)
            .await
            .expect("test takes too long")
            .unwrap();
        assert_eq!(vote_granted, true);
        assert_eq!(term, 42);

        inner.voted_for = Some("one".to_string());
        let (resp_tx, resp_rx) = oneshot::channel();
        inner.vote(42, "other".to_string(), resp_tx);
        let (_, vote_granted) = timeout(Duration::from_millis(TIMEOUT - 100), resp_rx)
            .await
            .expect("test takes too long")
            .unwrap();
        assert_eq!(vote_granted, false);

        let (resp_tx, resp_rx) = oneshot::channel();
        inner.vote(41, "test".to_string(), resp_tx);
        let (_, vote_granted) = timeout(Duration::from_millis(TIMEOUT - 100), resp_rx)
            .await
            .expect("test takes too long")
            .unwrap();
        assert_eq!(vote_granted, false);
    }
}

#[cfg(test)]
mod candidate {
    use super::*;
    use enum_extract::let_extract;
    // use std::net::{Ipv4Addr, SocketAddrV4};
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    const TIMEOUT: u64 = 5000;

    fn new_candidate(tx: Sender<Message>) -> Raft<Candidate> {
        let inner = RaftInner {
            peers: vec![],
            id: "test".to_string(),
            voted_for: Some("test".to_string()),
            term: 42,
            tx,
        };
        let votes = 0;
        let timeout = inner.spawn_timer(Timer::Election);
        let state = Candidate { votes, timeout };
        Raft { inner, state }
    }

    impl Message {
        fn timer_elapsed(self) -> Option<()> {
            if let Self::TimerElapsed = self {
                Some(())
            } else {
                None
            }
        }
    }

    #[tokio::test]
    async fn election_restart() {
        let (tx, _) = mpsc::channel(1);
        let candidate = new_candidate(tx);
        let variant = candidate.timeout();
        let_extract!(Variant::Candidate(candidate), variant, panic!());
        let votes = &candidate.state.votes;
        assert_eq!(*votes, 1);
    }

    #[tokio::test]
    async fn demote_to_follower() {
        let (tx, _) = mpsc::channel(1);
        let candidate = new_candidate(tx);
        let (resp_tx, _resp_rx) = oneshot::channel();
        let variant = candidate.append_entries(42, resp_tx);
        let_extract!(Variant::Follower(_follower), variant, panic!());
    }

    #[tokio::test]
    async fn election_times_out() {
        let (tx, mut rx) = mpsc::channel(1);
        let _candidate = new_candidate(tx.clone());
        let message = timeout(Duration::from_millis(TIMEOUT + 100), rx.recv())
            .await
            .expect("test takes too long")
            .unwrap();
        message
            .timer_elapsed()
            .expect("should be a timeout message");
    }

    #[tokio::test]
    async fn receiving_votes() {
        let (tx, _) = mpsc::channel(1);
        let mut candidate = new_candidate(tx);
        // let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 10000);
        candidate.inner.peers = vec![Member, Member];
        let vote = (42, true);
        let mut variant = candidate.receive_vote(vote);
        let_extract!(Variant::Candidate(candidate), variant, panic!());
        let mut votes = &candidate.state.votes;
        assert_eq!(*votes, 1);
        let vote = (42, false);
        variant = candidate.receive_vote(vote);
        let_extract!(Variant::Candidate(candidate), variant, panic!());
        votes = &candidate.state.votes;
        assert_eq!(*votes, 1);
        let vote = (42, true);
        let variant = candidate.receive_vote(vote);
        let_extract!(Variant::Leader(_l), variant, panic!());
    }

    #[tokio::test]
    async fn restart_election_on_timeout() {
        let (tx, _) = mpsc::channel(1);
        let mut candidate = new_candidate(tx);
        candidate.state.votes = 42;
        let mut variant = candidate.into_enum();
        variant = variant.transition(Message::TimerElapsed);
        let_extract!(Variant::Candidate(candidate), variant, panic!());
        let votes = &candidate.state.votes;
        assert_eq!(*votes, 1);
    }
}

pub async fn message_loop(peers: Vec<Member>, mut rx: Receiver<Message>, tx: Sender<Message>) {
    let voted_for = None;
    let term = 0;
    let id = "mock".to_string();
    let inner = RaftInner {
        peers,
        id,
        term,
        tx,
        voted_for,
    };
    let timeout = inner.spawn_timer(Timer::Election);
    let state = Follower { timeout };
    let initial = Raft { inner, state };

    let mut sm = initial.into_enum();
    while let Some(message) = rx.recv().await {
        println!("message: {:?}", &message);
        sm = sm.transition(message);
    }
}
