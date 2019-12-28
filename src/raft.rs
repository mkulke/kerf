// Code inspired by:
// https://yoric.github.io/post/rust-typestate
// https://github.com/rustic-games/sm

use futures_util::future::{abortable, AbortHandle};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::delay_for;

const TIMEOUT: u64 = 5;
const NUM_NODES: u8 = 3;
const QUORUM: u8 = (NUM_NODES / 2) + 1;

type Oneshot<T> = oneshot::Sender<T>;

#[derive(Debug)]
pub enum Message {
    Rpc { term: i32, rpc: Rpc },
    TimerElapsed,
    Vote((i32, bool)),
}

pub type Reply = (i32, bool);

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

        let timeout = spawn_timer(&inner.tx);
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
            (Leader(_), AppendEntries(_)) => todo!(),
            (Leader(_), VoteRequest(_)) => todo!(),
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
                Leader(l) => l.into_enum(),
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

struct RaftInner {
    candidate_id: String,
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
        self.voted_for = Some(self.candidate_id.clone());
        let votes = 1;
        let timeout = spawn_timer(&self.tx);
        let candidate = Candidate { votes, timeout };
        request_votes(&self.tx, self.term);
        Raft {
            inner: self,
            state: candidate,
        }
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
        self.state.timeout = spawn_timer(&self.inner.tx.clone());
        self.into_enum()
    }
}

impl Raft<Candidate> {
    fn into_leader(self) -> Raft<Leader> {
        Raft {
            inner: self.inner,
            state: Leader,
        }
    }

    fn into_follower(self) -> Raft<Follower> {
        let timeout = spawn_timer(&self.inner.tx);
        let follower = Follower { timeout };
        Raft {
            inner: self.inner,
            state: follower,
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
        self.state.timeout = spawn_timer(&self.inner.tx);
        request_votes(&self.inner.tx, self.inner.term);
        self.into_enum()
    }

    fn receive_vote(mut self, vote: (i32, bool)) -> Variant {
        let (_term, yes) = vote;
        let votes = &mut self.state.votes;
        if yes {
            *votes += 1;
        }

        if *votes >= QUORUM {
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

struct Follower {
    timeout: Timeout,
}

struct Candidate {
    timeout: Timeout,
    votes: u8,
}

struct Leader;

#[derive(Debug)]
struct Timeout {
    abort_handle: AbortHandle,
}

impl Drop for Timeout {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

fn spawn_timer(tx: &Sender<Message>) -> Timeout {
    let delay = delay_for(Duration::from_secs(TIMEOUT));
    let (abortable_delay, abort_handle) = abortable(delay);
    let mut tx = tx.clone();
    tokio::spawn(async move {
        if abortable_delay.await.is_ok() {
            tx.send(Message::TimerElapsed).await.expect("channel error");
        }
    });
    Timeout { abort_handle }
}

// TODO: the individual operations can fail, is it a 'no' vote?
fn request_votes(tx: &Sender<Message>, term: i32) {
    for n in 1..NUM_NODES {
        let mut vote_tx = tx.clone();
        let delay = delay_for(Duration::from_secs(1 + n as u64));
        tokio::spawn(async move {
            delay.await;
            vote_tx
                .send(Message::Vote((term, true)))
                .await
                .expect("channel error");
        });
    }
}

#[cfg(test)]
mod follower {
    use super::*;
    use enum_extract::let_extract;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn new_follower(tx: Sender<Message>) -> Raft<Follower> {
        let timeout = spawn_timer(&tx);
        let voted_for = None;
        let term = 0;
        let candidate_id = "test".to_string();
        let inner = RaftInner {
            candidate_id,
            term,
            tx,
            voted_for,
        };
        let state = Follower { timeout };
        Raft { inner, state }
    }

    #[tokio::test]
    async fn react_to_append_entries() {
        let (tx, mut rx) = mpsc::channel(1);
        let follower = new_follower(tx.clone());
        let (resp_tx, _resp_rx) = oneshot::channel();
        delay_for(Duration::from_secs(1)).await;
        let _variant = follower.append_entries(0, resp_tx);
        let result = timeout(Duration::from_millis(TIMEOUT * 1000 - 100), rx.recv()).await;
        assert!(result.is_err(), "there should be no follower timeout");
    }

    #[tokio::test]
    async fn promotion_to_candidate_after_timeout() {
        let (tx, _) = mpsc::channel(1);
        let follower = new_follower(tx.clone());
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

    impl Message {
        fn vote(self) -> Option<bool> {
            if let Self::Vote((_, b)) = self {
                Some(b)
            } else {
                None
            }
        }
    }

    fn new_inner(tx: Sender<Message>) -> RaftInner {
        RaftInner {
            candidate_id: "test".to_string(),
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
        let (term, vote_granted) = timeout(Duration::from_millis(TIMEOUT * 1000 - 100), resp_rx)
            .await
            .expect("test takes too long")
            .unwrap();
        assert_eq!(vote_granted, true);
        assert_eq!(term, 42);

        inner.voted_for = Some("one".to_string());
        let (resp_tx, resp_rx) = oneshot::channel();
        inner.vote(42, "other".to_string(), resp_tx);
        let (_, vote_granted) = timeout(Duration::from_millis(TIMEOUT * 1000 - 100), resp_rx)
            .await
            .expect("test takes too long")
            .unwrap();
        assert_eq!(vote_granted, false);

        let (resp_tx, resp_rx) = oneshot::channel();
        inner.vote(41, "test".to_string(), resp_tx);
        let (_, vote_granted) = timeout(Duration::from_millis(TIMEOUT * 1000 - 100), resp_rx)
            .await
            .expect("test takes too long")
            .unwrap();
        assert_eq!(vote_granted, false);
    }

    #[tokio::test]
    async fn start_election() {
        let n: usize = NUM_NODES as usize - 1;
        let (tx, mut rx) = mpsc::channel(1);
        let inner = new_inner(tx);
        inner.start_election();
        for _ in 0..n {
            let message = timeout(
                Duration::from_millis(NUM_NODES as u64 * 1000 + 100),
                rx.recv(),
            )
            .await
            .expect("test takes too long")
            .unwrap();
            // because we mocked the voting
            let yes = message.vote().expect("should be a vote message");
            assert_eq!(yes, true);
        }
    }
}

#[cfg(test)]
mod candidate {
    use super::*;
    use enum_extract::let_extract;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    fn new_candidate(tx: Sender<Message>) -> Raft<Candidate> {
        let inner = RaftInner {
            candidate_id: "test".to_string(),
            voted_for: Some("test".to_string()),
            term: 42,
            tx: tx.clone(),
        };
        let votes = 0;
        let timeout = spawn_timer(&tx);
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
        let message = timeout(Duration::from_millis(TIMEOUT * 1000 + 100), rx.recv())
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
        let candidate = new_candidate(tx);
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
        let mut candidate = new_candidate(tx.clone());
        candidate.state.votes = 42;
        let mut variant = candidate.into_enum();
        variant = variant.transition(Message::TimerElapsed);
        let_extract!(Variant::Candidate(candidate), variant, panic!());
        let votes = &candidate.state.votes;
        assert_eq!(*votes, 1);
    }
}

pub async fn message_loop(mut rx: Receiver<Message>, tx: Sender<Message>) {
    let timeout = spawn_timer(&tx);
    let voted_for = None;
    let term = 0;
    let candidate_id = "mock".to_string();
    let inner = RaftInner {
        candidate_id,
        term,
        tx,
        voted_for,
    };
    let state = Follower { timeout };
    let initial = Raft { inner, state };

    let mut sm = initial.into_enum();
    while let Some(message) = rx.recv().await {
        println!("message: {:?}", &message);
        sm = sm.transition(message);
    }
}
