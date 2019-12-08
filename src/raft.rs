// Code inspired by:
// https://yoric.github.io/post/rust-typestate
// https://github.com/rustic-games/sm

use futures_util::future::{abortable, AbortHandle};
use std::time::Duration;
use tokio::clock::now;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::timer::delay;

const TIMEOUT: u64 = 5;
const NUM_NODES: u8 = 3;
const QUORUM: u8 = (NUM_NODES / 2) + 1;

type Oneshot<T> = oneshot::Sender<T>;

#[derive(Debug)]
pub enum Message {
    AppendEntries(Oneshot<()>),
    TimerElapsed,
    VoteRequest(Oneshot<bool>),
    Vote(bool),
}

impl Message {
    #[allow(dead_code)]
    fn append_entries(self) -> Option<Oneshot<()>> {
        if let Self::AppendEntries(o) = self {
            Some(o)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn timer_elapsed(self) -> Option<()> {
        if let Self::TimerElapsed = self {
            Some(())
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn vote(self) -> Option<bool> {
        if let Self::Vote(b) = self {
            Some(b)
        } else {
            None
        }
    }
}

trait AsEnum {
    type Enum;

    fn as_enum(self) -> Self::Enum;
}

enum Variant {
    Follower(Raft<Follower>),
    Candidate(Raft<Candidate>),
    Leader(Raft<Leader>),
}

impl Variant {
    fn transition(self, message: Message) -> Self {
        use Message::*;
        use Variant::*;

        match (self, message) {
            (Follower(f), AppendEntries(o)) => f.append_entries(o),
            (Follower(f), TimerElapsed) => f.timeout(),
            (Follower(f), VoteRequest(o)) => f.vote(o),
            (Follower(f), Vote(_)) => f.as_enum(),
            (Candidate(c), Vote(v)) => c.receive_vote(v),
            (Candidate(c), VoteRequest(o)) => c.vote(o),
            (Candidate(c), _) => c.as_enum(),
            (Leader(y), _) => y.as_enum(),
        }
    }

    #[allow(dead_code)]
    fn follower(self) -> Option<Raft<Follower>> {
        if let Self::Follower(f) = self {
            Some(f)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn candidate(self) -> Option<Raft<Candidate>> {
        if let Self::Candidate(c) = self {
            Some(c)
        } else {
            None
        }
    }

    #[allow(dead_code)]
    fn leader(self) -> Option<Raft<Leader>> {
        if let Self::Leader(l) = self {
            Some(l)
        } else {
            None
        }
    }
}

impl AsEnum for Raft<Follower> {
    type Enum = Variant;

    fn as_enum(self) -> Self::Enum {
        Variant::Follower(self)
    }
}

impl AsEnum for Raft<Candidate> {
    type Enum = Variant;

    fn as_enum(self) -> Self::Enum {
        Variant::Candidate(self)
    }
}

impl AsEnum for Raft<Leader> {
    type Enum = Variant;

    fn as_enum(self) -> Self::Enum {
        Variant::Leader(self)
    }
}

struct RaftInner {
    tx: Sender<Message>,
    term: i32,
}

struct Raft<S> {
    inner: RaftInner,
    /// 0-sized field, doesn't exist at runtime.
    state: S,
}

impl Raft<Follower> {
    fn to_candidate(mut self) -> Raft<Candidate> {
        self.inner.term += 1;
        let votes = Votes { yes: 1, no: 0 };
        let candidate = Candidate { votes };
        request_votes(&self.inner.tx, self.inner.term);
        Raft {
            inner: self.inner,
            state: candidate,
        }
    }

    fn timeout(self) -> Variant {
        self.to_candidate().as_enum()
    }

    fn vote(mut self, oneshot: Oneshot<bool>) -> Variant {
        if self.state.voted_yet {
            oneshot.send(false).expect("oneshot error");
        } else {
            oneshot.send(true).expect("oneshot error");
            self.state.voted_yet = true;
        }
        self.as_enum()
    }

    fn append_entries(mut self, oneshot: Oneshot<()>) -> Variant {
        oneshot.send(()).expect("oneshot error");
        self.state.timeout = spawn_timer(&self.inner.tx.clone());
        self.as_enum()
    }
}

impl Raft<Candidate> {
    fn to_leader(self) -> Raft<Leader> {
        Raft {
            inner: self.inner,
            state: Leader,
        }
    }

    fn to_follower(self) -> Raft<Follower> {
        let timeout = spawn_timer(&self.inner.tx);
        let voted_yet = false;
        let follower = Follower { timeout, voted_yet };
        Raft {
            inner: self.inner,
            state: follower,
        }
    }

    fn receive_vote(mut self, yes: bool) -> Variant {
        let votes = &mut self.state.votes;
        if yes {
            votes.yes += 1
        } else {
            votes.no += 1
        }
        if votes.yes >= QUORUM {
            self.to_leader().as_enum()
        } else if votes.no >= QUORUM {
            self.to_follower().as_enum()
        } else {
            self.as_enum()
        }
    }

    // TODO: term
    fn vote(self, oneshot: Oneshot<bool>) -> Variant {
        oneshot.send(false).expect("oneshot error");
        self.as_enum()
    }
}

struct Follower {
    timeout: Timeout,
    voted_yet: bool,
}

struct Candidate {
    votes: Votes,
}

#[derive(Debug, Clone)]
struct Votes {
    yes: u8,
    no: u8,
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
    let when = now() + Duration::from_secs(TIMEOUT);
    let delay = delay(when);
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
fn request_votes(tx: &Sender<Message>, _term: i32) {
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

#[cfg(test)]
mod follower {
    use super::*;
    use tokio::prelude::*;
    use tokio::sync::mpsc;

    fn new_follower(tx: Sender<Message>) -> Raft<Follower> {
        let timeout = spawn_timer(&tx);
        let voted_yet = false;
        let term = 0;
        let inner = RaftInner { term, tx };
        let state = Follower { timeout, voted_yet };
        Raft { inner, state }
    }

    #[tokio::test]
    async fn promotion_to_candidate() {
        let n: usize = NUM_NODES as usize - 1;
        let (tx, mut rx) = mpsc::channel(n);
        let follower = new_follower(tx);
        follower.to_candidate();
        for _ in 0..n {
            let message = rx
                .recv()
                .timeout(Duration::from_millis(NUM_NODES as u64 * 1000 + 100))
                .await
                .expect("test takes too long")
                .unwrap();
            let yes = message.vote().expect("should be a vote message");
            assert_eq!(yes, true);
        }
    }
}

#[cfg(test)]
mod candidate {
    use super::*;
    use tokio::prelude::*;
    use tokio::sync::mpsc;

    fn new_candidate(tx: Sender<Message>) -> Raft<Candidate> {
        let inner = RaftInner { term: 42, tx };
        let votes = Votes { yes: 0, no: 0 };
        let state = Candidate { votes };
        Raft { inner, state }
    }

    #[tokio::test]
    async fn demotion_to_follower() {
        let (tx, mut rx) = mpsc::channel(1);
        let candidate = new_candidate(tx.clone());
        // note: we have to keep the follower in scope
        // or the timeout will be dropped
        let _follower = candidate
            .receive_vote(false)
            .candidate()
            .expect("should be candidate")
            .receive_vote(false)
            .follower()
            .expect("should be follower");
        let message = rx
            .recv()
            .timeout(Duration::from_millis(TIMEOUT * 1000 + 100).into())
            .await
            .expect("test takes too long")
            .unwrap();
        message
            .timer_elapsed()
            .expect("should be a timeout message");
    }

    // #[tokio::test]
    #[test]
    fn receiving_votes() {
        let (tx, _) = mpsc::channel(1);
        let mut candidate = new_candidate(tx.clone());
        let mut variant = candidate.receive_vote(true);
        candidate = variant.candidate().unwrap();
        let mut votes = &candidate.state.votes;
        assert_eq!(votes.yes, 1);
        assert_eq!(votes.no, 0);
        variant = candidate.receive_vote(false);
        candidate = variant.candidate().expect("should be candidate");
        votes = &candidate.state.votes;
        assert_eq!(votes.yes, 1);
        assert_eq!(votes.no, 1);
        candidate
            .receive_vote(true)
            .leader()
            .expect("should be leader");
    }
}

pub async fn message_loop(mut rx: Receiver<Message>, tx: Sender<Message>) {
    let timeout = spawn_timer(&tx);
    let voted_yet = false;
    let term = 0;
    let inner = RaftInner { term, tx };
    let state = Follower { timeout, voted_yet };
    let initial = Raft { inner, state };

    let mut sm = initial.as_enum();
    while let Some(message) = rx.recv().await {
        sm = sm.transition(message);
    }
}
