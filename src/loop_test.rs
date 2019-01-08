use futures::future::{ok, loop_fn, Future, FutureResult, Loop};
use std::time::{Duration, Instant};
use tokio::timer::Delay;

#[derive(Clone)]
enum Role {
    Follower(bool),
    Candidate,
}

pub struct Node {
    role: Role,
}

impl Node {
    fn new() -> Self {
        Node { role: Role::Follower(false) }
    }

    // fn send_ping(self) -> FutureResult<Self, ()> {
    //     ok(Client { ping_count: self.ping_count + 1 })
    // }

    // fn receive_pong(self) -> FutureResult<(Self, bool), ()> {
    //     let done = self.ping_count >= 5;
    //     ok((self, done))
    // }

    fn transition_to_candidate(self) -> impl Future<Item = Self, Error = ()>{
        let now = Instant::now();
        let duration = Duration::new(1, 0);
        let when = now + duration;
        Delay::new(when)
            .map(|_| Node { role: Role::Candidate })
            .map_err(|_| ())

    }

    fn do_stuff(self) -> impl Future<Item = Self, Error = ()> {
        match self.role {
            Role::Follower(_) => {
                let fut = self.transition_to_candidate();
                fut.boxed()
            },
            _ => ok(self).boxed(),
        }
    }
}

pub fn ping_til_done() -> impl Future<Item = Node, Error = ()> {
    loop_fn(Node::new(), |node| {
        node.do_stuff()
            .and_then(|node| {
                match node.role {
                    Role::Follower(_voted_yet) => {
                        Ok(Loop::Continue(node))
                    },
                    Role::Candidate => {
                        println!("done!");
                        Ok(Loop::Break(node))
                    }
                }
            })
    })
    // match node.role {
    //     Role::Follower(voted_yet) => {
    //         timeout(1).map(|x| {

    //         }); 
    //     }
    // }
    // client.send_ping()
    //     .and_then(|client| client.receive_pong())
    //     .and_then(|(client, done)| {
    //         if done {
    //             Ok(Loop::Break(client))
    //         } else {
    //             println("looping...");
    //             Ok(Loop::Continue(client))
    //         }
    //     })
}
