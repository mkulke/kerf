extern crate bytes;
extern crate futures;
extern crate prost;
#[macro_use]
extern crate prost_derive;
extern crate tokio;
extern crate tokio_connect;
extern crate tower_grpc;
extern crate tower_h2;
extern crate tower_http;
extern crate tower_util;

use std::cell::{RefCell, RefMut};
use futures::future::lazy;
use futures::future::{loop_fn, Loop};
use futures::stream;
use futures::stream::iter_ok;
// use futures::stream::unfold;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::sync::oneshot::Receiver;
use futures::sync::mpsc::UnboundedSender;
use futures::{future, Future, Stream};
use rand::distributions::{IndependentSample, Range};
use std::net::{SocketAddr, SocketAddrV4};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::executor::DefaultExecutor;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tokio::net::TcpListener;
use tokio::timer::{Delay, Interval};
use tower_grpc::{Request, Response};
use tower_h2::{client, Server};
use tower_util::MakeService;
use std::rc::Rc;

mod loop_test;
mod stream_test;
mod state;

pub mod raft {
    include!(concat!(env!("OUT_DIR"), "/raft.rs"));
}

#[derive(Clone)]
pub struct Config {
    pub listen: SocketAddrV4,
    pub cluster: Vec<SocketAddrV4>,
}

#[derive(Clone, Debug)]
enum VotedFor {
    Candidate(SocketAddrV4),
    NoOne,
}

#[derive(Debug)]
struct State {
    current_term: u64,
    voted_for: Mutex<VotedFor>,
}

#[derive(Clone)]
struct Voter {
    config: Config,
    state: Arc<State>,
}

struct Dst {
    addr: SocketAddrV4,
}

type Leader = Option<SocketAddrV4>;

#[derive(Clone)]
enum Role {
    Follower(bool),
    Candidate(u8),
}

#[derive(Clone)]
struct XState {
    term: u64,
    role: Role,
}

#[derive(Clone)]
struct Node {
    config: Config,
    state: XState,
    tx: UnboundedSender<Message>,
}

// fn transit_state(config: Config, state: XState) -> impl Future<Item = XState, Error = ()> {
//     let now = Instant::now();
//     let duration = Duration::new(1, 0);
//     let when = now + duration;
//     let x = Delay::new(when);
   
//     match state.role {
//         Role::Follower(leader) => {
//             match leader {
//                 Some(_) => future::ok(state),
//                 None => {
//                     let new_state = XState { term: state.term + 1, role: Role::Candidate(1) };
//                     future::ok(new_state)
//                 }
//             }
//         }
//         Role::Candidate(votes) => {
//             let quorum = config.cluster.len() as u8 / 2;
//             if votes > quorum {
//                 let new_state = XState { term: state.term, role: Role::Leader };
//                 return future::ok(new_state)
//             }
//             future::ok(state)
//         }
//         _ => future::ok(state)
//     }
// }

impl tokio_connect::Connect for Dst {
    type Connected = TcpStream;
    type Error = ::std::io::Error;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        let addr = SocketAddr::from(self.addr);
        TcpStream::connect(&addr)
    }
}

impl Voter {
    pub fn new(config: Config) -> Voter {
        let state = Arc::new(State {
            current_term: 0,
            voted_for: Mutex::new(VotedFor::NoOne),
        });

        Voter { config, state }
    }

    fn to_member(&self, candidate: String) -> Option<&SocketAddrV4> {
        self.config
            .cluster
            .iter()
            .find(|member| format!("{}:{}", member.ip(), member.port()) == candidate)
    }
}

impl raft::server::LeaderElection for Voter {
    type RequestVoteFuture = future::FutureResult<Response<raft::VoteReply>, tower_grpc::Error>;

    fn request_vote(&mut self, request: Request<raft::VoteRequest>) -> Self::RequestVoteFuture {
        println!("REQUEST = {:?}", request);

        let message = request.get_ref();
        let candidate = &message.candidate;
        let mut voted_for = self.state.voted_for.lock().unwrap();
        let yes = match self.to_member(candidate.clone()) {
            None => false,
            Some(&member) => match voted_for.deref_mut() {
                // If the receiving node hasn't voted yet in this 
                // term then it votes for the candidate...
                // ...and the node resets its election timeout.
                VotedFor::NoOne => {
                    println!("set voted_for to {}", member);
                    *voted_for = VotedFor::Candidate(member);
                    true
                }
                VotedFor::Candidate(_) => false,
            },
        };

        let response = Response::new(raft::VoteReply { yes });

        future::ok(response)
    }
}

impl Node {
    pub fn new(config: Config, tx: UnboundedSender<Message>) -> Node {
        let state = XState {
            term: 0,
            role: Role::Follower(false),
        };

        Node { config, state, tx }
    }

    fn to_member(&self, candidate: String) -> Option<&SocketAddrV4> {
        self.config
            .cluster
            .iter()
            .find(|member| format!("{}:{}", member.ip(), member.port()) == candidate)
    }
}

impl raft::server::LeaderElection for Node {
    type RequestVoteFuture = future::FutureResult<Response<raft::VoteReply>, tower_grpc::Error>;

    fn request_vote(&mut self, request: Request<raft::VoteRequest>) -> Self::RequestVoteFuture {
        let yes = match self.state.role {
            Role::Follower(voted_yet) => if voted_yet { false } else { true }, 
            _ => false,
        };

        if yes {
            self.state.role = Role::Follower(true);
        }

        let response = Response::new(raft::VoteReply { yes });

        future::ok(response)
    }
}

fn request_vote(
    server: SocketAddrV4,
    candidate: SocketAddrV4,
) -> impl Future<Item = bool, Error = ()> {
    let h2_settings = Default::default();
    let dst = Dst { addr: server };
    let mut make_client = client::Connect::new(dst, h2_settings, DefaultExecutor::current());
    make_client
        .make_service(())
        .map(move |conn| {
            let uri: http::Uri = format!("http://{}:{}", server.ip(), server.port())
                .parse()
                .unwrap();

            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();

            raft::client::LeaderElection::new(conn)
        })
        .and_then(move |mut client| {
            client
                .request_vote(Request::new(raft::VoteRequest {
                    term: 1,
                    candidate: candidate.to_string(),
                }))
                .map_err(|e| panic!("gRPC request failed; err={:?}", e))
        })
        .inspect(|response| {
            println!("RESPONSE = {:?}", response);
        })
        .map(|response| {
            let message = response.get_ref();
            let yes = &message.yes;
            *yes
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        })
}

fn emit(config: Config) -> impl Future<Item = (), Error = ()> {
    interval()
        .inspect(|_| println!("emission"))
        .and_then(move |_| {
            let cluster_stream = iter_ok::<_, ()>(config.cluster.clone());
            let candidate = config.listen.clone();
            let votes = cluster_stream.fold(1, move |acc, addr| {
                request_vote(addr, candidate)
                    .map(move |yes| if yes { acc + 1 } else { acc })
                    .or_else(move |_| Ok(acc))
            });
            votes
        })
        .inspect(|votes| println!("votes: {}", votes))
        .for_each(|_| Ok(()))
}

fn get_random_duration() -> Duration {
    let between = Range::new(0, 4);
    let mut rng = rand::thread_rng();
    let sample = between.ind_sample(&mut rng);
    Duration::new(sample, 0)
}

fn interval() -> impl Stream<Item = (), Error = ()> {
    let now = Instant::now();
    let duration = Duration::new(1, 0);
    Interval::new(now, duration)
        .and_then(|_| {
            let when = Instant::now() + get_random_duration();
            Delay::new(when)
        })
        .map(|_| ())
        .map_err(|_| ())
}

fn listen(node: Node) -> impl Future<Item = (), Error = ()> {
    let listen = node.config.listen;
    let addr = SocketAddr::from(listen);

    let new_service = raft::server::LeaderElectionServer::new(node);
    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, DefaultExecutor::current());

    let bind = TcpListener::bind(&addr).expect("bind");

    bind.incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = h2.serve(sock);
            tokio::spawn(serve.map_err(|e| eprintln!("h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| eprintln!("accept error: {}", e))
}

pub fn start_client(server: SocketAddrV4, candidate: SocketAddrV4) -> () {
    let vote = request_vote(server, candidate).map(|_| ());
    tokio::run(vote);
}

fn mgns_request_vote(
    server: SocketAddrV4,
    candidate: SocketAddrV4,
) -> impl Future<Item = bool, Error = ()> {
    let h2_settings = Default::default();
    let dst = Dst { addr: server };
    let mut make_client = client::Connect::new(dst, h2_settings, DefaultExecutor::current());
    make_client
        .make_service(())
        .map(move |conn| {
            let uri: http::Uri = format!("http://{}:{}", server.ip(), server.port())
                .parse()
                .unwrap();

            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();

            raft::client::LeaderElection::new(conn)
        })
        .and_then(move |mut client| {
            client
                .request_vote(Request::new(raft::VoteRequest {
                    term: 1,
                    candidate: candidate.to_string(),
                }))
                .map_err(|e| panic!("gRPC request failed; err={:?}", e))
        })
        .inspect(|response| {
            println!("RESPONSE = {:?}", response);
        })
        .map(|response| {
            let message = response.get_ref();
            let yes = &message.yes;
            *yes
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        })
}

// Once a candidate has a majority of votes it becomes leader.
// The leader begins sending out Append Entries messages to its followers.
// These messages are sent in intervals specified by the heartbeat timeout.
// Followers then respond to each Append Entries message.
// This election term will continue until a follower stops receiving
// heartbeats and becomes a candidate.
enum Message {
    ElectionTimeout,
    Voted,
}

// The election timeout is the amount of time a follower waits 
// until becoming a candidate.
// The election timeout is randomized to be between 150ms and 300ms.
fn timeout(seconds: u64) -> impl Future<Item = (), Error = ()> {
    let now = Instant::now();
    let duration = Duration::new(seconds, 0);
    let when = now + duration;
    Delay::new(when).map(|_| ()).map_err(|_| ())
}

pub fn start_server(config: Config) -> () {
    tokio::run(lazy(move || {
        let (tx, rx) = mpsc::unbounded();
        let node = Node::new(config, tx);

        let serve = listen(node);
        tokio::spawn(serve);
      
        // let initial_state = XState { term: 0, role: Role::Follower };
        // After the election timeout the follower becomes a candidate and
        // starts a new election term...
        // let a = tx.clone();
        // let election_timeout = timeout(2).inspect(move |_| {
        //     a.unbounded_send(Message::ElectionTimeout).unwrap()
        // }).map(|_| ());
        // tokio::spawn(election_timeout);

        let (voted_tx, voted_rx) = oneshot::channel();
        let voted = timeout(1).inspect(move |_| {
            println!("voted");
            voted_tx.send(()).unwrap();
        }).map(|_| ());
        tokio::spawn(voted);

        let (new_leader_tx, new_leader_rx) = oneshot::channel();
        let new_leader = timeout(2).inspect(move |_| {
            println!("new leader");
            new_leader_tx.send(()).unwrap();
        }).map(|_| ());
        tokio::spawn(new_leader);

        // let stream_test = stream_test::follow(voted_rx, new_leader_rx).inspect(|role| {
        //     match role {
        //         state::Role::Follower(_) => println!("follower"),
        //         state::Role::Candidate(_) => println!("candidate"),
        //     }
        // }).map(|_| ());
        // tokio::spawn(stream_test);

        // let voted_rx_cell = RefCell::new(&voted_rx);
        // let new_leader_rx_cell = RefCell::new(&new_leader_rx);

        // let bla = loop_fn(false, |done| {
        //     let fut = stream_test::follow(voted_rx_cell.borrow_mut(), &mut new_leader_rx_cell.borrow_mut())
        //         .and_then(move |_| {
        //             if done {
        //                 Ok(Loop::Break(true))
        //             } else {
        //                 Ok(Loop::Continue(true))
        //             }
        //         });
        //     fut
        // });
        //         state::Role::Follower(_) => {
        //             let fut = stream_test::follow(voted_rx, new_leader_rx)
        //                 .map(|new_role| ((), new_role));
        //             Some(fut)
        //         },
        //         state::Role::Candidate(_) => None,

        // let blub = move |role| {
        //     match role {
        //         state::Role::Follower(_) => {
        //             let fut = stream_test::follow(voted_rx, new_leader_rx)
        //                 .map(|new_role| ((), new_role));
        //             Some(fut)
        //         },
        //         state::Role::Candidate(_) => None,
        //     }
        // };

        fn some_fn(a: Receiver<()>) -> impl Future<Item = (), Error = ()> {
            a.map_err(|_| ())
        }

        let (_, oneshot_rx) = oneshot::channel();
        let oneshot_rx_refcell_rc = Rc::new(RefCell::new(&oneshot_rx));
        let bla = stream::unfold(false, |state| {
            match state {
                false => {
                    let x = Rc::clone(&oneshot_rx_refcell_rc);
                    let fut = *x.borrow_mut().map(|_: ()| ((), true));
                    // let fut = oneshot_rx.map(|_: ()| ((), true));
                    Some(fut)
                },
                true => None,
            }
        });
        

        // let bla = unfold(state::Role::Follower(false), blub)
        //     .for_each(|_| {
        //         println!("whop");
        //         Ok(())
        //     });
        // tokio::spawn(bla);

        // let receiver  = rx.for_each(|message| {
        //     match message {
        //         Message::ElectionTimeout => {
        //             println!("election timeout");
        //         }
        //         Message::Voted => {
        //             println!("voted");
        //         }
        //     }
        //     Ok(())
        // }).map_err(|e| println!("error = {:?}", e));
        // tokio::spawn(receiver);

        Ok(())
    }))
}
