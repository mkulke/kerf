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

use futures::future::lazy;
use futures::stream::iter_ok;
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

pub fn start_client(server: SocketAddrV4, candidate: SocketAddrV4) -> () {
    let vote = request_vote(server, candidate).map(|_| ());
    tokio::run(vote);
}

pub fn start_server(config: Config) -> () {
    tokio::run(lazy(move || {
        let serve = listen(config.clone());
        tokio::spawn(serve);
        let emissions = interval()
            .inspect(|_| println!("emission"))
            .and_then(move |_| {
                let cluster_stream = iter_ok::<_, ()>(config.cluster.clone());
                let candidate = config.listen.clone();
                let votes = cluster_stream.fold(1, move |acc, addr| {
                    request_vote(addr, candidate)
                        .map(move |yes| if yes { acc + 1 } else { acc })
                });
                votes
            })
            .for_each(|_| Ok(()));
        tokio::spawn(emissions);
        Ok(())
    }))
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

fn listen(config: Config) -> impl Future<Item = (), Error = ()> {
    let addr = SocketAddr::from(config.listen);
    let voter = Voter::new(config);

    let new_service = raft::server::LeaderElectionServer::new(voter);
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
