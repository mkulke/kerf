#[macro_use]
extern crate clap;
extern crate failure;
extern crate futures;
extern crate grpcio;
extern crate protos;
extern crate rand;
extern crate tokio;

mod util;

use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::distributions::{IndependentSample, Range};

// use tokio::timer::Delay;
use tokio::timer::Interval;

use failure::Error;

use futures::future::lazy;
// use futures::sync::mpsc;
// use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{Future, Stream};
// use futures::Future;

use grpcio::{ChannelBuilder, EnvBuilder};
use grpcio::{Environment, RpcContext, RpcStatus, RpcStatusCode, Server, ServerBuilder, UnarySink};

use std::process;

use protos::raft::{VoteReply, VoteRequest};
use protos::raft_grpc::{self, LeaderElection};
// use protos::raft::VoteRequest;
use protos::raft_grpc::LeaderElectionClient;

use clap::{App, Arg};

#[derive(Clone)]
struct Config {
    listen: SocketAddrV4,
    cluster: Vec<SocketAddrV4>,
}

#[derive(Clone)]
enum VotedFor {
    Candidate(SocketAddrV4),
    NoOne,
}

fn get_cli_app<'a, 'b>() -> App<'a, 'b> {
    App::new("kerfuffle")
        .version(crate_version!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("listen")
                .long("listen")
                .short("l")
                .required(true)
                .takes_value(true)
                .validator(util::is_host_port),
        )
        .arg(
            Arg::with_name("cluster")
                .long("cluster")
                .short("c")
                .required(true)
                .multiple(true)
                .takes_value(true)
                .validator(util::is_host_port),
        )
}

#[derive(Clone)]
struct LeaderElectionService {
    config: Config,
    current_term: u64,
    voted_for: VotedFor,
}

impl LeaderElectionService {
    fn new(config: Config) -> LeaderElectionService {
        LeaderElectionService {
            config: config,
            current_term: 0,
            voted_for: VotedFor::NoOne,
        }
    }

    fn to_member(&self, candidate: String) -> Option<&SocketAddrV4> {
        self.config
            .cluster
            .iter()
            .find(|member| format!("{}:{}", member.ip(), member.port()) == candidate)
    }
}

impl LeaderElection for LeaderElectionService {
    fn request_vote(
        &mut self,
        ctx: RpcContext,
        req: VoteRequest,
        sink: UnarySink<VoteReply>,
    ) -> () {
        let fut = match self.to_member(req.candidate) {
            None => Box::new(sink.fail(RpcStatus::new(
                RpcStatusCode::InvalidArgument,
                Some("candidate not in member list".to_string()),
            ))),
            Some(&member) => {
                let mut rep = VoteReply::new();
                let voted_for = &self.voted_for;
                match voted_for {
                    VotedFor::Candidate(_) => {
                        rep.set_yes(false);
                        println!("rejected vote for {}", member);
                        Box::new(sink.success(rep.clone()))
                    }
                    VotedFor::NoOne => {
                        rep.set_yes(true);
                        self.voted_for = VotedFor::Candidate(member);
                        println!("voted for {}", member);
                        Box::new(sink.success(rep.clone()))
                    }
                }
            }
        };
        ctx.spawn(fut.map_err(|err| eprintln!("Failed to reply: {:?}", err)))
    }
}

fn start_server(config: Config) -> Result<Server, Error> {
    let env = Arc::new(Environment::new(1));
    let ip = config.listen.ip().to_string();
    let port = config.listen.port();
    let service = LeaderElectionService::new(config);
    let grpc_service = raft_grpc::create_leader_election(service);
    let mut server = ServerBuilder::new(env)
        .register_service(grpc_service)
        .bind(ip, port)
        .build()?;
    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    Ok(server)
}

fn bail_out(err: &Error) -> () {
    eprintln!("{}", err);
    process::exit(1);
}

fn get_raft_client(addr: &SocketAddrV4) -> LeaderElectionClient {
    let ip = addr.ip();
    let port = addr.port();
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(format!("{}:{}", ip, port).as_str());
    LeaderElectionClient::new(ch)
}

fn request_vote(client: LeaderElectionClient, addr: &SocketAddrV4) -> impl Future<Item = bool, Error = ()> {
    use futures::future::{ok, Either};

    // let client = get_raft_client(addr);
    let mut req = VoteRequest::new();
    req.set_term(1);
    req.set_candidate(format!("{}:{}", addr.ip(), addr.port()));
    let res = client.request_vote_async(&req); 
    let a = addr.clone();
    match res {
        Ok(y) => Either::A(
            y.map(|z| z.get_yes())
                .map_err(move |e| {
                    eprintln!("Failed to send vote request to {:?} {:?}", a, e);
                    ()
                })
                .or_else(|_| Ok(false)),
        ),
        Err(_) => Either::B(ok::<bool, ()>(false)),
    }
}

fn get_random_duration() -> Duration {
    let between = Range::new(10, 15);
    let mut rng = rand::thread_rng();
    let sample = between.ind_sample(&mut rng);
    Duration::new(sample, 0)
}

fn do_business(config: Config) -> () {
    tokio::run(lazy(move || {
        let now = Instant::now();
        let duration = get_random_duration();
        let interval = Interval::new(now, duration)
            .for_each(move |_| {
                config.cluster.iter().for_each(|addr| {
                    let client = get_raft_client(addr);
                    let vote = request_vote(client, &config.listen).map(|_| ());
                    tokio::spawn(vote);
                });
                Ok(())
            })
            .map_err(|_| ());
        tokio::spawn(interval);
        Ok(())
    }));
}

fn main() -> () {
    let app = get_cli_app();
    let matches = app.get_matches();
    let listen = value_t!(matches, "listen", SocketAddrV4).unwrap();
    let cluster = values_t!(matches, "cluster", SocketAddrV4).unwrap();
    let config = Config { listen, cluster };
    // let (tx, rx) = mpsc::unbounded();
    match start_server(config.clone()) {
        Err(ref err) => {
            bail_out(err);
        }
        Ok(_) => {
            let _ = do_business(config);
        }
    }
}
