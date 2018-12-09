#[macro_use]
extern crate clap;
extern crate failure;
extern crate futures;
extern crate grpcio;
extern crate protos;
extern crate rand;
extern crate tokio;

use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::distributions::{IndependentSample, Range};

use tokio::timer::Delay;

use failure::Error;

use std::io::Read;
use std::{io, thread};

use futures::future::lazy;
use futures::sync::mpsc::UnboundedSender;
use futures::sync::{mpsc, oneshot};
use futures::Future;

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

#[derive(Clone)]
struct LeaderElectionService {
    config: Config,
    current_term: u64,
    voted_for: VotedFor,
    tx: UnboundedSender<Message>,
}

enum Message {
    VotedFor(SocketAddrV4),
}

fn is_host_port(val: String) -> Result<(), String> {
    match val.parse::<SocketAddrV4>() {
        Ok(_) => Ok(()),
        _ => Err(String::from("wrong format")),
    }
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
                .validator(is_host_port),
        )
        .arg(
            Arg::with_name("cluster")
                .long("cluster")
                .short("c")
                .required(true)
                .multiple(true)
                .takes_value(true)
                .validator(is_host_port),
        )
}

impl LeaderElectionService {
    fn to_member(&self, candidate: String) -> Option<&SocketAddrV4> {
        self.config
            .cluster
            .iter()
            .find(|member| format!("{}:{}", member.ip(), member.port()) == candidate)
    }

    fn vote_yes(
        &self,
        member: &SocketAddrV4,
        sink: UnarySink<VoteReply>,
    ) -> Box<Future<Item = (), Error = grpcio::Error> + Send> {
        let mut rep = VoteReply::new();
        let tx = self.tx.clone();
        rep.set_yes(true);
        let message = Message::VotedFor(*member);
        Box::new(sink.success(rep.clone()).inspect(move |_| {
            let _ = tx.unbounded_send(message);
        }))
    }
}

impl LeaderElection for LeaderElectionService {
    fn request_vote(
        &mut self,
        ctx: RpcContext,
        req: VoteRequest,
        sink: UnarySink<VoteReply>,
    ) -> () {
        let mut rep = VoteReply::new();
        let fut = match &self.voted_for {
            VotedFor::Candidate(_) => {
                rep.set_yes(false);
                Box::new(sink.success(rep.clone()))
            }
            VotedFor::NoOne => {
                let member = self.to_member(req.candidate);
                match member {
                    None => Box::new(sink.fail(RpcStatus::new(
                        RpcStatusCode::InvalidArgument,
                        Some("candidate not in member list".to_string()),
                    ))),
                    Some(m) => self.vote_yes(m, sink),
                }
            }
        };
        ctx.spawn(fut.map_err(|err| eprintln!("Failed to reply: {:?}", err)))
    }
}

fn start_server(config: Config) -> Result<Server, Error> {
    let env = Arc::new(Environment::new(1));
    let (tx, _rx) = mpsc::unbounded();
    let ip = config.listen.ip().to_string();
    let port = config.listen.port();
    let service = LeaderElectionService {
        tx: tx,
        config: config,
        current_term: 0,
        voted_for: VotedFor::NoOne,
    };
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

fn request_vote(addr: &SocketAddrV4) -> impl Future<Item = bool, Error = ()> {
    use futures::future::{ok, Either};

    let client = get_raft_client(addr);
    let mut req = VoteRequest::new();
    req.set_term(1);
    let res = client.request_vote_async(&req);
    let ip = *addr.ip();
    match res {
        Ok(y) => Either::A(
            y.map(|z| z.get_yes())
                .map_err(move |e| {
                    eprintln!("Failed to send request to {:?}: {:?}", ip, e);
                    ()
                })
                .or_else(|_| Ok(false)),
        ),
        Err(_) => Either::B(ok::<bool, ()>(false)),
    }
}

fn get_random_duration() -> Duration {
    let between = Range::new(1, 5);
    let mut rng = rand::thread_rng();
    let sample = between.ind_sample(&mut rng);
    Duration::new(sample, 0)
}

fn do_business(cluster: Vec<SocketAddrV4>) -> Result<(), Error> {
    tokio::run(lazy(move || {
        cluster.iter().for_each(|addr| {
            let now = Instant::now();
            let duration = get_random_duration();
            let delay = Delay::new(now + duration).map_err(|_| ());
            let vote = request_vote(addr);
            tokio::spawn(delay.and_then(|_| vote).map(|_| ()));
        });
        Ok(())
    }));
    Ok(())
}

fn main() -> () {
    let app = get_cli_app();
    let matches = app.get_matches();
    let listen = value_t!(matches, "listen", SocketAddrV4).unwrap();
    let cluster = values_t!(matches, "cluster", SocketAddrV4).unwrap();
    let config = Config { listen, cluster };
    // if let Err(ref err) = start_server(config.clone()) {
    //     bail_out(err);
    // }
    // if let Err(ref err) = do_business(config.clone().cluster) {
    //     bail_out(err);
    // }
    match start_server(config.clone()) {
        Err(ref err) => {
            bail_out(err);
        }
        Ok(mut server) => {
            let (tx, rx) = oneshot::channel();
            thread::spawn(move || {
                println!("Press ENTER to exit...");
                let _ = io::stdin().read(&mut [0]).unwrap();
                tx.send(())
            });
            let _ = rx.wait();
            let _ = server.shutdown().wait();
        }
    }
}
