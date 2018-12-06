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

use futures::future::lazy;
use futures::Future;

use grpcio::{ChannelBuilder, EnvBuilder};
use grpcio::{Environment, RpcContext, Server, ServerBuilder, UnarySink};

use std::process;

use protos::hello::{HelloReply, HelloRequest};
use protos::hello_grpc::Greeter;

use protos::raft::{VoteReply, VoteRequest};
use protos::raft_grpc::{self, LeaderElection};

// use protos::raft::VoteRequest;
use protos::raft_grpc::LeaderElectionClient;

use clap::{App, Arg};

struct Config {
    listen: SocketAddrV4,
    cluster: Vec<SocketAddrV4>,
}

#[derive(Clone)]
struct GreeterService;

#[derive(Clone)]
struct LeaderElectionService;

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
        ).arg(
            Arg::with_name("cluster")
                .long("cluster")
                .short("c")
                .required(true)
                .multiple(true)
                .takes_value(true)
                .validator(is_host_port),
        )
}

impl LeaderElection for LeaderElectionService {
    fn request_vote(
        &mut self,
        ctx: RpcContext,
        _req: VoteRequest,
        sink: UnarySink<VoteReply>,
    ) -> () {
        let mut reply = VoteReply::new();
        reply.set_yes(true);
        let f = sink
            .success(reply.clone())
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
}

impl Greeter for GreeterService {
    fn say_hello(&mut self, ctx: RpcContext, req: HelloRequest, sink: UnarySink<HelloReply>) -> () {
        println!("Received HelloRequest {{ {:?} }}", req);
        let mut reply = HelloReply::new();
        reply.set_message(format!("Hello {}!", req.get_name()));
        let f = sink
            .success(reply.clone())
            .map(move |_| println!("Responded with HelloReply {{ {:?} }}", reply))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
}

fn start_server(listen: SocketAddrV4) -> Result<Server, Error> {
    let env = Arc::new(Environment::new(1));
    // let service = hello_grpc::create_greeter(GreeterService);
    let service = raft_grpc::create_leader_election(LeaderElectionService);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(listen.ip().to_string(), listen.port())
        // .bind("127.0.0.1", 0)
        .build()?;
    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    Ok(server)
}

fn bail_out(err: Error) -> () {
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
    let ip = addr.ip().clone();
    match res {
        Ok(y) => Either::A(
            y.map(|z| z.get_yes())
                .map_err(move |e| {
                    eprintln!("Failed to send request to {:?}: {:?}", ip, e);
                    ()
                }).or_else(|_| Ok(false)),
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
    if let Err(err) = start_server(config.listen) {
        bail_out(err);
    }
    if let Err(err) = do_business(config.cluster) {
        bail_out(err);
    }
}
