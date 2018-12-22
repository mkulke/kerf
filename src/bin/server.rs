#[macro_use]
extern crate clap;
extern crate failure;
extern crate bytes;
extern crate futures;
extern crate prost;
#[macro_use]
extern crate prost_derive;
extern crate tokio;
extern crate tower_h2;
extern crate tower_grpc;

mod util;

pub mod raft {
    include!(concat!(env!("OUT_DIR"), "/raft.rs"));
}

use std::net::SocketAddrV4;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::distributions::{IndependentSample, Range};

// use tokio::timer::Delay;
use tokio::timer::Interval;
use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;

use failure::Error;

use futures::future::lazy;
use futures::future;
// use futures::sync::mpsc;
// use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::{Future, Stream};
// use futures::Future;

// use grpcio::{ChannelBuilder, EnvBuilder};
// use grpcio::{Environment, RpcContext, RpcStatus, RpcStatusCode, Server, ServerBuilder, UnarySink};

use tower_grpc::{Request, Response};
use tower_h2::Server;

use std::process;

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

#[derive(Clone, Debug)]
struct Elect;

impl raft::server::LeaderElection for Elect {
    type RequestVoteFuture = future::FutureResult<Response<raft::VoteReply>, tower_grpc::Error>;

    fn request_vote(&mut self, request: Request<raft::VoteRequest>) -> Self::RequestVoteFuture {
        println!("REQUEST = {:?}", request);

        let response = Response::new(raft::VoteReply {
            yes: true,
        });

        future::ok(response)
    }
}

// #[derive(Clone)]
// struct LeaderElectionService {
//     config: Config,
//     current_term: u64,
//     voted_for: VotedFor,
// }

// impl LeaderElectionService {
//     fn new(config: Config) -> LeaderElectionService {
//         LeaderElectionService {
//             config: config,
//             current_term: 0,
//             voted_for: VotedFor::NoOne,
//         }
//     }

//     fn to_member(&self, candidate: String) -> Option<&SocketAddrV4> {
//         self.config
//             .cluster
//             .iter()
//             .find(|member| format!("{}:{}", member.ip(), member.port()) == candidate)
//     }
// }

// impl LeaderElection for LeaderElectionService {
//     fn request_vote(
//         &mut self,
//         ctx: RpcContext,
//         req: VoteRequest,
//         sink: UnarySink<VoteReply>,
//     ) -> () {
//         let fut = match self.to_member(req.candidate) {
//             None => Box::new(sink.fail(RpcStatus::new(
//                 RpcStatusCode::InvalidArgument,
//                 Some("candidate not in member list".to_string()),
//             ))),
//             Some(&member) => {
//                 let mut rep = VoteReply::new();
//                 let voted_for = &self.voted_for;
//                 match voted_for {
//                     VotedFor::Candidate(_) => {
//                         rep.set_yes(false);
//                         println!("rejected vote for {}", member);
//                         Box::new(sink.success(rep.clone()))
//                     }
//                     VotedFor::NoOne => {
//                         rep.set_yes(true);
//                         self.voted_for = VotedFor::Candidate(member);
//                         println!("voted for {}", member);
//                         Box::new(sink.success(rep.clone()))
//                     }
//                 }
//             }
//         };
//         ctx.spawn(fut.map_err(|err| eprintln!("Failed to reply: {:?}", err)))
//     }
// }

// fn start_server(config: Config) -> Result<Server, Error> {
//     let env = Arc::new(Environment::new(1));
//     let ip = config.listen.ip().to_string();
//     let port = config.listen.port();
//     let service = LeaderElectionService::new(config);
//     let grpc_service = raft_grpc::create_leader_election(service);
//     let mut server = ServerBuilder::new(env)
//         .register_service(grpc_service)
//         .bind(ip, port)
//         .build()?;
//     server.start();
//     for &(ref host, port) in server.bind_addrs() {
//         println!("listening on {}:{}", host, port);
//     }
//     Ok(server)
// }

fn bail_out(err: &Error) -> () {
    eprintln!("{}", err);
    process::exit(1);
}

fn get_random_duration() -> Duration {
    let between = Range::new(10, 15);
    let mut rng = rand::thread_rng();
    let sample = between.ind_sample(&mut rng);
    Duration::new(sample, 0)
}

fn do_business(config: Config) -> () {
    let new_service = raft::server::LeaderElectionServer::new(Elect);

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, DefaultExecutor::current());

    let addr = SocketAddr::from(config.listen);
    let bind = TcpListener::bind(&addr).expect("bind");

    let serve = bind.incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = h2.serve(sock);
            tokio::spawn(serve.map_err(|e| eprintln!("h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| eprintln!("accept error: {}", e));

    tokio::run(serve)
}

fn main() -> () {
    let app = get_cli_app();
    let matches = app.get_matches();
    let listen = value_t!(matches, "listen", SocketAddrV4).unwrap();
    let cluster = values_t!(matches, "cluster", SocketAddrV4).unwrap();
    let config = Config { listen, cluster };
    // let (tx, rx) = mpsc::unbounded();
    do_business(config);
    // match start_server(config.clone()) {
    //     Err(ref err) => {
    //         bail_out(err);
    //     }
    //     Ok(_) => {
    //         let _ = do_business(config);
    //     }
    // }
}
