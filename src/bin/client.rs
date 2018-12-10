#[macro_use]
extern crate clap;
extern crate grpcio;
extern crate protos;
extern crate tokio;

mod util;

use failure::Error;
use std::process;
use std::sync::Arc;
// use std::thread::sleep;
// use std::time::{Duration, Instant};

use std::net::SocketAddrV4;

use grpcio::{ChannelBuilder, EnvBuilder};

// use tokio::prelude::{Future, Stream};
// use tokio::timer::Interval;

use protos::raft::VoteRequest;
use protos::raft_grpc::LeaderElectionClient;

use clap::{App, Arg};

fn bail_out(err: Error) -> () {
    eprintln!("{}", err);
    process::exit(1);
}

fn get_cli_app<'a, 'b>() -> App<'a, 'b> {
    App::new("kerfuffle client")
        .version(crate_version!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("server")
                .long("server")
                .short("s")
                .required(true)
                .takes_value(true)
                .validator(util::is_host_port),
        )
        .arg(
            Arg::with_name("candidate")
                .long("candidate")
                .short("c")
                .required(true)
                .takes_value(true)
                .validator(util::is_host_port),
        )
}

fn get_raft_client(host: String, port: String) -> LeaderElectionClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(format!("{}:{}", host, port).as_str());
    LeaderElectionClient::new(ch)
}

fn request_vote(client: LeaderElectionClient, candidate: &SocketAddrV4) -> Result<bool, Error> {
    let mut req = VoteRequest::new();
    req.set_term(1);
    req.set_candidate(format!("{}:{}", candidate.ip(), candidate.port()));
    let reply = client.request_vote(&req)?;
    let yes = reply.get_yes();
    println!("Client received: {}", yes);
    Ok(yes)
}

fn do_business(server: &SocketAddrV4, candidate: &SocketAddrV4) -> Result<(), Error> {
    let client = get_raft_client(server.ip().to_string(), server.port().to_string());
    request_vote(client, candidate)?;
    Ok(())
}

fn main() -> () {
    let app = get_cli_app();
    let matches = app.get_matches();
    let server = value_t!(matches, "server", SocketAddrV4).unwrap();
    let candidate = value_t!(matches, "candidate", SocketAddrV4).unwrap();
    if let Err(err) = do_business(&server, &candidate) {
        bail_out(err);
    }
}
