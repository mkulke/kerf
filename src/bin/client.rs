extern crate grpcio;
extern crate protos;
extern crate tokio;
#[macro_use]
extern crate failure;

use failure::Error;
use std::env;
use std::process;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use grpcio::{ChannelBuilder, EnvBuilder};

use tokio::prelude::{Future, Stream};
use tokio::timer::Interval;

use protos::hello::HelloRequest;
use protos::hello_grpc::GreeterClient;

use protos::raft::VoteRequest;
use protos::raft_grpc::LeaderElectionClient;

#[derive(Debug, Fail)]
enum DomainError {
    #[fail(display = "{} is not a valid port number", port)]
    NoPortNumber { port: String },
    #[fail(display = "expected 1 or 2 arguments")]
    ArgumentError,
}

fn bail_out(err: Error) -> () {
    eprintln!("{}", err);
    process::exit(1);
}

fn assert_args() -> Result<(), DomainError> {
    let args = env::args().collect::<Vec<_>>();
    match args.len() {
        1...3 => Ok(()),
        _ => Err(DomainError::ArgumentError),
    }
}

fn get_name() -> String {
    let args = env::args().collect::<Vec<_>>();
    match args.get(2) {
        Some(name) => name.to_owned(),
        _ => "John Doe".to_string(),
    }
}

fn get_port() -> Result<String, DomainError> {
    let args = env::args().collect::<Vec<_>>();
    let first_arg = args.get(1).ok_or(DomainError::ArgumentError)?;
    first_arg
        .parse::<u16>()
        .map_err(|_| DomainError::NoPortNumber {
            port: first_arg.to_string(),
        })?;
    Ok(first_arg.to_string())
}

fn get_client(port: String) -> GreeterClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(format!("localhost:{}", port).as_str());
    GreeterClient::new(ch)
}

fn get_raft_client(host: String, port: String) -> LeaderElectionClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(format!("{}:{}", host, port).as_str());
    LeaderElectionClient::new(ch)
}

fn send(client: GreeterClient, name: String) -> Result<(), Error> {
    let mut req = HelloRequest::new();
    req.set_name(name);
    let reply = client.say_hello(&req)?;
    println!("Client received: {}", reply.get_message());
    Ok(())
}

fn request_vote(client: LeaderElectionClient) -> Result<bool, Error> {
    let mut req = VoteRequest::new();
    req.set_term(1);
    let reply = client.request_vote(&req)?;
    let yes = reply.get_yes();
    println!("Client received: {}", yes);
    Ok(yes)
}

fn do_business() -> Result<(), Error> {
    assert_args()?;
    let port = get_port()?;
    let _name = get_name();
    // let client = get_client(port);
    // send(client, name)?;
    let client = get_raft_client(String::from("localhost"), port);
    request_vote(client)?;
    Ok(())
}

fn main() -> () {
    // let task = Interval::new(Instant::now(), Duration::from_millis(100))
    //     .take(10)
    //     .for_each(|instant| {
    //         println!("fire; instant={:?}", instant);
    //         Ok(())
    //     }).map_err(|e| panic!("interval errored; err={:?}", e));
    // tokio::run(task);
    loop {
        sleep(Duration::new(2, 0));
        let x = do_business();
        if let Err(err) = x {
            bail_out(err);
        }
    }
}
