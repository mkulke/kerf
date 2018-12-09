extern crate grpcio;
extern crate protos;
extern crate tokio;
#[macro_use]
extern crate failure;

use failure::Error;
use std::env;
use std::process;
use std::sync::Arc;
// use std::thread::sleep;
// use std::time::{Duration, Instant};

use grpcio::{ChannelBuilder, EnvBuilder};

// use tokio::prelude::{Future, Stream};
// use tokio::timer::Interval;

use protos::raft::VoteRequest;
use protos::raft_grpc::LeaderElectionClient;

#[derive(Debug, Fail)]
enum DomainError {
    #[fail(display = "{} is not a valid port number", port)]
    NoPortNumber { port: String },
    #[fail(display = "expected 1 argument")]
    ArgumentError,
}

fn bail_out(err: Error) -> () {
    eprintln!("{}", err);
    process::exit(1);
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

// fn get_client(port: String) -> GreeterClient {
//     let env = Arc::new(EnvBuilder::new().build());
//     let ch = ChannelBuilder::new(env).connect(format!("localhost:{}", port).as_str());
//     GreeterClient::new(ch)
// }

fn get_raft_client(host: String, port: String) -> LeaderElectionClient {
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(format!("{}:{}", host, port).as_str());
    LeaderElectionClient::new(ch)
}

fn request_vote(client: LeaderElectionClient) -> Result<bool, Error> {
    let mut req = VoteRequest::new();
    req.set_term(1);
    req.set_candidate("123.0.0.1:2345".to_string());
    let reply = client.request_vote(&req)?;
    let yes = reply.get_yes();
    println!("Client received: {}", yes);
    Ok(yes)
}

fn do_business() -> Result<(), Error> {
    let port = get_port()?;
    // let _name = get_name();
    // let client = get_client(port);
    // send(client, name)?;
    let client = get_raft_client(String::from("localhost"), port);
    request_vote(client)?;
    Ok(())
}

fn main() -> () {
    if let Err(err) = do_business() {
        bail_out(err);
    }
    // let task = Interval::new(Instant::now(), Duration::from_millis(100))
    //     .take(10)
    //     .for_each(|instant| {
    //         println!("fire; instant={:?}", instant);
    //         Ok(())
    //     }).map_err(|e| panic!("interval errored; err={:?}", e));
    // tokio::run(task);
    // loop {
    //     sleep(Duration::new(2, 0));
    //     let x = do_business();
    //     if let Err(err) = x {
    //         bail_out(err);
    //     }
    // }
}
