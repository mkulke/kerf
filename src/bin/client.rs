extern crate grpcio;
extern crate protos;
#[macro_use]
extern crate failure;

use failure::Error;
use std::env;
use std::process;
use std::sync::Arc;

use grpcio::{ChannelBuilder, EnvBuilder};

use protos::hello::HelloRequest;
use protos::hello_grpc::GreeterClient;

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

fn send(client: GreeterClient, name: String) -> Result<(), Error> {
    let mut req = HelloRequest::new();
    req.set_name(name);
    let reply = client.say_hello(&req)?;
    println!("Client received: {}", reply.get_message());
    Ok(())
}

fn do_business() -> Result<(), Error> {
    assert_args()?;
    let port = get_port()?;
    let name = get_name();
    let client = get_client(port);
    send(client, name)
}

fn main() -> () {
    let x = do_business();
    if let Err(err) = x {
        bail_out(err);
    }
}
