extern crate failure;
extern crate futures;
extern crate grpcio;
extern crate protos;

use std::io::Read;
use std::sync::Arc;
use std::{io, thread};

use failure::Error;
use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, Server, ServerBuilder, UnarySink};
use std::process;

use protos::hello::{HelloReply, HelloRequest};
use protos::hello_grpc::{self, Greeter};

#[derive(Clone)]
struct GreeterService;

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

fn start_server() -> Result<Server, Error> {
    let env = Arc::new(Environment::new(1));
    let service = hello_grpc::create_greeter(GreeterService);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 0)
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

fn do_business() -> Result<(), Error> {
    let mut server = start_server()?;
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
    Ok(())
}

fn main() -> () {
    let x = do_business();
    if let Err(err) = x {
        bail_out(err);
    }
}
