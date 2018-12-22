#[macro_use]
extern crate clap;
extern crate tokio;
extern crate tokio_connect;
extern crate prost;
#[macro_use]
extern crate prost_derive;
extern crate tower_h2;
extern crate tower_grpc;
extern crate tower_util;
extern crate tower_http;

mod util;

use failure::Error;
use std::process;
// use std::thread::sleep;
// use std::time::{Duration, Instant};

use std::net::SocketAddr;
use std::net::SocketAddrV4;

use tokio::net::tcp::{ConnectFuture, TcpStream};
use tokio::executor::DefaultExecutor;

use futures::Future;

use tower_h2::client;
use tower_util::MakeService;
use tower_grpc::Request;

pub mod raft {
    include!(concat!(env!("OUT_DIR"), "/raft.rs"));
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

fn do_business(server: SocketAddrV4, candidate: &SocketAddrV4) -> () {
    let h2_settings = Default::default();
    let dst = Dst { addr: server };
    let mut make_client = client::Connect::new(dst, h2_settings, DefaultExecutor::current());
    let fut = make_client.make_service(())
        .map(move |conn| {
            let uri: http::Uri = format!("http://{}:{}", server.ip(), server.port()).parse().unwrap();

            let conn = tower_http::add_origin::Builder::new()
                .uri(uri)
                .build(conn)
                .unwrap();

            raft::client::LeaderElection::new(conn)
        })
        .and_then(|mut client| {
            client.request_vote(Request::new(raft::VoteRequest {
                term: 1,
                candidate: "bla".to_string(),
            })).map_err(|e| panic!("gRPC request failed; err={:?}", e))
        })
        .and_then(|response| {
            println!("RESPONSE = {:?}", response);
            Ok(())
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        });
    tokio::run(fut);
}

fn main() -> () {
    let app = get_cli_app();
    let matches = app.get_matches();
    let server = value_t!(matches, "server", SocketAddrV4).unwrap();
    let candidate = value_t!(matches, "candidate", SocketAddrV4).unwrap();
    do_business(server, &candidate);
    // if let Err(err) = do_business(&server, &candidate) {
    //     bail_out(err);
    // }
}
