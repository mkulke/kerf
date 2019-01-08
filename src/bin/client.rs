extern crate clap;
extern crate failure;
extern crate kerfuffle;

mod util;

use clap::{App, Arg, crate_version, crate_authors, value_t};
use failure::Error;
use kerfuffle::start_client;
use std::net::SocketAddrV4;
use std::process;

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

fn main() -> () {
    let app = get_cli_app();
    let matches = app.get_matches();
    let server = value_t!(matches, "server", SocketAddrV4).unwrap();
    let candidate = value_t!(matches, "candidate", SocketAddrV4).unwrap();
    // do_business(server, candidate);
    start_client(server, candidate);
    // if let Err(err) = do_business(&server, &candidate) {
    //     bail_out(err);
    // }
}
