use super::state::Role;
use futures::future::Future;
use futures::sync::oneshot::Receiver;
use std::time::{Duration, Instant};
use tokio::timer::Delay;
use tokio::timer::Timeout;

fn race<T>(
    fut_a: impl Future<Item = T, Error = ()>,
    fut_b: impl Future<Item = T, Error = ()>,
) -> impl Future<Item = T, Error = ()> {
    fut_a.select(fut_b).map(|(a, _)| a).map_err(|(a, _)| a)
}

fn delay() -> impl Future<Item = (), Error = ()> {
    let now = Instant::now();
    let duration = Duration::from_millis(3000);
    let when = now + duration;
    Delay::new(when)
        .inspect(|_| println!("3s expired"))
        .map(|_| ())
        .map_err(|_| ())
}

pub fn follow(
    voted_rx: Receiver<()>,
    new_leader_rx: Receiver<()>,
) -> impl Future<Item = Role, Error = ()> {
    let x = voted_rx
        .inspect(|_| println!("vote received"))
        .map_err(|_| ());

    let y = race(x, delay())
        .and_then(|_| delay())
        .map(|_| Role::Candidate(1));

    let z = new_leader_rx
        .inspect(|_| println!("new leader received"))
        .map(|_| Role::Follower(false))
        .map_err(|_| ());

    race(y, z)
}

pub fn bla(voted_rx: Receiver<()>) -> impl Future<Item = (), Error = ()> {
    // let first_timeout = Timeout::new(voted_rx, Duration::from_millis(2000));
    // first_timeout
    //     .or_else(|_| Ok(()))
    //     .and_then(|_| delay())
    let x = voted_rx.map_err(|_| ()).and_then(|_| delay());
    let y = Timeout::new(x, Duration::from_millis(2000)).map_err(|_| ());
    y
}
