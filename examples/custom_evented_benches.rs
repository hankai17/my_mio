extern crate my_mio;

use my_mio::{channel, Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use my_mio::event::Evented;
use std::time::Duration;
use std::thread;
use std::sync::Arc;

fn main1() {
    const NUM: usize = 10_000 * 10;
    const THREADS: usize = 4;

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let mut registrations = vec![];
    let mut set_readiness = vec![];

    for i in 0..NUM {
        let (r, s) = Registration::new( &poll, Token(i), Ready::readable(), PollOpt::edge()); // 用 let (_, s) 接 则立即drop
        registrations.push(r);
        set_readiness.push(s);
    }

    let set_readiness = Arc::new(set_readiness);
    for mut i in 0..THREADS {
        let set_readiness = set_readiness.clone();
        thread::spawn(move || {
            while i < NUM {
                set_readiness[i].set_readiness(Ready::readable()).unwrap();
                i += THREADS;
            }
        });
    }
    let mut n = 0;
    while n < NUM {
        n += poll.poll(&mut events, None).unwrap();
    }
    println!("n: {}", n);
}

fn main() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    let (r, s) = Registration::new( &poll, Token(0), Ready::readable(), PollOpt::edge()); // 用let (_, s)接 则立即析构(drop) 标记为drop并入队 set_readiness入队发现为drop态则不能置位
    s.set_readiness(Ready::readable()).unwrap();
    //println!("after clone poll.readiness_queue.inner use_count2: {}", Arc::strong_count(&poll.readiness_queue.inner));

    let mut n = 0;
    n = poll.poll(&mut events, None).unwrap();
    println!("n: {}", n);
}
