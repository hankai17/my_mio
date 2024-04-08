extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job, Registration, SetReadiness};
use my_mio::net::{TcpListener, TcpStream, EventLoop, EventLoopBuilder, Acceptor, TcpConnection, TcpServer, Handler};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};
use std::time::Duration;
use my_mio::timer::{self, Timer, Timeout};

const TIMER: Token = Token(usize::MAX - 2);

fn main() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);

    let mut t: Timer<i32> = timer::Builder::default()
        .tick_duration(Duration::from_millis(100))
        .num_slots(1024)
        .capacity(65536)
        .build();

    let job2 = Box::new(move |val: i64| {});
    poll.register(&t, TIMER, Ready::readable(), PollOpt::edge(), job2);

    //t.set_timeout(Duration::from_millis(1000 * 10), 10 as i32);
    //t.set_timeout(Duration::from_millis(1000 * 3), 3 as i32);
    t.set_timeout(Duration::from_millis(1000 * 2), 2 as i32);
    t.set_timeout(Duration::from_millis(1000 * 3), 3 as i32);
    t.set_timeout(Duration::from_millis(1000 * 4), 4 as i32);
    t.set_timeout(Duration::from_millis(1000 * 5), 5 as i32);
    t.set_timeout(Duration::from_millis(1000 * 6), 6 as i32);

    loop {
        let n = poll.poll(&mut events, None).unwrap();
        if n == 0 {
            continue;
        }
        assert_eq!(n, 1);
        //assert_eq!(events.get(0).unwrap().token(), Token(111));
        //break;
        while let Some(t) = t.poll() {
            println!("--->t: {}", t);
        }
    }
    println!("done");
}

