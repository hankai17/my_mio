extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::net::{TcpListener, TcpStream, EventLoop, EventLoopBuilder, Acceptor, TcpConnection};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};

fn sleep_ms(ms: u64) {
    use std::thread;
    thread::sleep(Duration::from_millis(ms));
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {
    println!("--------------");
    println!("accept {}", addr);
    /*
    let mut conn = Arc::new(Mutex::new(TcpConnection::new(self.event_loop.clone(), stream)));
    let clone = conn.clone();
    let job = Box::new(move |val: i64| { clone.lock().unwrap().handleRead(val); });
    self.event_loop.register(&conn.lock().unwrap().sock, SERVER, Ready::readable(), 
            PollOpt::edge(), job);
            */
}

fn main() {
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let mut event_loop = b.get_build().unwrap();
    let mut acceptor = Arc::new (
        Mutex::new (
            Acceptor::new(event_loop.clone(), &"0.0.0.0:9527".to_string())
        )
    );

    let clone = acceptor.clone();
    let job = Box::new(move |val: i64| { clone.lock().unwrap().handleRead(val); });
    acceptor.lock().unwrap().bind(job);
    acceptor.lock().unwrap().set_accept_cb(default_accept_cb);
    event_loop.lock().unwrap().run();
}

