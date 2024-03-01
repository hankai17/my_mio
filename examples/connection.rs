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

fn main() {
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let event_loop = b.build().unwrap();

    let mut arc_event_loop = Arc::new(event_loop);
    let mut arc_mutex_event_loop = Arc::new(Mutex::new(event_loop));
    let mut acceptor = Arc::new (
        //Acceptor::new(event_loop, &"0.0.0.0:9527".to_string()) // found struct `Arc<Mutex<EventLoop>>`
        //Acceptor::new(event_loop.lock().unwrap(), &"0.0.0.0:9527".to_string()) // `MutexGuard<'_, EventLoop>`
        //Acceptor::new(event_loop.lock().unwrap(), &"0.0.0.0:9527".to_string()) 
        Acceptor::new(arc_event_loop, &"0.0.0.0:9527".to_string()) 
    );

    let clone = acceptor.clone();
    let job = Box::new(move |val: i64| { clone.handleRead(val); });
    acceptor.bind(job);
    arc_mutex_event_loop.lock().unwrap().run();
}

