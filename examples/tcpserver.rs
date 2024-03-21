extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::net::{TcpListener, TcpStream, EventLoop, EventLoopBuilder, Acceptor, TcpConnection, TcpServer, Handler};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};
use std::time::Duration;

struct Test {
    id: i32
}
impl Handler for Test {
    fn new() -> Test {
        Test {
            id: 32
        }
    }
}

fn main() {
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let mut event_loop = b.get_build().unwrap();
    let mut tcp_server = Arc::new(Mutex::new(TcpServer::new(event_loop.clone(), &"0.0.0.0:9528".to_string())));
    tcp_server.lock().unwrap().start::<Test>();
    TcpServer::start_internal1(tcp_server);

    let ptr: *mut Mutex<EventLoop> = Arc::as_ptr(&mut event_loop) as *mut _;
    unsafe {
        (*ptr).get_mut().unwrap().run();
    }
}

