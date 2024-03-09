use std::{io, mem, fmt};
use net::{TryRead, TryWrite};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use {Events, Poll, PollOpt, Ready, Token, Job};
use event_imp::{ready_from_usize, ready_as_usize};
use net::{EventLoop, TcpStream};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};

unsafe impl Send for Session {}
unsafe impl Sync for Session {}

pub struct Session { // trait TODO 1
    token: Option<Token>,
    connection: Arc<Mutex<TcpConnection>>,
};

impl Session {
    pub fn new(connection: Arc<Mutex<TcpConnection>>) -> Session {
        Session {
            token: None,
            connection: connection 
        }
    }
    fn onRecv(&mut self, &mut bytes: BytesMut) -> io::Result<()> {
        Ok(())
    }
    fn onWritten(&mut self) -> bool {
        false
    }
    fn onError(&mut self) {
    }
    fn send(&mut self, &mut bytes: BytesMut) -> io::Result<()> {
        Ok(())
    }
    fn shutdown() {
    }
    fn safeShutdown() {
    }
}

pub struct SessionManager {
    // map<string, weak<Session>>
}

impl SessionManager {
    fn add(&s: String, session: Arc<Mutex<Session>>) -> bool {
        false
    }
    fn del(&s: String) {
    }
}

pub struct TcpServer {
    event_loop: Arc<Mutex<EventLoop>>, 
    acceptor: Arc<Mutex<Acceptor>>,
    // timer
    //sessionAlloc
    // on_read_cb
    // on_written_cb
    // on_err_cb
}

impl TcpServer {
    fn start_internal(port, addr backlog) {
    }
    pub fn start<Session>(&mut self, port, addr backlog) {
        session_alloc = Session::new()
        start_internal()
    }
    fn ...
}

