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

pub trait Handler: Sized {
    type Connection;
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
};

pub struct SessionManager {
    // map<string, weak<Session>>
}

impl SessionManager {
    /*
    fn add(&s: String, session: Arc<Mutex<Session>>) -> bool {
        false
    }
    fn del(&s: String) {
    }
    */
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

impl <H: Handler>TcpServer {
    /*
    fn start_internal(port, addr backlog) {
    }
    */
    pub fn start(&mut self, handler: &mut H) {
        //session_alloc = []() -> Arc<Mutex<Handler>> { Session::new() }
        //start_internal()
    }
    pub onAcceptConnection(&mut self, &mut connection: TcpConnection) {
        // let mut session = self.sessino_alloc();
        // connection.set_read_cb(session.onRecv);
    }
}

