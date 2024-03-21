use std::{io, mem, fmt};
use net::{TryRead, TryWrite};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use {Events, Poll, PollOpt, Ready, Token, Job};
use event_imp::{ready_from_usize, ready_as_usize};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};

pub trait Handler {
    //type Connection;
    fn new() -> Self where Self: Sized;
    //fn onRecv(&mut self, bytes: &mut BytesMut) -> io::Result<()>;
    //fn onWritten(&mut self) -> bool;
    //fn onError(&mut self);
    //fn send(&mut self, bytes: &mut BytesMut) -> io::Result<()>;
    ////fn shutdown();
    ////fn safeShutdown();
}

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
    session_alloc: Option<fn() -> Arc<Mutex<dyn Handler>>>,
    // on_read_cb
    // on_written_cb
    // on_err_cb
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {
    println!("move it into struct TODO");
}

impl TcpServer {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, addr: &String) -> TcpServer {
        let mut acceptor = Arc::new(Mutex::new(Acceptor::new(event_loop.clone(), addr)));
        TcpServer {
            event_loop: event_loop,
            acceptor: acceptor,
            session_alloc: None,
        }
    }
    pub fn onAcceptConnection(&mut self, stream: TcpStream, addr: SocketAddr) {
        println!("into struct");
        // let mut session = self.sessino_alloc();
        // connection.set_read_cb(session.onRecv);

        // put conn into session // TODO

        //let mut conn = Arc::new(Mutex::new(TcpConnection::new(event_loop.clone(), stream)));
        //let clone = conn.clone();
        //let job = Box::new(move |val: i64| { clone.lock().unwrap().handleEvent(val); });
        //conn.lock().unwrap().set_read_cb(default_read_cb);
        //conn.lock().unwrap().send(rsp);
        //event_loop.lock().unwrap().register(&conn.lock().unwrap().sock, CLIENT, Ready::readable(), 
        //        PollOpt::edge(), job);
    }
    fn start_internal(&mut self) {
        let mut clone = self.acceptor.clone();
        let job = Box::new(move |val: i64| { clone.lock().unwrap().handleRead(val); });
        self.acceptor.lock().unwrap().bind(job);
        self.acceptor.lock().unwrap().set_accept_cb(default_accept_cb);
    }
    pub fn start_internal1(this: Arc<Mutex<Self>>) {
        let mut clone = this.lock().unwrap().acceptor.clone();
        let job = Box::new(move |val: i64| { clone.lock().unwrap().handleRead(val); });
        this.lock().unwrap().acceptor.lock().unwrap().bind(job);

        let mut clone = this.clone();
        //let job = Box::new(move |val: &mut TcpConnection| { clone.lock().unwrap().onAcceptConnection(val); });
        let job = Box::new(move |stream: TcpStream, addr: SocketAddr| { clone.lock().unwrap().onAcceptConnection(stream, addr); });
        this.lock().unwrap().acceptor.lock().unwrap().set_accept_job(job);
    }
    pub fn start<H: Sized + 'static>(&mut self)
        where H: Handler {
        let mut session_alloc = || -> Arc<Mutex<dyn Handler>> {
            Arc::new(Mutex::new(<H as Handler>::new()))
        };
        self.session_alloc = Some(session_alloc);
        //self.start_internal();
    }
}

