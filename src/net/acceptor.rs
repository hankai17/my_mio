use net::{EventLoop, TcpStream, TcpListener};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use {Events, Poll, PollOpt, Ready, Token, Job};

const SERVER: Token = Token(10_000_000);
pub type AcceptorJob = Box<dyn FnMut(TcpStream, SocketAddr) + 'static + Send + Sync>;

unsafe impl Send for Acceptor {}
unsafe impl Sync for Acceptor {}

pub struct Acceptor {
    tcp_listener: TcpListener,
    event_loop: Arc<Mutex<EventLoop>>,
    is_listening: bool,
    accept_cb: fn(TcpStream, SocketAddr),
    acceptor_job: AcceptorJob
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {}

impl Acceptor {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, addr: &String) -> Acceptor {
        Acceptor {
            tcp_listener: TcpListener::new(&(addr.parse().unwrap())),
            event_loop: event_loop,
            is_listening: false,
            accept_cb: default_accept_cb,
            acceptor_job: Box::new(move |stream: TcpStream, addr: SocketAddr| { println!("default acceptor job"); })
        }
    }

    //pub fn set_accept_cb(&mut self, cb: impl Fn(TcpStream, SocketAddr)) {
    //pub fn set_accept_cb(&mut self, cb: FnMut(TcpStream, SocketAddr)) {
    //pub fn set_accept_cb(&mut self, cb: FnMut(TcpStream, SocketAddr)) {
    pub fn set_accept_cb(&mut self, cb: fn(TcpStream, SocketAddr)) {
        self.accept_cb = cb;
    }

    pub fn set_accept_job(&mut self, job: AcceptorJob) {
        self.acceptor_job = job;
    }

    pub fn handleRead(&mut self, val: i64) {
        // while 1 TODO
        // set sockopt TODO
        let (stream, addr) = self.tcp_listener.accept().unwrap();
        //(self.accept_cb)(stream, addr);
        (self.acceptor_job)(stream, addr);
    }
    pub fn bind(&mut self, job: Job) {
        self.event_loop.lock().unwrap().register(&self.tcp_listener, SERVER, Ready::readable(), 
                PollOpt::edge(), job);
        self.is_listening = true;
    }
}

