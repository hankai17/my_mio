use net::{EventLoop, TcpStream, TcpListener};
use std::net::{SocketAddr};
use std::sync::{Arc, Mutex};
use {PollOpt, Ready, Token, Job, TokenType, TokenEntry};

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

fn default_accept_cb(stream: TcpStream, _addr: SocketAddr) {}

impl Acceptor {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, addr: &String) -> Acceptor {
        Acceptor {
            tcp_listener: TcpListener::new(&(addr.parse().unwrap())),
            event_loop: event_loop,
            is_listening: false,
            accept_cb: default_accept_cb,
            acceptor_job: Box::new(move |_, _| { println!("default acceptor job"); })
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

    /*
    pub fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let (s, a) = try!(self.accept_std());
        Ok((TcpStream::from_stream(s)?, a))
    }
    */
    pub fn handleRead(&mut self, _val: i64) {
        use std::io::ErrorKind::WouldBlock;
        use std::io::ErrorKind::Interrupted;
        loop {
            match self.tcp_listener.accept() {
                Ok((stream, addr)) => {
                    (self.acceptor_job)(stream, addr);
                }
                Err(ref e) if e.kind() == Interrupted => {
                    continue;
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    break;
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
    pub fn bind(&mut self, job: Job) {
        self.event_loop.lock().unwrap().register(
                &self.tcp_listener, 
                TokenEntry {
                    ttype: TokenType::AcceptEvent,
                    token: Token(0)
                }, 
                Ready::readable(), 
                PollOpt::edge(),
                job
        );
        self.is_listening = true;
    }
}

