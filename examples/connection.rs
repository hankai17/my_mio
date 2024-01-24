extern crate my_mio;
extern crate bytes;

use std::{io, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::deprecated::{unix, EventLoop, Handler, EventLoopBuilder};
use my_mio::net::{TcpListener, TcpStream};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

const SERVER: Token = Token(10_000_000);
const CLIENT: Token = Token(10_000_001);

struct Acceptor {
    tcp_listener: TcpListener,
    event_loop: Box<EventLoop>,
    is_listening: bool,
    accept_cb: fn(TcpStream, SocketAddr)
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {}

impl Acceptor {
    fn new(event_loop: Box<EventLoop>, addr: &String) -> Acceptor {
        Acceptor {
            tcp_listener: TcpListener::new(&(addr.parse().unwrap())),
            event_loop: event_loop,
            is_listening: false,
            accept_cb: default_accept_cb,
        }
    }
    fn set_accept_cb(&mut self, cb: fn(TcpStream, SocketAddr)) {
        self.accept_cb = cb;
    }
    fn bind(&mut self) {
        self.event_loop.register(&self.tcp_listener, SERVER, Ready::readable(), 
                PollOpt::edge() | PollOpt::oneshot()).unwrap();
        self.is_listening = true;
    }
}

impl Handler for Acceptor {
    fn ready(&mut self, event_loop: &mut EventLoop, token: Token, 
            events: Ready) {
        let (stream, addr) = self.tcp_listener.accept().unwrap();
        (self.accept_cb)(stream, addr);
    }
    fn notify(&mut self, event_loop: &mut EventLoop, msg: i32) {
    }
}

struct Connector {
    addr: String,
    connector: TcpStream,
    event_loop: Box<EventLoop>,
    is_connected: bool,
    connect_cb: fn(&TcpStream)
}

impl Connector {
    fn attach(&mut self, event_loop: Box<EventLoop>) {
        self.event_loop = event_loop;
        self.is_connected = false;
    }
    fn set_connect_cb(&mut self, cb: fn(&TcpStream)) {
        self.connect_cb = cb;
    }
    fn connect(&mut self, addr: &String) {
        let sock = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        self.connector = sock;
        self.event_loop.register(&self.connector, CLIENT, Ready::writable(),
                PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }
}

impl Handler for Connector {
    fn ready(&mut self, event_loop: &mut EventLoop, token: Token, 
            events: Ready) {
        (self.connect_cb)(&self.connector);
    }
    fn notify(&mut self, event_loop: &mut EventLoop, msg: i32) {
    }
    fn timeout(&mut self, event_loop: &mut EventLoop, timeout: i32) {
    }
    fn interrupted(&mut self, event_loop: &mut EventLoop) {
    }
    fn tick(&mut self, event_loop: &mut EventLoop) {
    }
}

pub struct TcpConnection {
    token: Option<Token>,
    interest: Ready,

    event_loop: Box<EventLoop>,
    sock: TcpStream,
    // timer
    read_buf: Option<BytesMut>,
    write_buf: Option<BytesMut>,
    write_buf_waiting: Option<BytesMut>,

    read_cb: fn(BytesMut, SocketAddr),
    written_cb: fn() -> bool,
    err_cb: fn(),

    read_enable: bool,
    write_enable: bool,
    read_triggered: bool,
    write_triggered: bool,
    is_closed: bool
}

fn default_read_cb(bytes: BytesMut, addr: SocketAddr) {}
fn default_written_cb() -> bool { false }
fn default_err_cb() {}

impl TcpConnection {
    fn new(event_loop: Box<EventLoop>, sock: TcpStream) -> TcpConnection {
        TcpConnection {
            token: None,
            interest: Ready::empty(),
            event_loop: event_loop,
            sock: sock,

            read_buf: Some(BytesMut::with_capacity(1024)),
            write_buf: Some(BytesMut::with_capacity(1024)),
            write_buf_waiting: Some(BytesMut::with_capacity(1024)),

            read_cb: default_read_cb,
            written_cb: default_written_cb,
            err_cb: default_err_cb,

            read_enable: false,
            write_enable: false,
            read_triggered: false,
            write_triggered: true,
            is_closed: false
        }
    }

    fn on_read(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        // read to buffer
        //read_cb(bytes, sockaddr)
        Ok(())
    }
    fn on_written(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        //written_cb()
        Ok(())
    }
    fn write_data(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        Ok(())
    }
    fn on_write(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        Ok(())
    }
    fn send_l(bytes: BytesMut) -> io::Result<usize> {
        Ok(0)
    }
    //fn send() 
    fn set_on_read_cb() { // set by upper eg: session
    }
    fn set_on_written_cb() {
    }
    fn set_on_error_cb() {
    }
    fn clone_stream() {
    }
}

impl Handler for TcpConnection {
    fn ready(&mut self, event_loop: &mut EventLoop, token: Token, 
            events: Ready) {
        /*
        if read 
            on_read
        if write 
            on_write
        if error 
            on_err
        */
    }
    fn notify(&mut self, event_loop: &mut EventLoop, msg: i32) {
    }
    fn timeout(&mut self, event_loop: &mut EventLoop, timeout: i32) {
    }
    fn interrupted(&mut self, event_loop: &mut EventLoop) {
    }
    fn tick(&mut self, event_loop: &mut EventLoop) {
    }
}

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
    let mut event_loop = Box::new(b.build().unwrap());
    let acceptor = Acceptor::new(event_loop, &"0.0.0.1:9527".to_string());
    sleep_ms(1000 * 100);
}

