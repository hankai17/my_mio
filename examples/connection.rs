extern crate bytes;

use std::fmt;
use my_mio::{Events, Poll, PollOpt, Ready, Token};
use bytes::{Buf, BufMut, Bytes, BytesMut};

struct Acceptor {
    acceptor: TcpListener,
    event_loop: EventLoop,
    is_listening: bool,
    accept_cb: fn(TcpStream, SocketAddr)
}

impl Acceptor {
    fn new(event_loop: &EventLoop) -> Acceptor {
        Acceptor {
            acceptor: TcpListener::new(),
            event_loop: event_loop,
            is_listening: true
        }
    }
    fn set_accept_cb(&mut self, cb: fn(TcpStream, SocketAddr)) {
        self.accpet_cb = cb;
    }
    fn bind(&mut self, ip: str) {
        let addr = ip.parse().unwrap();
        let srv = self.bind(&addr).unwrap();
        event_loop.register(&srv, SERVER, Ready::readable(), 
                PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }
}

impl Handler for Acceptor {
    type Timeout = usize;
    type Message = String;
    fn ready(&mut self, event_loop: &mut EventLoop<Acceptor>, token: Token, 
            events: Ready) {
        local (stream, addr) = self.accept().unwrap();
        accept_cb(stream, addr);
    }
    fn notify(&mut self, event_loop: &mut EventLoop<Acceptor>, msg: String) {
    }
}

struct Connector {
    addr: str,
    connector: TcpStream,
    event_loop: EventLoop,
    is_connected: bool,
    connect_cb: fn(TcpStream, io::Result<()>)
}

impl Connector {
    fn new(event_loop: &EventLoop, ip: str) -> Connector {
        Connector {
            addr: ip,
            //connector: TcpStream,
            event_loop: event_loop,
            is_connected: false,
        }
    }
    fn set_connect_cb(&mut self, cb: fn(TcpStream, io::Result<()>)) {
        connect_cb = cb;
    }
    fn connect(&mut self) {
        let addr = self.addr.parse().unwrap();
        let sock = TcpStream::connect(&addr).unwrap();
        self.connector = sock;
        event_loop.register(&sock, CLIENT, Ready::writable(),
                PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }
}

impl Handler for Connector {
    type Timeout = usize;
    type Message = String;
    fn ready(&mut self, event_loop: &mut EventLoop<Connector>, token: Token, 
            events: Ready) {
        connect_cb(self.connector, Ok(()))
    }
    fn notify(&mut self, event_loop: &mut EventLoop<Connector>, msg: String) {
    }
    fn timeout(&mut self, event_loop: &mut EventLoop<Connector>, timeout: Self::Timeout) {
    }
    fn interrupted(&mut self, event_loop: &mut EventLoop<Connector>) {
    }
    fn tick(&mut self, event_loop: &mut EventLoop<Connector>) {
    }
}

// accept_cb connect_cb return => TcpConnection

pub struct TcpConnection {
    token: Option<Token>,
    interest: Ready

    poller: Poll,
    sock: TcpStream,
    // timer
    read_buf: Option<BytesMut>,
    write_buf: Option<BytesMut>,
    write_buf_waiting: Option<BytesMut>,

    read_cb: fn(BytesMut, SockAddr) -> void;
    written_cb: fn() -> bool;
    err_cb: fn() -> void;

    read_enable: bool,
    write_enable: bool,
    read_triggered: bool,
    write_triggered: bool,
    is_closed: bool
}

impl TcpConnection {
    fn new(poll: Poll, sock: TcpStream) -> TcpConnection {
        TcpConnection {
            token: None,
            interest: Ready::empty(),
            poller: poll,
            sock: sock,

            read_buf: Some(BytesMut::with_capacity(1024)),
            write_buf: Some(BytesMut::with_capacity(1024)),
            write_buf_waiting: Some(BytesMut::with_capacity(1024)),

            read_enable: false,
            write_enable: false,
            read_triggered: false,
            write_triggered: true,
            is_closed: false
        }
    }

    fn on_read(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        // read to buffer
        read_cb(bytes, sockaddr)
    }
    fn on_written(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        written_cb()
    }
    fn write_data(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn on_write(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn send_l(bytes: ByteMut) -> io::Result<usize> {
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
    type Timeout = usize;
    type Message = String;
    fn ready(&mut self, event_loop: &mut EventLoop<TcpConnection>, token: Token, 
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
    fn notify(&mut self, event_loop: &mut EventLoop<TcpConnection>, msg: String) {
    }
    fn timeout(&mut self, event_loop: &mut EventLoop<TcpConnection>, timeout: Self::Timeout) {
    }
    fn interrupted(&mut self, event_loop: &mut EventLoop<TcpConnection>) {
    }
    fn tick(&mut self, event_loop: &mut EventLoop<TcpConnection>) {
    }
}

