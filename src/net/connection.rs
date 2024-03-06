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

// token incr TODO

unsafe impl Send for TcpConnection {}
unsafe impl Sync for TcpConnection {}

macro_rules! pub_struct {
    ($name:ident {$($field:ident: $t:ty,)*}) => {
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            $(pub $field: $t),*
        }
    }
}

pub struct TcpConnection {
    token: Option<Token>,
    interest: Ready,

    event_loop: Arc<Mutex<EventLoop>>,
    pub sock: TcpStream,
    // timer
    read_buf: Option<BytesMut>,
    write_buf: Option<BytesMut>,
    write_buf_waiting: Option<BytesMut>,

    read_cb: fn(&mut BytesMut),
    written_cb: fn() -> bool,
    err_cb: fn(),

    read_enable: bool,
    write_enable: bool,
    read_triggered: bool,
    write_triggered: bool,
    is_closed: bool
}

fn default_read_cb(bytes: &mut BytesMut) {}
fn default_written_cb() -> bool { false }
fn default_err_cb() {}

impl TcpConnection {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, sock: TcpStream) -> TcpConnection {
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

    fn handleRead(&mut self) -> io::Result<()> {
        let mut buf = self.read_buf.take().unwrap();
        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                println!("Conn: spurious read wakeup");
                self.read_buf = Some(buf);
            }
            Ok(Some(r)) => {
                println!("Conn: read {} bytes, {:?}", r, buf);
                if r == 0 {
                    self.event_loop.lock().unwrap().deregister(&self.sock);
                }
                // buf toto
                (self.read_cb)(&mut buf);
                self.read_buf = Some(buf);
                //self.interest.remove(Ready::readable());
                //self.interest.insert(Ready::writable());
            }
            Err(e) => {
                println!("not implemented client err: {:?}", e);
                // deregister
            }
        };
        Ok(())
    }

    fn writeData(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        // re-construct
        Ok(())
    }
    fn send(bytes: BytesMut) -> io::Result<usize> {
        // re-construct
        Ok(0)
    }
    fn handleWrite(&mut self) -> io::Result<()> {
        // re-construct
        let mut buf = self.write_buf.take().unwrap();
        match self.sock.try_write_buf(&mut buf) {
            Ok(None) => {
                println!("client flushing buf; WouldBlock");
                self.write_buf = Some(buf);
                //self.interest.insert(Ready::writable());
            }
            Ok(Some(r)) => {
                println!("Conn: write {} bytes", r);
                //self.write_buf = Some(buf);
                (self.written_cb)();
                //self.interest.insert(Ready::readable());
                //self.interest.remove(Ready::writable());
            }
            Err(e) => {
                println!("not implemented; client err: {:?}", e);
            }
        }
        Ok(())
    }

    fn handleError(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub fn handleEvent(&mut self, event: i64) -> io::Result<()> {
        // check closed
        let ready = ready_from_usize(event as usize);
        println!("ready: {:?}", ready);
        if (ready.is_readable()) {
            self.handleRead();
        }
        if (ready.is_writable()) {
            self.handleWrite();
        }
        if (ready.is_error() || 
                ready.is_hup()) {
            self.handleError();
        }
        Ok(())
    }

    pub fn attachEvent(&mut self) {
        //self.event_loop.register(&self, SERVER, r|w|e, self.handleEvent) 
    }

    pub fn set_on_read_cb() { // set by upper eg: session
    }
    pub fn set_on_written_cb() {
    }
    pub fn set_on_error_cb() {
    }
    pub fn clone_stream() {
    }
}

impl Drop for TcpConnection {
    fn drop(&mut self) {
        //self.event_loop.lock().unwrap().deregister(&self.sock);
        println!("---------------------drop for tcpconnection")
    }
}

