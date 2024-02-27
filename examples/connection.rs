extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::deprecated::{unix, EventLoop, EventLoopBuilder};
use my_mio::net::{TcpListener, TcpStream};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};

use std::io::{Read, Write};
trait MapNonBlock<T> {
    fn map_non_block(self) -> io::Result<Option<T>>;
}
impl<T> MapNonBlock<T> for io::Result<T> {
    fn map_non_block(self) -> io::Result<Option<T>> {
        use std::io::ErrorKind::WouldBlock;
        match self {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if let WouldBlock = err.kind() {
                    Ok(None) 
                } else {
                    Err(err)
                }
            }
        }
    }
}
pub trait TryRead {
    fn try_read_buf<B: BufMut>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
            where Self : Sized {
        let bytes = buf.chunk_mut();
        let res = self.try_read(unsafe { 
            std::slice::from_raw_parts_mut(bytes.as_mut_ptr(), bytes.len())
        });

        if let Ok(Some(cnt)) = res {
            unsafe { buf.advance_mut(cnt); }
        }
        res 
    }
    fn try_read(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>>;
}

pub trait TryWrite {
    fn try_write_buf<B: Buf>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
        where Self : Sized {
        let res = self.try_write(buf.chunk());
        if let Ok(Some(cnt)) = res {
            buf.advance(cnt);
        }
        res
    }
    fn try_write(&mut self, buf: &[u8]) -> io::Result<Option<usize>>;
}

impl<T: Read> TryRead for T {
    fn try_read(&mut self, dst: &mut [u8]) -> io::Result<Option<usize>> {
        self.read(dst).map_non_block()
    }
}

impl<T: Write> TryWrite for T {
    fn try_write(&mut self, src: &[u8]) -> io::Result<Option<usize>> {
        self.write(src).map_non_block()
    }
}

const SERVER: Token = Token(10_000_000);
const CLIENT: Token = Token(10_000_001);

unsafe impl Send for Acceptor {} // `std::sync::mpsc::Receiver<i32>` cannot be shared between threads safely
unsafe impl Sync for Acceptor {}
struct Acceptor {
    tcp_listener: TcpListener,
    event_loop: Arc<EventLoop>,
    is_listening: bool,
    accept_cb: fn(TcpStream, SocketAddr)
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {}

impl Acceptor {
    fn new(event_loop: Arc<EventLoop>, addr: &String) -> Acceptor {
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
    pub fn handleRead(&self, val: i64) {
        println!("---hello world-------------- {}", val);
        let (stream, addr) = self.tcp_listener.accept().unwrap();
        //(self.accept_cb)(stream, addr);
        println!("---hello world-------------- {}", val);
        //let mut connection = TcpConnection::new(self.event_loop.clone(), stream);
        //self.event_loop.run(&mut connection);
        // 怎样注册事件? // 模拟server1.rs ?

        let mut conn = Arc::new(TcpConnection::new(self.event_loop.clone(), stream));
        let clone = conn.clone();
        let job = Box::new(move |val: i64| { clone.handleRead(val); });
        self.event_loop.register(&conn.sock, SERVER, Ready::readable(), 
                PollOpt::edge(), job);
    }
    fn bind(&self, job: Job) {
        self.event_loop.register(&self.tcp_listener, SERVER, Ready::readable(), 
                PollOpt::edge(), job);
        //self.is_listening = true;
    }
}

struct Connector {
    addr: String,
    connector: TcpStream,
    event_loop: Arc<EventLoop>,
    is_connected: bool,
    connect_cb: fn(&TcpStream)
}

impl Connector {
    fn attach(&mut self, event_loop: Arc<EventLoop>) {
        self.event_loop = event_loop;
        self.is_connected = false;
    }
    fn set_connect_cb(&mut self, cb: fn(&TcpStream)) {
        self.connect_cb = cb;
    }
    fn connect(&mut self, addr: &String) {
        let sock = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        self.connector = sock;
        let job = Box::new(move |val: i64| { println!("--------------"); });
        self.event_loop.register(&self.connector, CLIENT, Ready::writable(),
                PollOpt::edge() | PollOpt::oneshot(), job).unwrap();
    }
}

unsafe impl Send for TcpConnection {}
unsafe impl Sync for TcpConnection {}
pub struct TcpConnection {
    token: Option<Token>,
    interest: Ready,

    event_loop: Arc<EventLoop>,
    sock: TcpStream,
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
    fn new(event_loop: Arc<EventLoop>, sock: TcpStream) -> TcpConnection {
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

    /*
	fn getReadBuffers(&mut bytes: BytesMut) -> &mut IoVec {
	    let bytes = bytes.chunk_mut(); 
	    let mut b = std::slice::from_raw_parts_mut(bytes.as_mut_ptr(), bytes.len());
        //let mut iov: [&mut IoVec; 1] = [
        //    b.into(),    
        //];
        return &mut b.into();
    }
    */

    fn handleRead(&mut self, event: i64) -> io::Result<()> {
        let mut buf = self.read_buf.take().unwrap();
        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                println!("Conn: spurious read wakeup");
                //self.read_buf = Some(buf);
            }
            Ok(Some(r)) => {
                println!("Conn: read {} bytes, {:?}", r, buf);
                //self.read_buf = Some(buf);
                (self.read_cb)(&mut buf);
                //self.interest.remove(Ready::readable());
                //self.interest.insert(Ready::writable());
            }
            Err(e) => {
                println!("not implemented client err: {:?}", e);
                //self.interest.remove(Ready::readable());
            }
        };
        Ok(())
    }
    fn handleWrite(&mut self, event: i64) -> io::Result<()> {
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
        //assert!(self.interest.is_readable() || self.interest.is_writable(),
        //        "actual: {:?}", self.interest);
        //poll.reregister(&self.sock, self.token.unwrap(), self.interest, 
        //        PollOpt::edge() | PollOpt::oneshot())
        Ok(())
    }
    fn handleClose(&mut self, pool: &mut Poll, sock: TcpStream) -> io::Result<()> {
        Ok(())
    }
    fn handleError(&mut self, pool: &mut Poll, sock: TcpStream) -> io::Result<()> {
        Ok(())
    }
    fn writeData(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
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

fn sleep_ms(ms: u64) {
    use std::thread;
    thread::sleep(Duration::from_millis(ms));
}

// TcpServer::new_connection->
fn accept_cb(stream: TcpStream, addr: SocketAddr) {
    println!("stream: {:?}, addr: {:?}", stream, addr);
    //let mut connection = TcpConnection::new(, stream);
}

fn main() {
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let mut event_loop = Arc::new(b.build().unwrap());
    let mut acceptor = Arc::new(Acceptor::new(event_loop.clone(), &"0.0.0.0:9527".to_string()));

    let clone = acceptor.clone();
    let job = Box::new(move |val: i64| { clone.handleRead(val); });
    acceptor.bind(job);
    //acceptor.set_accept_cb(accept_cb);
    event_loop.run();
}

