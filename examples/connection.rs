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

pub struct TcpConnection {
    token: Option<Token>,
    interest: Ready,

    event_loop: Arc<EventLoop>,
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

	fn getReadBuffers(&mut bytes: BytesMut) -> &mut IoVec {
	    let bytes = bytes.chunk_mut(); 
	    let mut b = std::slice::from_raw_parts_mut(bytes.as_mut_ptr(), bytes.len());
        //let mut iov: [&mut IoVec; 1] = [
        //    b.into(),    
        //];
        return &mut b.into();
    }

    fn handleRead(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        // read to buffer
        //read_cb(bytes, sockaddr)
    	//pub fn read_bufs(&self, bufs: &mut [&mut IoVec]) -> io::Result<usize> {

        let iov = getReadBuffers(self.read_buf);
        sock.read_buf(iov);

        while (m_read_enable) {
            do {
                std::vector<iovec> iovs = m_read_buffer->writeBuffers(32 * 1024);
                nread = recvFrom(fd, &iovs[0], iovs.size(), &addr, len);
            } while (-1 == nread && UV_EINTR == get_uv_error(true));
            if (nread <= 0) {
                setReadTriggered(false);
                if (nread < 0) {
                    auto err = get_uv_error(true);
                    if (err != UV_EAGAIN) {
                        if (!is_udp) {
                            emitErr(toSocketException(err));
                        } else {
                            HAMMER_LOG_WARN(g_logger) << "Recv err on udp socket: " << fd << uv_strerror(err);
                        }
                    }
                    return ret;
                }
                if (nread == 0) {
                    if (!is_udp) {
                        emitErr(SocketException(ERRCode::EEOF, "end of file..."));
                    } else {
                        HAMMER_LOG_WARN(g_logger) << "Recv eof on udp socket: " << fd;
                    }
                    return ret;
                }
            }

            ret += nread;
            m_read_buffer->product(nread);
            LOCK_GUARD(m_event_cb_mutex);
            try {
                m_on_read_cb(m_read_buffer, (struct sockaddr*)&addr, len);
                // assert upper consume over TODO
            } catch (std::exception &e) {
                HAMMER_LOG_WARN(g_logger) << "Exception occurred when emit on_read_cb: " << e.what();
            }
        }
        Ok(())
    }
    fn handleWrite(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
        //written_cb()
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

