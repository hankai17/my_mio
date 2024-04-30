extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job, Registration, SetReadiness, TokenType, TokenEntry};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::net::{TcpListener, TcpStream, EventLoop, EventLoopBuilder, Acceptor, TcpConnection, TcpServer, Handler};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};
use std::time::Duration;

struct Test {
    id: i32,
    conn: Option<Arc<Mutex<TcpConnection>>>
}

impl Drop for Test {
    fn drop(&mut self) {
        //println!("---------------------dropping for Test")
    }
}

impl Handler for Test {
    fn new() -> Test {
        Test {
            id: 32,
            conn: None,
        }
    }
    //pub type TimerJob = Box<dyn FnMut() + 'static + Send + Sync>;
    fn setConnection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.conn = Some(conn);
         
    }
    /*
    fn unsetConnection(&mut self) {
        self.conn = None;
    }
    */
    fn onRecv(&mut self, bytes: &mut BytesMut) {
        if bytes.len() <= 0 {
            return;
        }
        println!("bytes len: {}, {:?}", bytes.len(), bytes);
        bytes.advance(bytes.len());

        let (r, set) = Registration::new2();
        let mut r = Arc::new(r);
        let mut r_clone = r.clone();
        let mut set = Arc::new(set);
        let mut set_clone = set.clone();

        let mut conn = self.conn.take().unwrap();
        let mut conn_clone = conn.clone();
        self.conn = Some(conn);

        let event_loop = EventLoopBuilder::get_current_loop();
        let job = Arc::new(Mutex::new(move |val: i64| {
            r.clone(); 
            set.clone(); 
            let mut conn = conn_clone.lock().unwrap();
            let rsp = BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..]);
            conn.send(rsp);
            //println!("sending: conn use_count: {}", Arc::strong_count(&conn_clone));
        }));
        set_clone.set_readiness(Ready::readable()).unwrap();
        event_loop.lock().unwrap().register(
                &r_clone,
                TokenEntry {
                    ttype: TokenType::OTHER_EVENT,
                    token: Token(0)
                },
                Ready::readable(),
                PollOpt::edge(),
                job
        ).unwrap();
    }
    fn onWritten(&mut self) -> bool {
        let mut conn = self.conn.take().unwrap();
        //println!("onWritten: conn use_count: {}", Arc::strong_count(&conn));
        false
    }
    fn onError(&mut self) {
        println!("Test onError");
    }
}

fn main() {

    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let mut event_loop = b.get_build().unwrap();

    //pub type TimerJob = Box<dyn FnMut() + 'static + Send + Sync>;
    event_loop.lock().unwrap().timeout(Duration::from_millis(1000 * 3), 
        Box::new(move|| {println!("timer test...")})
    );

    let mut tcp_server = Arc::new(Mutex::new(TcpServer::new(event_loop.clone(), &"0.0.0.0:9528".to_string())));
    tcp_server.lock().unwrap().start::<Test>();
    TcpServer::start_internal1(tcp_server);

    let ptr: *mut Mutex<EventLoop> = Arc::as_ptr(&mut event_loop) as *mut _;
    unsafe {
        (*ptr).get_mut().unwrap().run();
    }
}

