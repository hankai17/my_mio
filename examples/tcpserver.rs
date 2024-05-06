extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job, TimerJob, Registration, SetReadiness, TokenType, TokenEntry};
use my_mio::timer::{Timeout};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::net::{TcpListener, TcpStream, EventLoop, EventLoopBuilder, Acceptor, TcpConnection, TcpServer, Handler};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, Condvar, Weak};
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};
use std::time::Duration;

struct Test {
    id: i32,
    timer: Option<Timeout>, //pub type TimerJob = Box<dyn FnMut() + 'static + Send + Sync>;
    conn: Option<Weak<Mutex<TcpConnection>>>
}

impl Test {
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
            timer: None,
            conn: None,
        }
    }

    fn attachConnection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.conn = Some(Arc::downgrade(&conn));
    }

    fn freeConnection(&mut self) {
        self.conn = None;
    }

    fn onAccept(&mut self) {
        let mut conn = self.conn.take().unwrap();
        let mut conn_clone = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.conn = Some(conn);
        let event_loop = EventLoopBuilder::get_current_loop();

        let mut timer = event_loop.lock().unwrap().timeout(
            Duration::from_millis(1000 * 3),
            Box::new(
                move || {
                    //println!("timeout test...");
                    conn_clone.lock().unwrap().close_stream();
                }
            )
        );
        match timer {
            Ok(timer) => self.timer = Some(timer),
            _ => return,
        }
    }

    fn onRecv(&mut self, bytes: &mut BytesMut) {
        if bytes.len() <= 0 {
            return;
        }
        match &self.timer {
            Some(timer) => {
                let mut event_loop = EventLoopBuilder::get_current_loop();
                event_loop.lock().unwrap().clear_timeout(&timer);
                //println!("timeout cancel")
            },
            _ => {},
        }
        //println!("bytes len: {}, {:?}", bytes.len(), bytes);
        bytes.advance(bytes.len());

        let (r, s) = Registration::new2();
        let mut r = Arc::new(r);
        let mut s = Arc::new(s);

        let mut r_clone = r.clone();
        let mut s_clone = s.clone();

        let mut conn = self.conn.take().unwrap();
        let mut conn_clone = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.conn = Some(conn);

        let event_loop = EventLoopBuilder::get_current_loop();
        let job = Arc::new(Mutex::new(move |val: i64| {
            r_clone.clone(); 
            s_clone.clone();
            let mut conn = conn_clone.lock().unwrap();
            conn.send(
                BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..])
            );
        }));

        let mut r_clone = r.clone();
        let mut s_clone = s.clone();
        s_clone.set_readiness(Ready::readable()).unwrap();

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
        self.freeConnection();
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

    let mut tcp_server = Arc::new(Mutex::new(TcpServer::new(event_loop.clone(), &"0.0.0.0:9528".to_string())));
    tcp_server.lock().unwrap().start::<Test>();
    TcpServer::start_internal(tcp_server);

    let ptr: *mut Mutex<EventLoop> = Arc::as_ptr(&mut event_loop) as *mut _;
    unsafe {
        (*ptr).get_mut().unwrap().run();
    }
}

