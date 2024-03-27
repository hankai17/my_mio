extern crate my_mio;
extern crate bytes;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job, Registration, SetReadiness};
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

impl Handler for Test {
    fn new() -> Test {
        Test {
            id: 32,
            conn: None
        }
    }
    fn setConnection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.conn = Some(conn);
        println!("Test setConnection");
    }
    fn onRecv(&mut self, bytes: &mut BytesMut) {
        if bytes.len() <= 0 {
            println!("bytes len 0");
            let (r, set) = Registration::new2();
            let mut r = Arc::new(r);
            let mut r_clone = r.clone();
            let mut set = Arc::new(set);
            let mut set_clone = set.clone();

            let mut conn = self.conn.take().unwrap();
            let mut conn_clone = conn.clone();
            self.conn = Some(conn);

            let event_loop = EventLoopBuilder::get_current_loop();
            let job = Box::new(move |val: i64| { 
                r.clone(); 
                set.clone(); 
                println!("1--------------"); 
                let mut conn = conn_clone.lock().unwrap();
                let rsp = BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..]);
                conn.send(rsp);
            });
            let token = event_loop.lock().unwrap().set_job(job);

            set_clone.set_readiness(Ready::readable()).unwrap();
            let job = Box::new(move |val: i64| { println!("2--------------"); });
            event_loop.lock().unwrap().register(&r_clone, token, Ready::readable(), PollOpt::edge(), job).unwrap();
            return;
        }
        println!("bytes len: {}, {:?}", bytes.len(), bytes);
        bytes.advance(bytes.len());

        // 死锁了 解决方案用可重入锁 但是改的地方稍微有点儿多
        /*
        let rsp = BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..]);
        let mut conn = self.conn.take().unwrap();
        conn.lock().unwrap().send(rsp);
        self.conn = Some(conn);
        */
        let (r, set) = Registration::new2();
        let mut r = Arc::new(r);
        let mut r_clone = r.clone();
        let mut set = Arc::new(set);
        let mut set_clone = set.clone();

        let mut conn = self.conn.take().unwrap();
        let mut conn_clone = conn.clone();
        self.conn = Some(conn);

        let event_loop = EventLoopBuilder::get_current_loop();
        let job = Box::new(move |val: i64| { 
            r.clone(); 
            set.clone(); 
            println!("1--------------"); 
            let mut conn = conn_clone.lock().unwrap();
            let rsp = BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..]);
            conn.send(rsp);
        });
        let token = event_loop.lock().unwrap().set_job(job);

        set_clone.set_readiness(Ready::readable()).unwrap();
        let job = Box::new(move |val: i64| { println!("2--------------"); });
        event_loop.lock().unwrap().register(&r_clone, token, Ready::readable(), PollOpt::edge(), job).unwrap();
    }
    fn onWritten(&mut self) -> bool {
        println!("Test onWritten");
        /*
        // 这里是发送完的回调 而非可发送回调 // 可发送回调是es直接触发而调用的 如果es中的数据发不完就不会调用这个函数
        // 那么得找一个地方可以发数据 且不能死锁
        let rsp = BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..]);
        let mut conn = self.conn.take().unwrap();
        conn.lock().unwrap().send(rsp);
        self.conn = Some(conn);
        */

        /*
        // 又死锁了 因为这个回调可能在conn.send函数里
        let mut conn = self.conn.take().unwrap();
        conn.lock().unwrap().close_stream();
        self.conn = Some(conn);
        true
        */
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
    TcpServer::start_internal1(tcp_server);

    let ptr: *mut Mutex<EventLoop> = Arc::as_ptr(&mut event_loop) as *mut _;
    unsafe {
        (*ptr).get_mut().unwrap().run();
    }
}

