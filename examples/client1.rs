extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;

use my_mio::{PollOpt, Ready, Token, Registration, TokenType, TokenEntry};
use my_mio::timer::{Timeout};
use my_mio::net::Connector;
use bytes::{Buf, BytesMut};
use my_mio::net::{EventLoop, EventLoopBuilder,  EventLoopPool, TcpConnection, TcpClient, ClientHandler, TcpStream};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::thread;
use log::debug;


fn sleep_ms(ms: u64) {
    use std::thread;
    use std::time::Duration;
    thread::sleep(Duration::from_millis(ms));
}

type Job = Arc<Mutex<dyn FnMut() + 'static + Send + Sync>>;

fn enqueue_job(poller: Arc<EventLoop>, cb: Job) {
    let (r, s) = Registration::new2();
    let r = Arc::new(r);
    let s = Arc::new(s);

    let r_clone = r.clone();
    let s_clone = s.clone();

    let job = Arc::new(Mutex::new(move |_| {
        let _ = r_clone.clone(); 
        let _ = s_clone.clone();
        cb.lock().unwrap()();
    }));
    s.set_readiness(Ready::readable()).unwrap();
    poller.register(
            &r,
            TokenEntry {
                ttype: TokenType::OtherEvent,
                token: Token(0)
            },
            Ready::readable(),
            PollOpt::edge(),
            job
    ).unwrap();
}

struct TestClient {
    timer: Option<Timeout>,
}

impl TestClient {
    fn new() -> TestClient {
        TestClient {
            timer: None,
        }
    } 
}

impl Drop for TestClient {
    fn drop(&mut self) {
        println!("-------------------------dropping for TestClient")
    }
}

impl ClientHandler for TestClient {
    fn shutdown(&mut self) {
    }

    /*
    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.conn = Some(Arc::downgrade(&conn));
    }
    */

    fn on_connect(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        let poller = EventLoopBuilder::get_current_loop();
        let job = Arc::new(Mutex::new(move|| {
            let mut conn = conn.lock().unwrap();
            conn.send(
                BytesMut::from(&b"GET /klsdjf HTTP/1.1\r\nHost: 0.0.0.0:90\r\nUser-Agent: curl/7.61.1\r\nAccept: */*\r\n"[..])
            ).unwrap(); // 由于已经调用过conn->set_writ_job 且writ_job也是ClientHandler 所以死锁
        }));
        enqueue_job(poller.clone(), job);
    }

    fn on_recv(&mut self, bytes: &mut BytesMut) {
        println!("on_recv: {:?}", bytes);
        bytes.advance(bytes.len());
    }

    fn on_written(&mut self) -> bool {
        println!("write done");
        true
    }

    fn on_error(&mut self) {
        println!("connect error");
    }
}


fn main() {
    let _ = ::env_logger::init();
    debug!("Starting main");
    let pool = EventLoopPool::new(1);
    for poller in pool.get_all_poller().iter() {
        for  i in 0..40 {
            let mut cli = TcpClient::new(poller.clone());
            cli.start_connect(
                &"127.0.0.1:90".to_string(),
                Arc::new(Mutex::new(TestClient::new()))
            );
        }
    }
    sleep_ms(1000 * 1000);
}

