extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;
extern crate chrono;

use my_mio::{PollOpt, Ready, Token, Registration, TokenType, TokenEntry};
use my_mio::timer::{Timeout};
use my_mio::net::Connector;
use bytes::{Buf, BytesMut};
use my_mio::net::{EventLoop, EventLoopBuilder,  EventLoopPool, TcpConnection, TcpClient, ClientHandler, TcpStream};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::thread;
use log::{debug, info};
use std::io::Write;
use std::fs::File;
use chrono::Local;

use log::LevelFilter;
use env_logger::{Builder, WriteStyle};

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
        debug!("dropping for TestClient")
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
        info!("on_recv: {:?}", bytes);
        bytes.advance(bytes.len());
    }

    fn on_written(&mut self) -> bool {
        debug!("write done");
        true
    }

    fn on_error(&mut self) {
        info!("connect error");
    }
}


fn main() {
    let target = Box::new(File::create("/tmp/test.txt").expect("Can't create file"));
	Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} {:?} {}:{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                thread::current().id(),
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.level(),
                record.args()
            )
        })
        .target(env_logger::Target::Pipe(target))
        .filter(None, LevelFilter::Debug)
        .init();

    debug!("Starting main");

    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let event_loop = b.get_build1().unwrap();

    for i in 0..2 {
        let clone = event_loop.clone();
        thread::spawn(move || {
            EventLoopBuilder::set_current_loop(clone.clone());
            clone.run();
            info!("clone run done");
        });
        info!("spawn thread done");
    }

    for  i in 0..80 {
        let poller_clone = event_loop.clone();
        let job = Arc::new(Mutex::new(move || {
            let mut cli = TcpClient::new(poller_clone.clone());
            cli.start_connect(
                //&"114.0.0.1:90".to_string(),
                &"127.0.0.1:90".to_string(),
                Arc::new(Mutex::new(TestClient::new()))
            );
        }));
        let poller_clone = event_loop.clone();
        enqueue_job(poller_clone.clone(), job);
    }

    sleep_ms(1000 * 1000);
}

