extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;
extern crate chrono;
extern crate lazy_static;

use my_mio::{PollOpt, Ready, Token, Registration, TokenType, TokenEntry};
use my_mio::timer::{Timeout};
use bytes::{Buf, BytesMut};
use my_mio::net::{EventLoop, EventLoopBuilder,  EventLoopPool, TcpConnection, TcpServer, Handler};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::thread;

use std::io::Write;
use std::fs::File;
use chrono::Local;
use log::LevelFilter;
use env_logger::{Builder};
use log::{debug};

type Job = Arc<Mutex<dyn FnMut(i64) + 'static + Send + Sync>>;

fn enqueue_job(poller: Arc<EventLoop>, cb: Job) {
    let (r, s) = Registration::new2();
    let r = Arc::new(r);
    let s = Arc::new(s);

    let r_clone = r.clone();
    let s_clone = s.clone();

    let job = Arc::new(Mutex::new(move |_| {
        let _ = r_clone.clone(); 
        let _ = s_clone.clone();
        cb.lock().unwrap()(0);
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

struct Test {
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
            timer: None,
            conn: None,
        }
    }

    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.conn = Some(Arc::downgrade(&conn));
    }

    fn free_connection(&mut self) {
        self.conn = None;
    }

    fn on_accept(&mut self) {
        /*
        let conn = self.conn.take().unwrap();
        let conn_clone = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.conn = Some(conn);
        let event_loop = EventLoopBuilder::get_current_loop();
        */

        /*
        let timer = event_loop.timeout(
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
        */
    }

    fn on_recv(&mut self, bytes: &mut BytesMut) {
        if bytes.len() <= 0 {
            return;
        }
        /*
        match &self.timer {
            Some(timer) => {
                let event_loop = EventLoopBuilder::get_current_loop();
                event_loop.clear_timeout(&timer);
                //println!("timeout cancel")
            },
            _ => {},
        }
        */
        //println!("bytes len: {}, {:?}", bytes.len(), bytes);
        /*
        bytes.advance(bytes.len());

        let (r, s) = Registration::new2();
        let r = Arc::new(r);
        let s = Arc::new(s);

        let r_clone = r.clone();
        let s_clone = s.clone();

        let conn = self.conn.take().unwrap();
        let conn_clone = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.conn = Some(conn);

        let event_loop = EventLoopBuilder::get_current_loop();
        let job = Arc::new(Mutex::new(move |_| {
            let _ = r_clone.clone(); 
            let _ = s_clone.clone();
            let mut conn = conn_clone.lock().unwrap();
            conn.send(
                BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..])
            ).unwrap();
        }));

        let r_clone = r.clone();
        let s_clone = s.clone();
        s_clone.set_readiness(Ready::readable()).unwrap();

        event_loop.register(
                &r_clone,
                TokenEntry {
                    ttype: TokenType::OtherEvent,
                    token: Token(0)
                },
                Ready::readable(),
                PollOpt::edge(),
                job
        ).unwrap();
        */
        if bytes.len() <= 0 {
            return;
        }
        bytes.advance(bytes.len());
        let conn = match self.conn.as_mut() {
            Some(conn) => {
                match conn.upgrade() {
                    Some(conn) => conn.clone(),
                    None => return,
                }
            },
            None => return,
        };
        //let poller = EventLoopBuilder::get_current_loop();
        let poller = POOL.get_random_poller();
        let job = Arc::new(Mutex::new(move |_| {
            let mut conn = conn.lock().unwrap();
            conn.send(
                BytesMut::from(&b"HTTP/1.1 200 OK\r\nSet-Cookie:k1=v1\r\nContent-Length: 15\r\nConnection: Keep-Alive\r\n\r\nabcdefghijkldef"[..])
            ).unwrap();
        }));
        enqueue_job(poller.clone(), job);
    }

    fn on_written(&mut self) -> bool {
        self.free_connection();
        false
    }

    fn on_error(&mut self) {
        debug!("Test onError");
    }
}

fn sleep_ms(ms: u64) {
    use std::thread;
    use std::time::Duration;
    thread::sleep(Duration::from_millis(ms));
}

fn main1() {
    let _ = ::env_logger::init();
    debug!("Starting main");
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    
    let event_loop = b.get_build().unwrap();

    let tcp_server = Arc::new(Mutex::new(TcpServer::new(event_loop.clone(), &"0.0.0.0:9528".to_string())));
    tcp_server.lock().unwrap().start::<Test>();
    TcpServer::start_internal(tcp_server);

    for i in 0..4 {
        let clone = event_loop.clone();
        thread::spawn(move || {
            EventLoopBuilder::set_current_loop(clone.clone());
            clone.run();
            debug!("clone run done");
        });
        debug!("spawn thread done");
    }
    sleep_ms(1000 * 1000);
}

use lazy_static::lazy_static;
lazy_static! {
    //static ref POOL: Arc<EventLoopPool> = Arc::new(EventLoopPool::new(4));
    static ref POOL: EventLoopPool = EventLoopPool::new(4);
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
        .filter(None, LevelFilter::Warn)
        .init();

    debug!("Starting main");

    //let pool = EventLoopPool::new(1);
    for poller in POOL.get_all_poller().iter() {
        let tcp_server = Arc::new(Mutex::new(
                TcpServer::new(
                    poller.clone(),
                    &"0.0.0.0:9528".to_string()
                )
        ));
        tcp_server.lock().unwrap().start::<Test>();
        TcpServer::start_internal(tcp_server);
    }
    //EventLoopPool::wait(POOL);
    //(*POOL).wait();
    sleep_ms(1000 * 1000);
}


// RUST_LOG=debug target/debug/examples/tcpserver 
