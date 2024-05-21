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

fn main1() {
    let pool = EventLoopPool::new(1);
    for poller in pool.get_all_poller().iter() {
        let job = Arc::new(Mutex::new(|| {
            let event_loop = EventLoopBuilder::get_current_loop();
            let connector = Arc::new(Mutex::new(Connector::new(event_loop.clone())));
            connector.lock().unwrap().set_conn_job(Box::new(|stream: TcpStream| {
                println!("stream: {:?}", stream);
                let event_loop = EventLoopBuilder::get_current_loop();
                let conn = Arc::new(Mutex::new(TcpConnection::new(event_loop.clone(), stream)));
                let conn_clone = conn.clone();

                let job = Arc::new(Mutex::new(
                        move |val: i64| {
                            conn_clone.lock().unwrap().handle_event(val).unwrap();
                        }
                ));

                event_loop.deregister(&conn.lock().unwrap().sock).unwrap();
                event_loop.register(
                        &conn.lock().unwrap().sock,
                        TokenEntry {
                            ttype: TokenType::SocketEvent, 
                            token: Token (0)
                        },
                        Ready::readable() | Ready::writable(),
                        PollOpt::edge(), 
                        job
                ).unwrap();

                false
            }));
            Connector::connect(connector, &"127.0.0.1:90".to_string());
        }));
        enqueue_job(poller.clone(), job);
    }
    sleep_ms(1000 * 1000);
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

impl ClientHandler for TestClient {
    fn shutdown(&mut self) {
    }

    fn on_connect(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        println!("conn connected ?");
    }

    fn on_recv(&mut self, bytes: &mut BytesMut) {
        println!("on_recv: {:?}", bytes);
    }

    fn on_written(&mut self) -> bool {
        println!("write done");
        false
    }

    fn on_error(&mut self) {
        println!("connect error");
    }
}


fn main() {
    let pool = EventLoopPool::new(1);
    for poller in pool.get_all_poller().iter() {
        let mut cli = TcpClient::new(poller.clone());
        TcpClient::start_connect(
                Arc::new(Mutex::new(cli)),
                &"127.0.0.1:90".to_string(),
                Arc::new(Mutex::new(TestClient::new()))
        );
    }
    sleep_ms(1000 * 1000);
}

