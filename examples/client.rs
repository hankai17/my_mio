extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;

use my_mio::{PollOpt, Ready, Token, Registration, TokenType, TokenEntry};
use my_mio::timer::{Timeout};
use my_mio::net::Connector;
use bytes::{Buf, BytesMut};
use my_mio::net::{EventLoop, EventLoopBuilder,  EventLoopPool, TcpConnection, TcpServer, Handler, TcpStream};
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

fn main() {
    let pool = EventLoopPool::new(1);
    for poller in pool.get_all_poller().iter() {
        let job = Arc::new(Mutex::new(|| {
            let event_loop = EventLoopBuilder::get_current_loop();
            let connector = Arc::new(Mutex::new(Connector::new(event_loop.clone())));
            connector.lock().unwrap().set_writ_job(Box::new(|stream: TcpStream| {
                println!("stream: {:?}", stream);
                let event_loop = EventLoopBuilder::get_current_loop();
                let conn = Arc::new(Mutex::new(TcpConnection::new(event_loop.clone(), stream)));
                false
            }));
            Connector::connect(connector, &"127.0.0.1:90".to_string());
        }));
        enqueue_job(poller.clone(), job);
    }
    sleep_ms(1000 * 1000);
}

