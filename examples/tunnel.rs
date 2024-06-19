extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;

use my_mio::{PollOpt, Ready, Token, Registration, TokenType, TokenEntry};
use my_mio::timer::{Timeout};
use bytes::{Buf, BytesMut};
use my_mio::net::{EventLoop, EventLoopBuilder,  EventLoopPool, TcpConnection, TcpServer, Handler};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::thread;
use log::{debug, info};

use my_mio::net::{Connector, TcpClient, ClientHandler};

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

struct Client {
    timer: Option<Timeout>,
    server_conn: Option<Weak<Mutex<TcpConnection>>>,
    client_conn: Option<Weak<Mutex<TcpConnection>>>,
}

impl Client {               // tunnel的client端处理
    fn new(server_conn: Weak<Mutex<TcpConnection>>) -> Client {
        Client {
            timer: None,
            server_conn: Some(server_conn),
            client_conn: None,
        }
    } 
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!("dropping for Client")
    }
}

impl ClientHandler for Client {                                     // hankai2
    fn shutdown(&mut self) {
    }

    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.client_conn = Some(Arc::downgrade(&conn));
    }

    fn on_connect(&mut self, conn: Arc<Mutex<TcpConnection>>) {         // hankai2.1
        // if connected
        let s_conn = self.server_conn.take().unwrap();
        let cs = match s_conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.server_conn = Some(s_conn);

        let poller = EventLoopBuilder::get_current_loop();
        let job = Arc::new(Mutex::new(move|| {
            let mut ss = conn.lock().unwrap();                           // hankai2.2
            //ss 监听读
            let mut bytes = cs.lock().unwrap().read_buffer.take().unwrap();
            let len = bytes.len();
            if len > 0 {
                ss.send(bytes.clone());
                bytes.advance(len);
            }
            cs.lock().unwrap().read_buffer = Some(bytes);
        }));
        enqueue_job(poller.clone(), job);
    }

    fn on_recv(&mut self, bytes: &mut BytesMut) {
        info!("on_recv: {:?}", bytes);
        let s_conn = self.server_conn.take().unwrap();
        let cs = match s_conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.server_conn = Some(s_conn);
        
        if bytes.len() > 0 {
            cs.lock().unwrap().send(bytes.clone());
            bytes.advance(bytes.len());
        }

        // job?
    }

    fn on_written(&mut self) -> bool {
        debug!("write done");
        true
    }

    fn on_error(&mut self) {
        info!("connect error");
    }
}

struct Tunnel {
    client: TcpClient,
    pub client_handler: Option<Weak<Mutex<Client>>>,
    pub server_conn: Option<Weak<Mutex<TcpConnection>>>,
}

impl Tunnel {
    pub fn new(event_loop: Arc<EventLoop>, addr: &String,
            server_conn: Weak<Mutex<TcpConnection>>) -> Tunnel {
        Tunnel {
            client: TcpClient::new(event_loop),
            client_handler: None,
            server_conn: Some(server_conn),                                 // hankai1
        }
    }

    pub fn connect(&mut self) {
        let s_conn = self.server_conn.take().unwrap();
        let s_conn_clone = match s_conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.server_conn = Some(s_conn);
        let handler = Arc::new(Mutex::new(Client::new(
            Arc::downgrade(&s_conn_clone)
        )));
        self.client.start_connect(&"127.0.0.1:90".to_string(), handler.clone());    // hankai2
        self.client_handler = Some(Arc::downgrade(&handler));
    }
}
                                                                            // hankai0 tunnel生命是在 server端起cs时 开始的

struct ServerSession {
    timer: Option<Timeout>, //pub type TimerJob = Box<dyn FnMut() + 'static + Send + Sync>;
    conn: Option<Weak<Mutex<TcpConnection>>>,
    tunnel: Option<Arc<Mutex<Tunnel>>>
}

impl ServerSession {
}

impl Drop for ServerSession {
    fn drop(&mut self) {
        //println!("---------------------dropping for ServerSession")
    }
}

impl Handler for ServerSession {
    fn new() -> ServerSession {
        ServerSession {
            timer: None,
            conn: None,
            tunnel: None,
        }
    }

    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.conn = Some(Arc::downgrade(&conn));
    }

    fn free_connection(&mut self) {
        self.conn = None;
    }

    fn on_accept(&mut self) {
        let conn = self.conn.take().unwrap();
        let conn_clone = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.conn = Some(conn);
        let event_loop = EventLoopBuilder::get_current_loop();


        let mut tunnel = Arc::new(Mutex::new(
            Tunnel::new(
                event_loop.clone(),
                &"127.0.0.1:90".to_string(),
                Arc::downgrade(&conn_clone))
        ));
        tunnel.lock().unwrap().connect();
        self.tunnel = Some(tunnel.clone());

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
        let mut ss = match &self.tunnel {
            Some(tunnel) => {
                match &tunnel.lock().unwrap().client_handler {
                    Some(handler) => {
                        match handler.upgrade() {
                            Some(cli) => {
                                match &cli.lock().unwrap().client_conn {
                                    Some(conn) => {
                                        match conn.upgrade() {
                                            Some(con) => con.clone(),
                                            None => return,
                                        }
                                    },
                                    None => return,
                                }
                            },
                            None => return,
                        }
                    },
                    None => return,
                }
            },
            None => return,
        };

        ss.lock().unwrap().send(bytes.clone());
        bytes.advance(bytes.len());
    }

    fn on_written(&mut self) -> bool {
        self.free_connection();
        false
    }

    fn on_error(&mut self) {
        debug!("ServerSession onError");
    }
}

fn sleep_ms(ms: u64) {
    use std::thread;
    use std::time::Duration;
    thread::sleep(Duration::from_millis(ms));
}

fn main() {
    let _ = ::env_logger::init();
    debug!("Starting main");
    /*
    let pool = EventLoopPool::new(1);
    for poller in pool.get_all_poller().iter() {
        let tcp_server = Arc::new(Mutex::new(
            TcpServer::new(
                poller.clone(),
                &"0.0.0.0:9528".to_string()
            )
        ));
        tcp_server.lock().unwrap().start::<ServerSession>();
        TcpServer::start_internal(tcp_server);
    }
    pool.wait();
    */

    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    
    let event_loop = b.get_build().unwrap();

    let tcp_server = Arc::new(Mutex::new(TcpServer::new(event_loop.clone(), &"0.0.0.0:9528".to_string())));
    tcp_server.lock().unwrap().start::<ServerSession>();
    TcpServer::start_internal(tcp_server);

    for i in 0..1 {
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

