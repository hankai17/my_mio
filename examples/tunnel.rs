extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;
extern crate chrono;

use my_mio::{PollOpt, Ready, Token, Registration, TokenType, TokenEntry};
//use my_mio::timer::{Timeout};
use bytes::{Buf, BytesMut};
use my_mio::net::{EventLoop, EventLoopBuilder,  EventLoopPool, TcpConnection, TcpServer, Handler};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use std::thread;

use log::{debug, info, LevelFilter};
use std::io::Write;
use std::fs::File;
use chrono::Local;
use env_logger::{Builder};

use std::net::Shutdown;
use my_mio::net::{TcpClient, ClientHandler};

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

struct ServerSession {
    //timer: Option<Timeout>,
    client_conn: Option<Weak<Mutex<TcpConnection>>>,
    server_conn: Option<Weak<Mutex<TcpConnection>>>,
}

impl ServerSession {               // tunnel的client端处理
    fn new(conn: Weak<Mutex<TcpConnection>>) -> ServerSession {
        ServerSession {
            //timer: None,
            client_conn: Some(conn),
            server_conn: None,
        }
    } 
}

impl Drop for ServerSession {
    fn drop(&mut self) {
        debug!("dropping for ServerSession")
    }
}

impl ClientHandler for ServerSession {
    fn shutdown(&mut self) {
    }

    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>) {
        self.server_conn = Some(Arc::downgrade(&conn));
    }

    fn on_connect(&mut self, new_conn: Arc<Mutex<TcpConnection>>) {
        // if connected
        let conn = self.client_conn.take().unwrap();
        let cs = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.client_conn = Some(conn);

        let poller = EventLoopBuilder::get_current_loop();
        let job = Arc::new(Mutex::new(move|| {
            let mut ss = new_conn.lock().unwrap();
            //ss 监听读
            let mut bytes = cs.lock().unwrap().read_buffer.take().unwrap();
            let len = bytes.len();
            if len > 0 {
                ss.send(bytes.clone()).unwrap();
                bytes.advance(len);
            }
            cs.lock().unwrap().read_buffer = Some(bytes);
        }));
        enqueue_job(poller.clone(), job);
    }

    fn on_recv(&mut self, bytes: &mut BytesMut) {
        info!("on_recv: {:?}", bytes);
        let conn = self.client_conn.take().unwrap();
        let cs = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.client_conn = Some(conn);
        
        if bytes.len() > 0 {
            cs.lock().unwrap().send(bytes.clone()).unwrap();
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
        self.server_conn = None;
        let conn = self.client_conn.take().unwrap();
        let cs = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.client_conn = Some(conn);
        cs.lock().unwrap().shutdown(Shutdown::Write).unwrap();
    }
}

struct Tunnel {
    addr: String,
    client: TcpClient,
    pub server_session: Option<Weak<Mutex<ServerSession>>>,
    pub client_conn: Option<Weak<Mutex<TcpConnection>>>,
}

impl Tunnel {
    pub fn new(event_loop: Arc<EventLoop>, addr: &String,
            client_conn: Weak<Mutex<TcpConnection>>) -> Tunnel {
        Tunnel {
            addr: String::from(addr),
            client: TcpClient::new(event_loop),
            server_session: None,
            client_conn: Some(client_conn),
        }
    }

    pub fn connect(&mut self) {
        let conn = self.client_conn.take().unwrap();
        let s_conn_clone = match conn.upgrade() {
            Some(conn) => conn.clone(),
            None => return,
        };
        self.client_conn = Some(conn);
        let session = Arc::new(Mutex::new(ServerSession::new(
            Arc::downgrade(&s_conn_clone)
        )));
        self.client.start_connect(&self.addr.to_string(), session.clone());
        self.server_session = Some(Arc::downgrade(&session));
    }
}

struct TunnelServer {
    //timer: Option<Timeout>, //pub type TimerJob = Box<dyn FnMut() + 'static + Send + Sync>;
    conn: Option<Weak<Mutex<TcpConnection>>>,
    tunnel: Option<Arc<Mutex<Tunnel>>>
}

impl TunnelServer {
}

impl Drop for TunnelServer {
    fn drop(&mut self) {
        //println!("---------------------dropping for TunnelServer")
    }
}

impl Handler for TunnelServer {
    fn new() -> TunnelServer {
        TunnelServer {
            //timer: None,
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


        let tunnel = Arc::new(Mutex::new(
            Tunnel::new(
                event_loop.clone(),
                &"127.0.0.1:80".to_string(),
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
        let ss = match &self.tunnel {
            Some(tunnel) => {
                match &tunnel.lock().unwrap().server_session {
                    Some(handler) => {
                        match handler.upgrade() {
                            Some(cli) => {
                                match &cli.lock().unwrap().server_conn {
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

        /*
        let mut ss = self.tunnel.unwrap_or(None)
                        .lock().unwrap()
                        .server_session.unwrap_or(None)
                        .upgrade().unwrap_or(None)
                        .lock().unwrap()
                        .server_conn.unwrap_or(None)
                        .upgrade();
        let mut ss = match ss {
            Some(ss) => ss,
            None => return,
        };
        */
        //fn fun_lock()

        /*
        let mut ss = self.tunnel.unwrap_or(None)
                        .lock().unwrap()
                        .server_session.unwrap_or(None)
                        .upgrade().unwrap_or(None)
                        .lock().unwrap()
                        .server_conn.unwrap_or(None)
                        .upgrade();
        let mut ss = match ss {
            Some(ss) => ss,
            None => return,
        };
        */

        ss.lock().unwrap().send(bytes.clone()).unwrap();
        bytes.advance(bytes.len());
    }

    fn on_written(&mut self) -> bool {
        true
    }

    fn on_error(&mut self) {
        self.free_connection();
        debug!("TunnelServer on_error");
        let ss = match &self.tunnel {
            Some(tunnel) => {
                match &tunnel.lock().unwrap().server_session {
                    Some(handler) => {
                        match handler.upgrade() {
                            Some(cli) => {
                                match &cli.lock().unwrap().server_conn {
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
        ss.lock().unwrap().shutdown(Shutdown::Write).unwrap();
    }
}

fn sleep_ms(ms: u64) {
    //use std::thread;
    //use std::time::Duration;
    thread::sleep(Duration::from_millis(ms));
}

fn main() {
    //let _ = ::env_logger::init();
    sleep_ms(1);
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
        .filter(None, LevelFilter::Error)
        .init();

    debug!("Starting main");
    let pool = EventLoopPool::new(1);
    for poller in pool.get_all_poller().iter() {
        let tcp_server = Arc::new(Mutex::new(
            TcpServer::new(
                poller.clone(),
                &"0.0.0.0:9528".to_string()
            )
        ));
        tcp_server.lock().unwrap().start::<TunnelServer>();
        TcpServer::start_internal(tcp_server);
    }
    pool.wait();

    /*
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    
    let event_loop = b.get_build().unwrap();

    let tcp_server = Arc::new(Mutex::new(TcpServer::new(event_loop.clone(), &"0.0.0.0:9528".to_string())));
    tcp_server.lock().unwrap().start::<TunnelServer>();
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
    */
}

