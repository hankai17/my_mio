use bytes::{BytesMut};
use {PollOpt, Ready, Token, TokenType, TokenEntry};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection, Connector};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::net::{SocketAddr};
use std::sync::{Arc, Mutex};

macro_rules! enclose {
    ( ($( $x:ident ),*) $y:expr ) => {
        {
            $(let $x = $x.clone();)*
            $y
        }
    };
}

pub trait Handler {
    //type Connection;
    fn new() -> Self where Self: Sized;
    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>);
    fn free_connection(&mut self);
    fn on_accept(&mut self);
    fn on_recv(&mut self, bytes: &mut BytesMut);
    fn on_written(&mut self) -> bool;
    fn on_error(&mut self);
    //fn send(&mut self, bytes: &mut BytesMut) -> io::Result<()>;
    ////fn shutdown();
    ////fn safeShutdown();
}

/*
pub struct SessionManager {
    // map<string, weak<Session>>
}

impl SessionManager {
    /*
    fn add(&s: String, session: Arc<Mutex<Session>>) -> bool {
        false
    }
    fn del(&s: String) {
    }
    */
}
*/

pub struct TcpServer {
    event_loop: Arc<EventLoop>, 
    acceptor: Arc<Mutex<Acceptor>>,
    // timer
    session_alloc: Option<fn() -> Arc<Mutex<dyn Handler + 'static + Send + Sync>>>,
    // on_read_cb
    // on_written_cb
    // on_err_cb
}

unsafe impl Send for TcpServer {}
unsafe impl Sync for TcpServer {}

static SOCKET_TOKEN_ID: AtomicUsize = AtomicUsize::new(0);

impl TcpServer {
    pub fn new(event_loop: Arc<EventLoop>, addr: &String) -> TcpServer {
        let acceptor = Arc::new(Mutex::new(Acceptor::new(event_loop.clone(), addr)));
        TcpServer {
            event_loop: event_loop,
            acceptor: acceptor,
            session_alloc: None,
        }
    }

    pub fn on_accept_connection(&mut self, stream: TcpStream, _addr: SocketAddr) {
        let conn = Arc::new(Mutex::new(TcpConnection::new(self.event_loop.clone(), stream)));
        let session = self.session_alloc.unwrap()();
        session.lock().unwrap().attach_connection(conn.clone());
        session.lock().unwrap().on_accept();

        conn.lock().unwrap().set_read_job(
            Box::new (enclose! {
                (session)
				move |bytes: &mut BytesMut| {
                    session.lock().unwrap().on_recv(bytes);
                }
            })
        );

        conn.lock().unwrap().set_writ_job(
            Box::new (enclose! {
                (session)
                move || {
                    session.lock().unwrap().on_written()
                }
            })
        );

        conn.lock().unwrap().set_close_job(
            Box::new (enclose! {
                (session)
                move || {
                    session.lock().unwrap().on_error()
                }
            })
        );

        let job = Arc::new(Mutex::new(
            enclose! {
                (conn)
                move |val: i64| {
                    conn.lock().unwrap().handle_event(val).unwrap();
                }
            }
        ));

        self.event_loop.register(
            &conn.lock().unwrap().tcp_stream,
            TokenEntry {
                ttype: TokenType::SocketEvent, 
                token: Token (
                    SOCKET_TOKEN_ID.fetch_add(1, Ordering::Relaxed) + 1
                )
            },
            Ready::readable() | Ready::writable(),
            PollOpt::edge(), 
            job
        ).unwrap();
    }

    pub fn start_internal(this: Arc<Mutex<Self>>) {
        let acceptor = this.lock().unwrap().acceptor.clone();
        this.lock().unwrap().acceptor.lock().unwrap().bind(
            Arc::new(Mutex::new(
                move |val: i64| {
                    acceptor.lock().unwrap().handle_read(val);
                }
            ))
        );

        let job = Box::new(
            enclose! {
                (this)
                move |stream: TcpStream, addr: SocketAddr| {
                    this.lock().unwrap().on_accept_connection(stream, addr);
                }
            }
        );
        this.lock().unwrap().acceptor.lock().unwrap().set_accept_job(job);
    }

    pub fn start<H: Sized + 'static + Send + Sync>(&mut self)
        where H: Handler {
        let session_alloc = || -> Arc<Mutex<dyn Handler + 'static + Send + Sync>> {
            Arc::new(Mutex::new(<H as Handler>::new()))
        };
        self.session_alloc = Some(session_alloc);
    }
}

pub trait ClientHandler {
    //fn start_connect(&mut self, );
    //fn free_connection(&mut self);
    fn shutdown(&mut self);
    fn attach_connection(&mut self, conn: Arc<Mutex<TcpConnection>>);
    //fn free_connection(&mut self);

    fn on_connect(&mut self, conn: Arc<Mutex<TcpConnection>>);
    fn on_recv(&mut self, bytes: &mut BytesMut);
    fn on_written(&mut self) -> bool;
    fn on_error(&mut self);
}

pub struct TcpClient {
    event_loop: Arc<EventLoop>, 
    connector: Arc<Mutex<Connector>>,
}

unsafe impl Send for TcpClient {}
unsafe impl Sync for TcpClient {}

impl TcpClient {
    pub fn new(event_loop: Arc<EventLoop>) -> TcpClient {
        TcpClient {
            event_loop: event_loop.clone(),
            connector: Arc::new(Mutex::new(Connector::new(event_loop.clone()))),
        }
    }

    pub fn start_connect(&mut self, addr: &String,
            handler: Arc<Mutex<dyn ClientHandler + 'static + Send + Sync>>) {
        let event_loop = self.event_loop.clone();
        let handler_clone = handler.clone();

        let conn_job = Arc::new(Mutex::new(move |stream: TcpStream| {
            let conn = Arc::new(Mutex::new(
                TcpConnection::new(event_loop.clone(), stream)
            ));
            handler_clone.lock().unwrap().attach_connection(conn.clone());

            conn.lock().unwrap().set_read_job(
                Box::new (enclose! { 
                    (handler)
			    	move |bytes: &mut BytesMut| {
                        handler.lock().unwrap().on_recv(bytes);
                    }
                })
            );

            conn.lock().unwrap().set_writ_job(
                Box::new (enclose! {
                    (handler)
                    move || {
                        handler.lock().unwrap().on_written()
                    }
                })
            );

            conn.lock().unwrap().set_close_job(
                Box::new (enclose! {
                    (handler)
                    move || {
                        handler.lock().unwrap().on_error()
                    }
                })
            );

            let job = Arc::new(Mutex::new(
                enclose! {
                    (conn)
                    move |val: i64| {
                        conn.lock().unwrap().handle_event(val).unwrap();
                    }
                }
            ));

            event_loop.deregister(&conn.lock().unwrap().tcp_stream).unwrap();

            use std::os::fd::AsRawFd;
            let fd = conn.lock().unwrap().tcp_stream.as_raw_fd();

            conn.lock().unwrap().set_enabled(true);
            event_loop.register(
                &conn.lock().unwrap().tcp_stream,
                TokenEntry {
                    ttype: TokenType::SocketEvent, 
                    token: Token (
                        fd as usize
                    )
                },
                Ready::readable() | Ready::writable(),  // 默认监听读写 // 设计理念是全局只调用一次epoll_ctl
                PollOpt::edge(), 
                job
            ).unwrap();

            handler.lock().unwrap().on_connect(conn);

            true
        }));

        self.connector.lock().unwrap().set_conn_job(conn_job);

        Connector::connect(self.connector.clone(), addr, usize::MAX);
    }

}

