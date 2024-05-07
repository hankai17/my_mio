use bytes::{BytesMut};
use {PollOpt, Ready, Token, TokenType, TokenEntry};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection};
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
    fn attachConnection(&mut self, conn: Arc<Mutex<TcpConnection>>);
    fn freeConnection(&mut self);
    fn onAccept(&mut self);
    fn onRecv(&mut self, bytes: &mut BytesMut);
    fn onWritten(&mut self) -> bool;
    fn onError(&mut self);
    //fn send(&mut self, bytes: &mut BytesMut) -> io::Result<()>;
    ////fn shutdown();
    ////fn safeShutdown();
}

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

pub struct TcpServer {
    event_loop: Arc<Mutex<EventLoop>>, 
    acceptor: Arc<Mutex<Acceptor>>,
    // timer
    session_alloc: Option<fn() -> Arc<Mutex<dyn Handler + 'static + Send + Sync>>>,
    // on_read_cb
    // on_written_cb
    // on_err_cb
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {
    //println!("move it into struct TODO");
}

unsafe impl Send for TcpServer {}
unsafe impl Sync for TcpServer {}

static SOCKET_TOKEN_ID: AtomicUsize = AtomicUsize::new(0);

impl TcpServer {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, addr: &String) -> TcpServer {
        let acceptor = Arc::new(Mutex::new(Acceptor::new(event_loop.clone(), addr)));
        TcpServer {
            event_loop: event_loop,
            acceptor: acceptor,
            session_alloc: None,
        }
    }

    pub fn onAcceptConnection(&mut self, stream: TcpStream, _addr: SocketAddr) {
        let conn = Arc::new(Mutex::new(TcpConnection::new(self.event_loop.clone(), stream)));
        let session = self.session_alloc.unwrap()();
        session.lock().unwrap().attachConnection(conn.clone());

        session.lock().unwrap().onAccept();

        conn.lock().unwrap().set_read_job(
            Box::new (enclose! { 
                (session)
				move |bytes: &mut BytesMut| {
                    session.lock().unwrap().onRecv(bytes);
                }
            })
        );

        conn.lock().unwrap().set_writ_job(
            Box::new (enclose! {
                (session)
                move || {
                    session.lock().unwrap().onWritten()
                }
            })
        );

        let job = Arc::new(Mutex::new(
            enclose! {
                (conn)
                move |val: i64| {
                    conn.lock().unwrap().handleEvent(val);
                }
            }
        ));

        self.event_loop.lock().unwrap().register(
                &conn.lock().unwrap().sock,
                TokenEntry {
                    ttype: TokenType::SocketEvent, 
                    token: Token (
                        SOCKET_TOKEN_ID.fetch_add(1, Ordering::Relaxed) + 1
                    )
                },
                Ready::readable() | Ready::writable(),
                PollOpt::edge(), 
                job
        );
    }

    pub fn start_internal(this: Arc<Mutex<Self>>) {
        let acceptor = this.lock().unwrap().acceptor.clone();
        this.lock().unwrap().acceptor.lock().unwrap().bind(
            Arc::new(Mutex::new(
                move |val: i64| {
                    acceptor.lock().unwrap().handleRead(val);
                }
            ))
        );

        let job = Box::new(
            enclose! {
                (this)
                move |stream: TcpStream, addr: SocketAddr| {
                    this.lock().unwrap().onAcceptConnection(stream, addr);
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

