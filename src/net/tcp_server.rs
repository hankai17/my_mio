use bytes::{BytesMut};
use {PollOpt, Ready, Token};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection};
use std::net::{SocketAddr};
use std::sync::{Arc, Mutex};

pub trait Handler {
    //type Connection;
    fn new() -> Self where Self: Sized;
    fn setConnection(&mut self, conn: Arc<Mutex<TcpConnection>>);
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

const CLIENT: Token = Token(10_000_000);

unsafe impl Send for TcpServer {}
unsafe impl Sync for TcpServer {}

impl TcpServer {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, addr: &String) -> TcpServer {
        let mut acceptor = Arc::new(Mutex::new(Acceptor::new(event_loop.clone(), addr)));
        TcpServer {
            event_loop: event_loop,
            acceptor: acceptor,
            session_alloc: None,
        }
    }
    pub fn onAcceptConnection(&mut self, stream: TcpStream, addr: SocketAddr) {
        println!("accept: {}, {:?}", addr, stream);
        let mut session = self.session_alloc.unwrap()();
        let mut conn = Arc::new(Mutex::new(TcpConnection::new(self.event_loop.clone(), stream)));
        let mut clone_conn = conn.clone();
        let mut clone_session = session.clone();
        session.lock().unwrap().setConnection(conn);

        let read_job = Box::new(move |bytes: &mut BytesMut| { session.lock().unwrap().onRecv(bytes); } );
        clone_conn.lock().unwrap().set_read_job(read_job);
        let writ_job = Box::new(move || { clone_session.lock().unwrap().onWritten() });
        clone_conn.lock().unwrap().set_writ_job(writ_job);

        let conn = clone_conn.clone();
        let job = Arc::new(Mutex::new(move |val: i64| { clone_conn.lock().unwrap().handleEvent(val); }));
        self.event_loop.lock().unwrap().register(&conn.lock().unwrap().sock, CLIENT, 
                Ready::readable() | Ready::writable(), PollOpt::edge(), job);
    }
    fn start_internal(&mut self) {
        let mut clone = self.acceptor.clone();
        let job = Arc::new(Mutex::new(move |val: i64| { clone.lock().unwrap().handleRead(val); }));
        self.acceptor.lock().unwrap().bind(job);
        self.acceptor.lock().unwrap().set_accept_cb(default_accept_cb);
    }
    pub fn start_internal1(this: Arc<Mutex<Self>>) {
        let mut clone = this.lock().unwrap().acceptor.clone();
        let job = Arc::new(Mutex::new(move |val: i64| { clone.lock().unwrap().handleRead(val); }));
        this.lock().unwrap().acceptor.lock().unwrap().bind(job);

        let mut clone = this.clone();
        //let job = Box::new(move |val: &mut TcpConnection| { clone.lock().unwrap().onAcceptConnection(val); });
        let job = (Box::new(move |stream: TcpStream, addr: SocketAddr| { clone.lock().unwrap().onAcceptConnection(stream, addr); }));
        this.lock().unwrap().acceptor.lock().unwrap().set_accept_job(job);
    }
    pub fn start<H: Sized + 'static + Send + Sync>(&mut self)
        where H: Handler {
        let mut session_alloc = || -> Arc<Mutex<dyn Handler + 'static + Send + Sync>> {
            Arc::new(Mutex::new(<H as Handler>::new()))
        };
        self.session_alloc = Some(session_alloc);
        //self.start_internal();
    }
}

