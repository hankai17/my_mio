use std::{io};
use bytes::{BytesMut};
use {PollOpt, Ready, Token, TokenType, TokenEntry};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::net::{SocketAddr};
use std::sync::{Arc, Mutex};
use event_imp::ready_from_usize;
use log::debug;

pub type ConnJob = Box<dyn FnMut(TcpStream)->bool + 'static + Send + Sync>;

pub struct Connector {
    //addr: String,
    connector: Option<TcpStream>,
    event_loop: Arc<EventLoop>,
    is_connected: bool,
    on_conn_job: ConnJob,
}

macro_rules! enclose {
    ( ($( $x:ident ),*) $y:expr ) => {
        {
            $(let $x = $x.clone();)*
            $y
        }
    };
}

fn default_connected_cb(stream: &mut TcpStream) {}

impl Connector {
    pub fn new(event_loop: Arc<EventLoop>) -> Connector {
        Connector {
            connector: None,
            event_loop,
            is_connected: false,
            on_conn_job: Box::new(move |_| { true }),
        }
    }

    pub fn set_conn_job(&mut self, job: ConnJob) {
        self.on_conn_job = job;
    }

    fn handle_on_connect(&mut self) -> io::Result<()> {
        // check connect ret
        // del event
        let _ = (self.on_conn_job)(self.connector.take().unwrap());
        //let conn = Arc::new(Mutex::new(
        //        TcpConnection::new(
        //            self.event_loop.clone(),
        //            self.connector.take().unwrap()
        //        )
        //));

        Ok(())
    }

    pub fn handle_event(&mut self, event: i64) -> io::Result<()> {
        let ready = ready_from_usize(event as usize);
        if ready.is_writable() {
            self.handle_on_connect();
        }
        if ready.is_error() ||
                ready.is_hup() {
        }
        Ok(())
    }

    pub fn connect(this: Arc<Mutex<Self>>, addr: &String) {
        let sock = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        this.lock().unwrap().connector = Some(sock);
        let job = Arc::new(Mutex::new(
            enclose! {
                (this)
                move |val: i64| {
                    this.lock().unwrap().handle_event(val);
                }
            }
        ));
        let event_loop = this.lock().unwrap().event_loop.clone();
        event_loop.register(this.lock().unwrap().connector.as_ref().unwrap(),
                TokenEntry {
                    ttype: TokenType::SocketEvent,
                    token: Token(0),
                },
                Ready::writable() | Ready::readable(),
                PollOpt::edge() | PollOpt::oneshot(),
                job
        ).unwrap();
    }
}

