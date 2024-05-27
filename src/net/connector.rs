use std::{io};
use bytes::{BytesMut};
use {PollOpt, Ready, Token, TokenType, TokenEntry};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::net::{SocketAddr};
use std::sync::{Arc, Mutex};
use event_imp::ready_from_usize;
use log::debug;

pub type ConnJob = Arc<Mutex<dyn FnMut(TcpStream)->bool + 'static + Send + Sync>>;

pub struct Connector {
    //addr: String,
    tcp_stream: Option<TcpStream>,
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

impl Connector {
    pub fn new(event_loop: Arc<EventLoop>) -> Connector {
        Connector {
            tcp_stream: None,
            event_loop,
            is_connected: false,
            on_conn_job: Arc::new(Mutex::new((move |_| { true }))),
        }
    }

    pub fn set_conn_job(&mut self, job: ConnJob) {
        self.on_conn_job = job;
    }

    fn handle_on_connect(&mut self) -> io::Result<()> {
        // check connect ret
        // del event
        //let _ = (self.on_conn_job.lock().unwrap())(self.tcp_stream.take().unwrap());
        /*
        let cb = match self.on_conn_job.lock() {
            Some(cb) => cb,
            None => {
                debug!("cb is None");
                assert_eq!(0, 1);
            },
        };
        */
        let mut cb = self.on_conn_job.lock().unwrap();

        let stream = match self.tcp_stream.take() {
            Some(stream) => stream,
            None => {
                debug!("stream is None");
                assert_eq!(0, 1);
                return Ok(());
            },
        };

        cb(stream);

        Ok(())
    }

    pub fn handle_event(&mut self, event: i64) -> io::Result<()> {
        let ready = ready_from_usize(event as usize);
        debug!("ready: {:?}", ready);

        if ready.is_writable() {
            self.handle_on_connect();
        }

        if ready.is_error() ||
                ready.is_hup() {
        }
        Ok(())
    }

    pub fn connect(this: Arc<Mutex<Self>>, addr: &String) {
        let stream = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        debug!("connect stream: {:?}", stream);

        this.lock().unwrap().tcp_stream = Some(stream);
        let job = Arc::new(Mutex::new(
            enclose! {
                (this)
                move |val: i64| {
                    this.lock().unwrap().handle_event(val);
                }
            }
        ));

        let event_loop = this.lock().unwrap().event_loop.clone();
        event_loop.register(this.lock().unwrap().tcp_stream.as_ref().unwrap(),
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

