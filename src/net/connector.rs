use std::{io};
use bytes::{BytesMut};
use {PollOpt, Ready, Token, TokenType, TokenEntry};
use net::{EventLoop, TcpStream, Acceptor, TcpConnection};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::net::{SocketAddr};
use std::sync::{Arc, Mutex};
use log::debug;

pub struct Connector {
    //addr: String,
    connector: Option<TcpStream>,
    event_loop: Arc<EventLoop>,
    is_connected: bool,
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
        }
    }

    fn handleRead(&mut self) -> io::Result<()> {
        // check connect ret
        // del event
        // cb (new connection ?)
        println!("handleRead...");
        Ok(())
    }

    pub fn handleEvent(&mut self, event: i64) -> io::Result<()> {
        self.handleRead()
    }

    pub fn connect(this: Arc<Mutex<Self>>, addr: &String) {
        let sock = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        this.lock().unwrap().connector = Some(sock);
        let job = Arc::new(Mutex::new(
            enclose! {
                (this)
                move |val: i64| {
                    this.lock().unwrap().handleEvent(val);
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

