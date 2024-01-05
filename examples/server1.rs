extern crate my_mio;
extern crate slab;
extern crate bytes;

use my_mio::{Events, Poll, PollOpt, Ready, Token};
use my_mio::net::{TcpListener, TcpStream};
use bytes::{Buf, ByteBuf, MutBuf, MutByteBuf, SliceBuf};
use slab::Slab;
use std::io;
use std::thread;
use std::time::Duration;
use std::collections::LinkedList;
use my_mio::deprecated::{unix, EventLoop, Handler, EventLoopBuilder};

use std::io::{Read, Write};
trait MapNonBlock<T> {
    fn map_non_block(self) -> io::Result<Option<T>>;
}
impl<T> MapNonBlock<T> for io::Result<T> {  // 给io::Result<T>添加trait
    fn map_non_block(self) -> io::Result<Option<T>> {
        use std::io::ErrorKind::WouldBlock;
        match self {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if let WouldBlock = err.kind() {
                    Ok(None) 
                } else {
                    Err(err)
                }
            }
        }
    }
}
pub trait TryRead {
    fn try_read_buf<B: MutBuf>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
            where Self : Sized {
        let res = self.try_read(unsafe { buf.mut_bytes() });
        if let Ok(Some(cnt)) = res {
            unsafe { buf.advance(cnt); }
        }
        res 
    }
    fn try_read(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>>;
}

pub trait TryWrite {
    fn try_write_buf<B: Buf>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
        where Self : Sized {
        let res = self.try_write(buf.bytes());
        if let Ok(Some(cnt)) = res {
            buf.advance(cnt);
        }
        res
    }
    fn try_write(&mut self, buf: &[u8]) -> io::Result<Option<usize>>;
}

impl<T: Read> TryRead for T {
    fn try_read(&mut self, dst: &mut [u8]) -> io::Result<Option<usize>> {
        self.read(dst).map_non_block()
    }
}

impl<T: Write> TryWrite for T {
    fn try_write(&mut self, src: &[u8]) -> io::Result<Option<usize>> {
        self.write(src).map_non_block()
    }
}

const SERVER: Token = Token(10_000_000);
const CLIENT: Token = Token(10_000_001);

const N: usize = 1_000_000;

struct EchoConn {
    sock: TcpStream,
    token: Option<Token>,
    count: usize,
    buf: Vec<u8>
}

impl EchoConn {
    fn new(sock: TcpStream) -> EchoConn {
        let mut ec = EchoConn {
            sock: sock,
            token: None,
            buf: Vec::with_capacity(22),
            count: 0
        };
        unsafe { ec.buf.set_len(22) };
        ec
    }
    fn writable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        event_loop.reregister(&self.sock, self.token.unwrap(), Ready::readable(), 
                PollOpt::edge() | PollOpt::oneshot())
    }
    fn readable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        loop {
            match self.sock.try_read(&mut self.buf[..]) {
                Ok(None) => {
                    break;
                }
                Ok(Some(_)) => {
                    self.count += 1;
                    if self.count % 10000 == 0 {
                        println!("Received {} msg", self.count);
                    }
                    if self.count == N {
                        event_loop.shutdown();
                    }
                }
                Err(_) => {
                    break;
                }
            };
        }
        event_loop.reregister(&self.sock, self.token.unwrap(), Ready::readable(),
                PollOpt::edge() | PollOpt::oneshot())
    }
}

struct EchoServer {
    sock: TcpListener,
    conns: Slab<EchoConn>
}

impl EchoServer {
    fn accept(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        println!("Server accepting socket");
        let sock = self.sock.accept().unwrap().0;
        let conn = EchoConn::new(sock,);
        let tok = self.conns.insert(conn);
        self.conns[tok].token = Some(Token(tok));
        event_loop.register(&self.conns[tok].sock, Token(tok), Ready::readable(),
                PollOpt::edge() | PollOpt::oneshot())
            .expect("could not register socket with event loop");
        Ok(())
    }
    fn conn_readable(&mut self, event_loop: &mut EventLoop<Echo>, 
            token: Token) -> io::Result<()> {
        println!("server conn readable, token: {:?}", token);
        self.conn(token).readable(event_loop)
        
    }
    fn conn_writable(&mut self, event_loop: &mut EventLoop<Echo>, 
            token: Token) -> io::Result<()> {
        println!("server conn writable, token: {:?}", token);
        self.conn(token).writable(event_loop)
    }
    fn conn<'a>(&'a mut self, token: Token) -> &'a mut EchoConn {
        &mut self.conns[token.into()]
    }
}

struct EchoClient {
    sock: TcpStream,
    backlog: LinkedList<String>,
    token: Token,
    count: u32
}

impl EchoClient {
    fn new(sock: TcpStream, token: Token) -> EchoClient {
        EchoClient {
            sock: sock,
            backlog: LinkedList::new(),
            token: token,
            count: 0
        }
    }
    fn readable(&mut self, _event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        Ok(())
    }
    fn writable(&mut self, event_loop: &mut EventLoop<Echo>) -> io::Result<()> {
        println!("client socket writable");
        while self.backlog.len() > 0 {
            match self.sock.try_write(self.backlog.front().unwrap().as_bytes()) {
                Ok(None) => {
                    break;
                }
                Ok(Some(_)) => {
                    self.backlog.pop_front();
                    self.count += 1;
                    if self.count % 10000 == 0 {
                        println!("Send {} msg", self.count);
                    }
                }
                Err(e) => {
                    println!("not implemented; client err: {:?}", e);
                    break;
                }
            }
        }
        if self.backlog.len() > 0 {
            event_loop.reregister(&self.sock, self.token, Ready::writable(),
                    PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }
        Ok(())
    }
}

struct Echo {
    server: EchoServer,
    client: EchoClient,
}

impl Echo {
    fn new(srv: TcpListener, client: TcpStream) -> Echo {
        Echo {
            server: EchoServer {
                sock: srv,
                conns: Slab::with_capacity(128),
            },
            client: EchoClient::new(client, CLIENT),
        }
    }
}

impl Handler for Echo {
    type Timeout = usize;
    type Message = String;
    fn ready(&mut self, event_loop: &mut EventLoop<Echo>, token: Token, 
            events: Ready) {
        if events.is_readable() {
            match token {
                SERVER => self.server.accept(event_loop).unwrap(),
                CLIENT => self.client.readable(event_loop).unwrap(),
                i => self.server.conn_readable(event_loop, i).unwrap()
            }
        }
        if events.is_writable() {
            match token {
                SERVER => panic!("received writable for token 0"),
                CLIENT => self.client.writable(event_loop).unwrap(),
                _ => self.server.conn_writable(event_loop, token).unwrap()
            }
        }
    }
    fn notify(&mut self, event_loop: &mut EventLoop<Echo>, msg: String) {
        match self.client.sock.try_write(msg.as_bytes()) {
            Ok(Some(n)) => {
                self.client.count += 1;
                if self.client.count % 10000 == 0 {
                    println!("Send {} bytes: count {}", n, self.client.count);
                }
            },
            _ => {
                self.client.backlog.push_back(msg);
                event_loop.reregister(&self.client.sock, self.client.token, 
                        Ready::writable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
            }
        }
    }
}

fn sleep_ms(ms: u64) {
    use std::thread;
    thread::sleep(Duration::from_millis(ms));
}

fn main() {
    let mut b = EventLoopBuilder::new();
    b.notify_capacity(1_048_576)
        .messages_per_tick(64)
        .timer_tick(Duration::from_millis(100))
        .timer_wheel_size(1024)
        .timer_capacity(65536);
    let mut event_loop = b.build().unwrap();
    let addr = "127.0.0.1:9527".parse().unwrap();
    let srv = TcpListener::bind(&addr).unwrap();
    println!("listen for connections");
    event_loop.register(&srv, SERVER, Ready::readable(), 
            PollOpt::edge() | PollOpt::oneshot()).unwrap();
    let sock = TcpStream::connect(&addr).unwrap();
    event_loop.register(&sock, CLIENT, Ready::writable(),
            PollOpt::edge() | PollOpt::oneshot()).unwrap();
    let chan = event_loop.channel(); // 发送端
    let go = move || {
        let mut i = N;
        sleep_ms(1000);
        let msg = "TEST MSG".to_string();
        while i > 0 {
            chan.send(msg.clone()).unwrap();
            i -= 1;
            if i % 10000 == 0 {
                println!("Enqueued {} msg", N - i);
            }
        }
    };
    let t = thread::spawn(go);
    event_loop.run(&mut Echo::new(srv, sock)).unwrap();
    t.join().unwrap();
}
