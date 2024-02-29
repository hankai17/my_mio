extern crate my_mio;
extern crate slab;
extern crate bytes;

use my_mio::{Events, Poll, PollOpt, Ready, Token};
use my_mio::net::{TcpListener, TcpStream};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use slab::Slab;
use std::io;
use std::mem::MaybeUninit;

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
    fn try_read_buf<B: BufMut>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
            where Self : Sized {
        let bytes = buf.chunk_mut();
        let res = self.try_read(unsafe { 
            std::slice::from_raw_parts_mut(bytes.as_mut_ptr(), bytes.len())
        });

        if let Ok(Some(cnt)) = res {
            unsafe { buf.advance_mut(cnt); }
        }
        res 
    }
    fn try_read(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>>;
}

pub trait TryWrite {
    fn try_write_buf<B: Buf>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
        where Self : Sized {
        let res = self.try_write(buf.chunk());
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

struct Session {
    sock: TcpStream,
    read_buf: Option<BytesMut>,
    write_buf: Option<BytesMut>,
    token: Option<Token>,
    interest: Ready
}

impl Session {
    fn new(sock: TcpStream) -> Session {
        Session {
            sock,
            read_buf: Some(BytesMut::with_capacity(1024)),
            write_buf: Some(BytesMut::with_capacity(1024)),
            token: None,
            interest: Ready::empty(),
        }
    }
    fn writable(&mut self, poll: &mut Poll) -> io::Result<()> {
        let mut buf = self.read_buf.take().unwrap();
        match self.sock.try_write_buf(&mut buf) {
            Ok(None) => {
                println!("client flushing buf; WouldBlock");
                self.read_buf = Some(buf);
                self.interest.insert(Ready::writable());
            }
            Ok(Some(r)) => {
                println!("Conn: write {} bytes", r);
                self.read_buf = Some(buf);
                self.interest.insert(Ready::readable());
                self.interest.remove(Ready::writable());
            }
            Err(e) => {
                println!("not implemented; client err: {:?}", e);
            }
        }
        assert!(self.interest.is_readable() || self.interest.is_writable(),
                "actual: {:?}", self.interest);
        poll.reregister(&self.sock, self.token.unwrap(), self.interest, 
                PollOpt::edge() | PollOpt::oneshot())
    }
    fn readable(&mut self, poll: &mut Poll) -> io::Result<()> {
        let mut buf = self.read_buf.take().unwrap();
        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                println!("Conn: spurious read wakeup");
                self.read_buf = Some(buf);
            }
            Ok(Some(r)) => {
                println!("Conn: read {} bytes, {:?}", r, buf);
                self.read_buf = Some(buf);
                self.interest.remove(Ready::readable());
                self.interest.insert(Ready::writable());
            }
            Err(e) => {
                println!("not implemented client err: {:?}", e);
                self.interest.remove(Ready::readable());
            }
        };
        assert!(self.interest.is_readable() || self.interest.is_writable(),
                "actual: {:?}", self.interest);
        poll.reregister(&self.sock, self.token.unwrap(), self.interest, PollOpt::edge())
    }
}

struct TcpServer {
    sock: TcpListener,
    conns: Slab<Session>
}

impl TcpServer {
    fn accept(&mut self, poll: &mut Poll) -> io::Result<()> {
        let sock = self.sock.accept().unwrap().0;
        let conn = Session::new(sock);
        let key = self.conns.insert(conn);
        self.conns[key].token = Some(Token(key));
        let job = Box::new(move |val: i64| { println!("conn--------------"); });
        poll.register(&self.conns[key].sock, Token(key), Ready::readable(),
                PollOpt::edge() | PollOpt::oneshot(), job)
            .expect("could not register socket with event loop");
        Ok(())
    }
    fn conn_readable(&mut self, poll: &mut Poll, token: Token) -> io::Result<()> {
        //println!("server conn readable, token: {:?}", token);
        self.conn(token).readable(poll)
        
    }
    fn conn_writable(&mut self, poll: &mut Poll, token: Token) -> io::Result<()> {
        //println!("server conn writable, token: {:?}", token);
        self.conn(token).writable(poll)
    }
    fn conn(&mut self, token: Token) -> &mut Session {
        &mut self.conns[token.into()]
    }
}

struct Test {
    server: TcpServer,
}

impl Test {
    fn new(srv: TcpListener) -> Test {
        Test {
            server: TcpServer {
                sock: srv,
                conns: Slab::with_capacity(128)
            }
        }
    }
}

fn main() {
    let mut poll = Poll::new().unwrap();
    //let addr = localhost();
    let addr = "127.0.0.1:9527".parse().unwrap();
    let srv = TcpListener::bind(&addr).unwrap();
    let job = Box::new(move |val: i64| { println!("accept--------------"); });
    poll.register(&srv, SERVER, Ready::readable(),
            PollOpt::edge() | PollOpt::oneshot(), job).unwrap();
    let mut events = Events::with_capacity(1024);

    let mut handler = Test::new(srv);
    while true {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            println!("ready: {:?} {:?}", event.token(), event.readiness());
            if event.readiness().is_readable() {
                match event.token() {
                    SERVER => handler.server.accept(&mut poll).unwrap(),
                    i => handler.server.conn_readable(&mut poll, i).unwrap()
                }
            }
            if event.readiness().is_writable() {
                match event.token() {
                    SERVER => panic!("reveived wirtable for token 0"),
                    i => handler.server.conn_writable(&mut poll, i).unwrap()
                };
            }
        }
    }

}
