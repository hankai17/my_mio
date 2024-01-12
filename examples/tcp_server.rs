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
        /*
        let bytes: &mut [MaybeUninit<u8>] = &mut buf.chunk_mut()[..];
        for b in &mut bytes[..] {
            *b.as_mut_ptr() = 0;
        }
        let res = self.try_read(unsafe { &mut *(bytes as *mut [MaybeUninit<u8>] as *mut [u8]) });
        */

        /*
        let res = self.try_read(unsafe {
            &mut *(&mut buf.chunk_mut()[..] as 
                *mut [MaybeUninit<u8>] as
                *mut [u8])
        });
        */

        let res = self.try_read(unsafe { 
            std::slice::from_raw_parts_mut(buf.chunk_mut().as_mut_ptr(), buf.remaining_mut())
        });

        if let Ok(Some(cnt)) = res {
            unsafe { buf.advance_mut(cnt); }    // len增大
        }
        res 
    }
    fn try_read(&mut self, buf: &mut [u8]) -> io::Result<Option<usize>>;
}

pub trait TryWrite {
    fn try_write_buf<B: Buf>(&mut self, buf: &mut B) -> io::Result<Option<usize>> 
        where Self : Sized {
        let res = self.try_write(buf.chunk());  // 从ptr处取len个字串
        if let Ok(Some(cnt)) = res {
            buf.advance(cnt);   // ptr右移 len缩小 cap缩小
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
    mut_buf: Option<BytesMut>,
    // rw mut_buf TODO
    token: Option<Token>,
    interest: Ready
}

impl Session {
    fn new(sock: TcpStream) -> Session {
        Session {
            sock,
            mut_buf: Some(BytesMut::with_capacity(2048)),
            token: None,
            interest: Ready::empty(),
        }
    }
    fn writable(&mut self, poll: &mut Poll) -> io::Result<()> {
        let mut buf = self.mut_buf.take().unwrap();
        match self.sock.try_write_buf(&mut buf) {
            Ok(None) => {
                println!("client flushing buf; WouldBlock");
                self.mut_buf = Some(buf);
                self.interest.insert(Ready::writable());
            }
            Ok(Some(r)) => {
                println!("Conn: write {} bytes", r);
                self.mut_buf = Some(buf);
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
        let mut buf = self.mut_buf.take().unwrap();
        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                println!("Conn: spurious read wakeup");
                self.mut_buf = Some(buf);
            }
            Ok(Some(r)) => {
                println!("Conn: read {} bytes", r);
                self.mut_buf = Some(buf);
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
        println!("Server accepting socket");
        let sock = self.sock.accept().unwrap().0;
        let conn = Session::new(sock);
        let key = self.conns.insert(conn);
        self.conns[key].token = Some(Token(key));
        poll.register(&self.conns[key].sock, Token(key), Ready::readable(),
                PollOpt::edge() | PollOpt::oneshot())
            .expect("could not register socket with event loop");
        Ok(())
    }
    fn conn_readable(&mut self, poll: &mut Poll, token: Token) -> io::Result<()> {
        println!("server conn readable, token: {:?}", token);
        self.conn(token).readable(poll)
        
    }
    fn conn_writable(&mut self, poll: &mut Poll, token: Token) -> io::Result<()> {
        println!("server conn writable, token: {:?}", token);
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
    println!("listen for connections");
    poll.register(&srv, SERVER, Ready::readable(),
            PollOpt::edge() | PollOpt::oneshot()).unwrap();
    let sock = TcpStream::connect(&addr).unwrap();
    poll.register(&sock, CLIENT, Ready::writable(),
            PollOpt::edge() | PollOpt::oneshot()).unwrap();
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
