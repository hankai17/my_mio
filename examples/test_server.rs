extern crate my_mio;
extern crate slab;
extern crate bytes;

use my_mio::{Events, Poll, PollOpt, Ready, Token};
use my_mio::net::{TcpListener, TcpStream};
use bytes::{Buf, ByteBuf, MutBuf, MutByteBuf, SliceBuf};
use slab::Slab;
use std::io;

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

struct EchoConn {
    sock: TcpStream,
    buf: Option<ByteBuf>,
    mut_buf: Option<MutByteBuf>,
    token: Option<Token>,
    interest: Ready
}

impl EchoConn {
    fn new(sock: TcpStream) -> EchoConn {
        EchoConn {
            sock,
            buf: None,
            mut_buf: Some(ByteBuf::mut_with_capacity(2048)),
            token: None,
            interest: Ready::empty(),
        }
    }
    fn writable(&mut self, poll: &mut Poll) -> io::Result<()> {
        let mut buf = self.buf.take().unwrap();
        match self.sock.try_write_buf(&mut buf) {
            Ok(None) => {
                println!("client flushing buf; WouldBlock");
                self.buf = Some(buf);
                self.interest.insert(Ready::writable());
            }
            Ok(Some(r)) => {
                println!("Conn: write {} bytes", r);
                self.mut_buf = Some(buf.flip());
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
                self.buf = Some(buf.flip());
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

struct EchoServer {
    sock: TcpListener,
    conns: Slab<EchoConn>
}

impl EchoServer {
    fn accept(&mut self, poll: &mut Poll) -> io::Result<()> {
        println!("Server accepting socket");
        let sock = self.sock.accept().unwrap().0;
        let conn = EchoConn::new(sock);
        let tok = self.conns.insert(conn);
        self.conns[tok].token = Some(Token(tok));
        poll.register(&self.conns[tok].sock, Token(tok), Ready::readable(),
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
    fn conn(&mut self, token: Token) -> &mut EchoConn {
        &mut self.conns[token.into()]
    }
}

struct EchoClient {
    sock: TcpStream,
    msg: Vec<&'static str>,
    tx: SliceBuf<'static>,
    rx: SliceBuf<'static>,
    mut_buf: Option<MutByteBuf>,
    token: Token,
    interest: Ready,
    shutdown: bool,
}

impl EchoClient {
    fn new(sock: TcpStream, token: Token, mut msg: Vec<&'static str>) -> EchoClient {
        let curr = msg.remove(0);
        EchoClient {
            sock,
            msg,
            tx: SliceBuf::wrap(curr.as_bytes()),    // as_bytes(&self) -> &[u8]  // wrap(bytes: &'a [u8]) -> SliceBuf<'a>
            rx: SliceBuf::wrap(curr.as_bytes()),
            mut_buf: Some(ByteBuf::mut_with_capacity(2048)),
            token,
            interest: Ready::empty(),
            shutdown: false,
        }
    }
    fn readable(&mut self, poll: &mut Poll) -> io::Result<()> {
        println!("client socket readable");
        let mut buf = self.mut_buf.take().unwrap(); // take(&mut self) -> Option<T> // Takes the value out of the option, leaving a None in its place.
        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                println!("Client spurious read wakeup");
                self.mut_buf = Some(buf);
            }
            Ok(Some(r)) => {
                println!("Client read {} bytes", r);
                let mut buf = buf.flip();
                while buf.has_remaining() {
                    let actual = buf.read_byte().unwrap();
                    let expect = self.rx.read_byte().unwrap();
                    assert!(actual == expect, "actual: {}, expect: {}", actual, expect);
                }
                self.mut_buf = Some(buf.flip());
                self.interest.remove(Ready::readable());
                if !self.rx.has_remaining() {
                    self.next_msg(poll).unwrap();
                }
            }
            Err(e) => {
                panic!("not implemented; client err: {:?}", e);
            }
        };
        if !self.interest.is_empty() {
            assert!(self.interest.is_readable() || self.interest.is_writable(), 
                    "actual: {:?}", self.interest);
            poll.reregister(&self.sock, self.token, self.interest, PollOpt::edge() | PollOpt::oneshot())?;
        }
        Ok(())
    }
    fn writable(&mut self, poll: &mut Poll) -> io::Result<()> {
        println!("client socket writable");
        match self.sock.try_write_buf(&mut self.tx) {
            Ok(None) => {
                println!("client flushing buf WouldBlock");
                self.interest.insert(Ready::writable());
            }
            Ok(Some(r)) => {
                println!("clinet write {} bytes", r);
                self.interest.insert(Ready::readable());
                self.interest.remove(Ready::writable());
            }
            Err(e) => println!("not implemented; client err: {:?}", e)
        }
        if self.interest.is_readable() || self.interest.is_writable() {
            try!(poll.reregister(&self.sock, self.token, self.interest,
                    PollOpt::edge() | PollOpt::oneshot()));
        } Ok(())
    }
    fn next_msg(&mut self, poll: &mut Poll) -> io::Result<()> {
        if self.msg.is_empty() {
            self.shutdown = true;
            return Ok(());
        }
        let curr = self.msg.remove(0);
        println!("client prepping next msg");
        self.tx = SliceBuf::wrap(curr.as_bytes());
        self.rx = SliceBuf::wrap(curr.as_bytes());
        self.interest.insert(Ready::writable());
        poll.reregister(&self.sock, self.token, self.interest,
                PollOpt::edge() | PollOpt::oneshot())
    }
}

struct Echo {
    server: EchoServer,
    client: EchoClient,
}

impl Echo {
    fn new(srv: TcpListener, client: TcpStream, msg: Vec<&'static str>) -> Echo {
        Echo {
            server: EchoServer {
                sock: srv,
                conns: Slab::with_capacity(128)
            },
            client: EchoClient::new(client, CLIENT, msg)
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

    let mut handler = Echo::new(srv, sock, vec!["foo", "bar"]);
    while !handler.client.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            println!("ready: {:?} {:?}", event.token(), event.readiness());
            if event.readiness().is_readable() {
                match event.token() {
                    SERVER => handler.server.accept(&mut poll).unwrap(),
                    CLIENT => handler.client.readable(&mut poll).unwrap(),
                    i => handler.server.conn_readable(&mut poll, i).unwrap()
                }
            }
            if event.readiness().is_writable() {
                match event.token() {
                    SERVER => panic!("reveived wirtable for token 0"),
                    CLIENT => handler.client.writable(&mut poll).unwrap(),
                    i => handler.server.conn_writable(&mut poll, i).unwrap()
                };
            }
        }
    }

}
