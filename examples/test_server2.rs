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

const SERVER: Token = Token(0);
const CLIENT: Token = Token(1);

#[derive(PartialEq, Debug)]
enum TestState {
    Initial,
    AfterRead,
}

use TestState::{Initial, AfterRead};

struct TestHandler {
    srv: TcpListener,
    cli: TcpStream,
    state: TestState,
    shutdown: bool,
}

impl TestHandler {
    fn new(srv: TcpListener, cli: TcpStream) -> TestHandler {
        TestHandler {
            srv,
            cli,
            state: Initial,
            shutdown: false,
        }
    }
    fn handle_read(&mut self, poll: &mut Poll, token: Token, events: Ready) {
        println!("readable, token: {:?}, hint: {:?}", token, events);
        match token {
            SERVER => {
                println!("server conn ready for accept");
                let _ = self.srv.accept().unwrap();
            }
            CLIENT => {
                println!("client readable");
                match self.state {
                    Initial => {
                        let mut buf = [0; 4096];
                        //println!("GOT: {:?}", self.cli.try_read(&mut buf[..]));
                        println!("GOT: {:?}", self.cli.try_read(&mut buf));
                        self.state = AfterRead;
                    },
                    AfterRead => {}
                }
                let mut buf = ByteBuf::mut_with_capacity(1024);
                match self.cli.try_read_buf(&mut buf) {
                    Ok(Some(0)) => self.shutdown = true,
                    Ok(_) => panic!("the cleint socket should not be readable"),
                    Err(e) => panic!("Unexpected err {:?}", e)
                }
            }
            _ => panic!("received unknown token {:?}", token)
        }
        poll.reregister(&self.cli, CLIENT, Ready::readable(), PollOpt::edge()).unwrap();
    }
    fn handle_write(&mut self, poll: &mut Poll, token: Token, _: Ready) {
        match token {
            SERVER => panic!("received writable for token 0"),
            CLIENT => {
                println!("client connected") ;
                poll.reregister(&self.cli, CLIENT, Ready::readable(), PollOpt::edge()).unwrap();
            }
            _ => panic!("received unknown token {:?}", token)
        }
    }
}

fn main() {
    let mut poll = Poll::new().unwrap();
    let addr = "127.0.0.1:9527".parse().unwrap();
    let srv = TcpListener::bind(&addr).unwrap();
    poll.register(&srv, SERVER, Ready::readable(), PollOpt::edge()).unwrap();
    let sock = TcpStream::connect(&addr).unwrap();
    poll.register(&sock, CLIENT, Ready::writable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(1024);
    let mut handler = TestHandler::new(srv, sock);
    while !handler.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            if event.readiness().is_readable() {
                handler.handle_read(&mut poll, event.token(), event.readiness());
            }
            if event.readiness().is_writable() {
                handler.handle_write(&mut poll, event.token(), event.readiness());
            }
        }
    }
    assert!(handler.state == AfterRead, "actual: {:?}", handler.state);
}

