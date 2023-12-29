extern crate my_mio;
extern crate bytes;

use my_mio::{Events, Poll, PollOpt, Ready, Token};
use my_mio::net::{TcpListener, TcpStream};
use bytes::{Buf, ByteBuf, MutByteBuf, SliceBuf};
use slab::Slab;
use std::io;

const SERVER: Token = Token(10_000_000);
const CLIENT: Token = Token(10_000_001);

struct EchoConn {
    sock: TcpStream,
    buf: Option<ByteBuf>,
    mut_buf: Option<MutByteBuf>,
    token: Option<Token>,
    interest: Ready
}

imp EchoConn {
    fn new(sock: TcpStream) -> EchoConn {
    }
    fn writable(&mut self, poll: &mut Poll) -> io::Result<()> {
    }
    fn readable(&mut self, poll: &mut Poll) -> io::Result<()> {
    }
}

struct EchoServer {
    sock: TcpListener,
    conns: Slab<EchoConn>
}

impl EchoServer {
    fn accept(&mut self, poll: &mut Poll) -> io::Result<()> {
    }
    fn conn_readable(&mut self, poll: &mut Poll, token: Token) -> io::Result<()> {
    }
    fn conn_writable(&mut self, poll: &mut Poll, token: Token) -> io::Result<()> {
    }
    fn conn(&mut self, token: Token) -> &mut EchoConn {
        *mut self.conns[token.into()]
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
    }
    fn readable(&mut self, poll: &mut Poll) -> io::Result<()> {
    }
    fn writable(&mut self, poll: &mut Poll) -> io::Result<()> {
    }
    fn next_msg(&mut self, poll: &mut Poll) -> io::Result<()> {
    }
}

struct Echo {
    server: EchoServer,
    client: EchoClient,
}

impl Echo {
    fn new(srv: TcpListener, client: TcpStream, msg: Vec<&'static str>) -> Echo {
    }
}
