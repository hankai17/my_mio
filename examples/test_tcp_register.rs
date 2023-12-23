extern crate my_mio;
extern crate bytes;
extern crate log;
extern crate env_logger;

use my_mio::{Events, Poll, PollOpt, Ready, Token};
use my_mio::event::Event;
use my_mio::net::{TcpListener, TcpStream};
use bytes::SliceBuf;
use std::time::Duration;

const SERVER: Token = Token(0);
const CLIENT: Token = Token(1);

use bytes::{Buf, MutBuf};
use std::io::{self, Read, Write};
trait MapNonBlock<T> {
    fn map_non_block(self) -> io::Result<Option<T>>;
}

impl<T> MapNonBlock<T> for io::Result<T> {
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

mod ports {
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
    use std::sync::atomic::Ordering::SeqCst;

    static mut NEXT_PORT: AtomicUsize = ATOMIC_USIZE_INIT;
    const FIRST_PORT: usize = 18080;

    fn next_port() -> usize {
        unsafe {
            NEXT_PORT.compare_and_swap(0, FIRST_PORT, SeqCst);
            NEXT_PORT.fetch_add(1, SeqCst)
        }
    }
    pub fn localhost() -> SocketAddr {
        let s = format!("127.0.0.1:{}", next_port());
        FromStr::from_str(&s).unwrap()
    }
}

fn expect_events(poll: &Poll, event_buffer: &mut Events, poll_try_count: usize, mut expected: Vec<Event>) {
    const MS: u64 = 1_000;
    for _ in 0..poll_try_count {
        poll.poll(event_buffer, Some(Duration::from_millis(MS))).unwrap();
        for event in event_buffer.iter() {
            let pos_opt = match expected.iter().position(|exp_event| {
                (event.token() == exp_event.token()) &&
                event.readiness().contains(exp_event.readiness())
            }) {
                Some(x) => Some(x),
                None => None,
            };
            if let Some(pos) = pos_opt { expected.remove(pos); }
        }
        if expected.is_empty() {
            break;
        }
    }
    assert!(expected.is_empty(), "The following expected events were not found: {:?}", expected);
}

struct TestHandler {
    server: TcpListener,
    client: TcpStream,
    state: usize,
}

impl TestHandler {
    fn new(srv: TcpListener, cli: TcpStream) -> TestHandler {
        TestHandler {
            server: srv,
            client: cli,
            state: 0,
        }
    }
    fn handle_read(&mut self, poll: &mut Poll, token: Token) {
        match token {
            SERVER => {
                println!("handle_read: token = SERVER");
                let mut sock = self.server.accept().unwrap().0;
                sock.try_write_buf(&mut SliceBuf::wrap(b"foobar")).unwrap();
            }
            CLIENT => {
                println!("handle_read: token = CLIENT");
                assert!(self.state == 0, "unexpected stat {}", self.state);
                self.state = 1;
                poll.reregister(&self.client, CLIENT, Ready::writable(), PollOpt::level()).unwrap();
            }
            _ => panic!("unexpected token"),

        }
    }
    fn handle_write(&mut self, poll: &mut Poll, token: Token) {
        println!("handle_write: token: {:?}, state: {:?}", token, self.state);
        assert!(token == CLIENT, "unexpected token: {:?}", token);
        assert!(self.state == 1, "unexpected state {}", self.state);
        self.state = 2;
        poll.deregister(&self.client).unwrap();
        poll.deregister(&self.server).unwrap();
    }
}

use ports::localhost;

fn test_register_deregister() {
    let _ = ::env_logger::init();
    println!("staring...");
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let addr = localhost();

    let server = TcpListener::bind(&addr).unwrap();
    println!("register server socket");
    poll.register(&server, SERVER, Ready::readable(), PollOpt::edge()).unwrap();

    let client = TcpStream::connect(&addr).unwrap();
    poll.register(&client, CLIENT, Ready::readable(), PollOpt::level()).unwrap();

    let mut handler = TestHandler::new(server, client);
    loop {
        poll.poll(&mut events, None).unwrap();
        if let Some(event) = events.get(0) {
            if event.readiness().is_readable() {
                handler.handle_read(&mut poll, event.token());
            }
            if event.readiness().is_writable() {
                handler.handle_write(&mut poll, event.token());
                break;
            }
        }
    }
    poll.poll(&mut events, Some(Duration::from_millis(1000))).unwrap();
    assert_eq!(events.len(), 0);
}

fn test_register_empty_interest() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let addr = localhost();

    let sock = TcpListener::bind(&addr).unwrap();
    poll.register(&sock, Token(0), Ready::empty(), PollOpt::edge()).unwrap();

    let client = TcpStream::connect(&addr).unwrap();
    poll.register(&client, Token(1), Ready::empty(), PollOpt::edge()).unwrap();

    poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
    assert_eq!(events.len(), 0, "Received unexpected event: {:?}", events.get(0).unwrap());

    poll.reregister(&sock, Token(0), Ready::readable(), PollOpt::edge()).unwrap();
    expect_events(&poll, &mut events, 100, vec![
        Event::new(Ready::readable(), Token(0))
    ]);
    poll.reregister(&sock, Token(0), Ready::empty(), PollOpt::edge()).unwrap();
}

use std::io::ErrorKind;
fn test_tcp_register_multiple_event_loops() {
    let addr = localhost();
    let listener = TcpListener::bind(&addr).unwrap();

    let poll1 = Poll::new().unwrap();
    let poll2 = Poll::new().unwrap();

    poll1.register(&listener, Token(0), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    let res = poll2.register(&listener, Token(0), Ready::readable() | Ready::writable(), PollOpt::edge());
    if res.is_err() {
        println!("is error");
    }
    //assert!(res.is_err());
    //assert_eq!(res.unwrap_err().kind(), ErrorKind::Other);

    let listener2 = listener.try_clone().unwrap();
    let res = poll2.register(&listener2, Token(0), Ready::readable() | Ready::writable(), PollOpt::edge());
    if res.is_err() {
        println!("is error");
    }
    //assert!(res.is_err());
    //assert_eq!(res.unwrap_err().kind(), ErrorKind::Other);

    let stream = TcpStream::connect(&addr).unwrap();
    poll1.register(&stream, Token(1), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    let res = poll2.register(&stream, Token(1), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    //assert!(res.is_err());
    //assert_eq!(res.unwrap_err().kind(), ErrorKind::Other);

    let stream2 = stream.try_clone().unwrap();
    let res = poll2.register(&stream2, Token(1), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    //assert!(res.is_err());
    //assert_eq!(res.unwrap_err().kind(), ErrorKind::Other);

}

fn main() {
    //test_register_deregister();
    //test_register_empty_interest();
    test_tcp_register_multiple_event_loops();
}

