extern crate my_mio;
extern crate bytes;
extern crate iovec;

use std::cmp;
use std::io;
use std::thread;
use std::collections::HashMap;
use std::net::{self, Shutdown};
use std::time::{Duration, Instant};
use std::sync::mpsc::channel;
use iovec::IoVec;

use my_mio::{Token, Ready, PollOpt, Poll, Events};
use my_mio::event::{Evented, Event};
use my_mio::net::{TcpListener, TcpStream};


use bytes::{Buf, MutBuf};
use std::io::{Read, Write};
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

fn accept() {
    struct H {
        hit: bool,
        listener: TcpListener,
        shutdown: bool,
    }

    let l = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = l.local_addr().unwrap();
    let t = thread::spawn(move || {
        net::TcpStream::connect(&addr).unwrap();
    });

    let poll = Poll::new().unwrap();
    poll.register(&l, Token(1), Ready::readable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(128);
    let mut h = H {
        hit: false,
        listener: l,
        shutdown: false
    };
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            h.hit = true;
            assert_eq!(event.token(), Token(1));
            assert!(event.readiness().is_readable());
            assert!(h.listener.accept().is_ok());
            h.shutdown = true;
        }
    }
    assert!(h.hit);
    assert!(h.listener.accept().unwrap_err().kind() == io::ErrorKind::WouldBlock);
    t.join().unwrap();
}

fn connect() {
    struct H {
        hit: u32,
        shutdown: bool
    }
    let l = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let (tx, rx) = channel();
    let (tx2, rx2) = channel();
    let t = thread::spawn(move || {
        let s = l.accept().unwrap();    // 2
        rx.recv().unwrap();             // 3.1
        drop(s);                        // 3.2
        tx2.send(()).unwrap();          // 3.3
    });

    let poll = Poll::new().unwrap();
    let s = TcpStream::connect(&addr).unwrap(); // 1
    poll.register(&s, Token(1), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(128);

    let mut h = H {
        hit: 0,
        shutdown: false,
    };
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            assert_eq!(event.token(), Token(1));
            match h.hit {
                0 => assert!(event.readiness().is_writable()), // 2.1
                1 => assert!(event.readiness().is_readable()),
                _ => panic!(),
            }
            h.hit += 1;
            h.shutdown = true;
        }
    }
    assert_eq!(h.hit, 1);   // 2.2

    tx.send(()).unwrap();   // 3
    rx2.recv().unwrap();    // 3.4

    h.shutdown = false;
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            assert_eq!(event.token(), Token(1));
            match h.hit {
                0 => assert!(event.readiness().is_writable()),
                1 => assert!(event.readiness().is_readable()), // 3.5
                _ => panic!(),
            }
            h.hit += 1;
            h.shutdown = true;
        }
    }
    assert_eq!(h.hit, 2);
    t.join().unwrap();
}

fn read() {
    const N: usize = 16 * 1024 * 1024;
    struct H {
        amt: usize,
        socket: TcpStream,
        shutdown: bool
    }
    let l = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = thread::spawn(move || {
        let mut s = l.accept().unwrap().0;
        let b = [0; 1024];
        let mut amt = 0;
        while amt < N {
            amt += s.write(&b).unwrap();
        }
    });
    let poll = Poll::new().unwrap();
    let s = TcpStream::connect(&addr).unwrap();
    poll.register(&s, Token(1), Ready::readable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(128);
    let mut h = H {
        amt: 0,
        socket: s,
        shutdown: false
    };
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            assert_eq!(event.token(), Token(1));
            let mut b = [0; 1024];
            loop {
                if let Some(amt) = h.socket.try_read(&mut b).unwrap() {
                    h.amt += amt;
                } else {
                    break;
                }
                if h.amt >= N {
                    h.shutdown = true;
                    break
                }
            }
        }
    }
    t.join().unwrap();
}

fn peek() {
    const N: usize = 16 * 1024 * 1024;
    struct H {
        amt: usize,
        socket: TcpStream,    
        shutdown: bool
    }
    let l = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = thread::spawn(move || {
        let mut s = l.accept().unwrap().0;
        let b = [0; 1024];
        let mut amt = 0;
        while amt < N {
            amt += s.write(&b).unwrap();
        }
    });
    let poll = Poll::new().unwrap();
    let s = TcpStream::connect(&addr).unwrap();
    poll.register(&s, Token(1), Ready::readable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(128);
    let mut h = H {
        amt: 0,
        socket: s,
        shutdown: false
    };
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            assert_eq!(event.token(), Token(1));
            let mut b = [0; 1024];
            match h.socket.peek(&mut b) {
                Ok(_) => (),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue
                },
                Err(e) => panic!("unexpected error: {:?}", e),
            }
            loop {
                if let Some(amt) = h.socket.try_read(&mut b).unwrap() {
                    h.amt += amt;
                } else {
                    break;
                }
                if h.amt >= N {
                    h.shutdown = true;
                    break
                }
            }
        }
    }
    t.join().unwrap();
}

fn read_bufs() {
    const N: usize = 16 * 1024 * 1024;
    let l = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = thread::spawn(move || {
        let mut s = l.accept().unwrap().0;
        let b = [1; 1024];
        let mut amt = 0;
        while amt < N {
            amt += s.write(&b).unwrap();
        }
    });
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);
    let s = TcpStream::connect(&addr).unwrap();
    poll.register(&s, Token(1), Ready::readable(), PollOpt::level()).unwrap();
    let b1 = &mut [0; 10][..];
    let b2 = &mut [0; 383][..];
    let b3 = &mut [0; 28][..];
    let b4 = &mut [0; 8][..];
    let b5 = &mut [0; 128][..];
    let mut b: [&mut IoVec; 5] = [
        b1.into(),
        b2.into(),
        b3.into(),
        b4.into(),
        b5.into(),
    ];
    let mut so_far = 0;
    loop {
        for buf in b.iter_mut() {
            for byte in buf.as_mut_bytes() {
                *byte = 0;
            }
        }
        poll.poll(&mut events, None).unwrap();
        match s.read_bufs(&mut b) {
            Ok(0) => {
                assert_eq!(so_far, N);
                break;
            }
            Ok(mut n) => {
                so_far += n;
                for buf in b.iter() {
                    let buf = buf.as_bytes();
                    for byte in buf[..cmp::min(n, buf.len())].iter() {
                        assert_eq!(*byte, 1);
                    }
                    n = n.saturating_sub(buf.len());
                    if n == 0 {
                        break;
                    }
                }
                assert_eq!(n, 0);
            }
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::WouldBlock),
        }
    }
    t.join().unwrap();
}

fn write() {
    const N: usize = 16 * 1024 * 1024;
    struct H {
        amt: usize,
        socket: TcpStream,
        shutdown: bool
    };
    let l = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = thread::spawn(move || {
        let mut s = l.accept().unwrap().0;
        let mut b = [0; 1024];
        let mut amt = 0;
        while amt < N {
            amt += s.read(&mut b).unwrap();
        }
    });

    let poll = Poll::new().unwrap();
    let s = TcpStream::connect(&addr).unwrap();
    poll.register(&s, Token(1), Ready::writable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(128);
    let mut h = H {
        amt: 0,
        socket: s,
        shutdown: false
    };
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            assert_eq!(event.token(), Token(1));
            let b = [0; 1024];
            loop {
                if let Some(amt) = h.socket.try_write(&b).unwrap() {
                    h.amt += amt;
                } else {
                    break;
                }
                if h.amt >= N {
                    h.shutdown = true;
                    break
                }
            }
        }
    }
    t.join().unwrap();
}

fn write_bufs() {
    const N: usize = 16 * 1024 * 1024;
    let l = net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let t = thread::spawn(move || {
        let mut s = l.accept().unwrap().0;
        let mut b = [0; 1024];
        let mut amt = 0;
        while amt < N {
            for byte in b.iter_mut() {
                *byte = 0;
            }
            let n = s.read(&mut b).unwrap();
            amt += n;
            for byte in b[..n].iter() {
                assert_eq!(*byte, 1);
            }
        }
    });

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);
    let s = TcpStream::connect(&addr).unwrap();
    poll.register(&s, Token(1), Ready::writable(), PollOpt::level()).unwrap();
    let b1 = &[1; 10][..];
    let b2 = &[1; 383][..];
    let b3 = &[1; 28][..];
    let b4 = &[1; 8][..];
    let b5 = &[1; 128][..];
    let b: [&IoVec; 5] = [
        b1.into(),
        b2.into(),
        b3.into(),
        b4.into(),
        b5.into(),
    ];

    let mut so_far = 0;
    while so_far < N {
        poll.poll(&mut events, None).unwrap();
        match s.write_bufs(&b) {
            Ok(n) => so_far += n,
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::WouldBlock),
        }
    }
    t.join().unwrap();
}

fn connect_then_close() {
    struct H {
        listener: TcpListener,
        shutdown: bool
    }
    let poll = Poll::new().unwrap();
    let l = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let s = TcpStream::connect(&l.local_addr().unwrap()).unwrap();
    poll.register(&l, Token(1), Ready::readable(), PollOpt::edge()).unwrap();
    poll.register(&s, Token(2), Ready::readable(), PollOpt::edge()).unwrap();
    let mut events = Events::with_capacity(128);
    let mut h = H {
        listener: l,
        shutdown: false
    };
    while !h.shutdown {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            if event.token() == Token(1) {
                let s = h.listener.accept().unwrap().0;
                poll.register(&s, Token(3), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
                drop(s);
            } else if event.token() == Token(2) {
                h.shutdown = true;
            }
        }
    }
}

fn main() {
    accept();
    connect();
    read();
    read_bufs();
    write();
    write_bufs();
    connect_then_close();
}

