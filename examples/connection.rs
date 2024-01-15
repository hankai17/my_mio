extern crate bytes;

use std::fmt;
use my_mio::{Events, Poll, PollOpt, Ready, Token};
use bytes::{Buf, BufMut, Bytes, BytesMut};

struct Acceptor() {
    poller: Poll,
    sock: TcpStream,    // 最好是原始socket + opts
    // accept cb
    listening: bool,
    // idle fd
}

impl Acceptor {
    fn new() -> Acceptor {
    }
    fn listen() {
        // register lfd & handle_read
    }
    fn handle_read() {
    }
}

//buffer + CB(epoll_cb + session_cb)
pub struct Connection {
    token: Option<Token>,
    interest: Ready

    poller: Poll,
    sock: TcpStream,
    // timer
    // conn_cb
    read_buf: Option<BytesMut>,
    write_buf: Option<BytesMut>,
    write_buf_waiting: Option<BytesMut>,

    // accept_cb
    // on_read_cb
    // on_written_cb
    // on_err_cb
    read_enable: bool,
    write_enable: bool,
    read_triggered: bool,
    write_triggered: bool,
    is_closed: bool
}

impl Connection {
    fn new(poll: Poll, sock: TcpStream) -> Connection {
        Connection {
            token: None,
            interest: Ready::empty(),
            poller: poll,
            sock: sock,

            read_buf: Some(BytesMut::with_capacity(1024)),
            write_buf: Some(BytesMut::with_capacity(1024)),
            write_buf_waiting: Some(BytesMut::with_capacity(1024)),

            read_enable: false,
            write_enable: false,
            read_triggered: false,
            write_triggered: true,
            is_closed: false
        }
    }

    fn on_read(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn on_written(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn write_data(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn on_write(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn attach_event(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    fn listen(&mut self, poll: &mut Poll, sock: TcpStream) -> io::Result<()> {
    }
    //fn listen(port, ip, backlog)
    fn on_accept(&mut self, poll: &mut Poll, event: Events) {
        while true {
            if event.readiness().is_readable() {
                // token check
                let (s, err) = sock.accept().unwrap();
                if err {
                    break;
                }
                // set non block and etc
                // m_on_brefore_accept_cb
                // m_on_accept_cb
            }
        }
    }
    fn on_connected(&mut self, poll: &mut Poll, sock: TcpStream, // err_cb) {
    }
    //fn connect()
    //fn connect_l()
    fn send_l(bytes: ByteMut) -> io::Result<usize> {
    }
    //fn send() 
    fn set_on_before_accept_cb() {
    }
    fn set_on_accept_cb() {
    }
    fn set_on_read_cb() {
    }
    fn set_on_written_cb() {
    }
    fn set_on_error_cb() {
    }
    fn clone_stream() {
    }
    
}

