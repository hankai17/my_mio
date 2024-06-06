use std::{io};
use net::{TryRead, TryWrite};
use bytes::{BufMut, BytesMut};
use event_imp::{ready_from_usize};
use net::{EventLoop, TcpStream};
use std::sync::{Arc, Mutex};
use log::{debug, info, warn, error};

unsafe impl Send for TcpConnection {}
unsafe impl Sync for TcpConnection {}

/*
macro_rules! pub_struct {
    ($name:ident {$($field:ident: $t:ty,)*}) => {
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            $(pub $field: $t),*
        }
    }
}
*/

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum StateE {
#[warn(non_camel_case_types)]
    Disconnected,
    Connecting,
    Connected,
    Disconnecting,
}

pub type ReadJob = Box<dyn FnMut(&mut BytesMut) + 'static + Send + Sync>;
pub type WritJob = Box<dyn FnMut()->bool + 'static + Send + Sync>;

pub struct TcpConnection {
    event_loop: Arc<EventLoop>,
    pub tcp_stream: TcpStream,
    // timer
    read_buffer: Option<BytesMut>,
    write_buffer_sending: Option<BytesMut>,
    write_buffer_waiting: Option<BytesMut>,

    read_job: ReadJob,
    writ_job: WritJob,

    state: StateE,
    read_enabled: bool,
    write_enabled: bool,
    read_triggered: bool,
    write_triggered: bool,
}

impl TcpConnection {
    pub fn new(event_loop: Arc<EventLoop>, tcp_stream: TcpStream)
            -> TcpConnection {
        TcpConnection {
            event_loop: event_loop,
            tcp_stream: tcp_stream,

            read_buffer: Some(BytesMut::with_capacity(1024)),
            write_buffer_sending: Some(BytesMut::with_capacity(1024)),
            write_buffer_waiting: Some(BytesMut::with_capacity(1024)),

            read_job: Box::new(move |_| { debug!("default read job"); }),
            writ_job: Box::new(move || { debug!("default write job"); true }),

            state: StateE::Connecting,
            read_enabled: true,
            write_enabled: true,
            read_triggered: false,
            write_triggered: false,
        }
    }

    pub fn connected(&self) -> bool {
        self.state == StateE::Connected
    }

    pub fn disconnected(&self) -> bool {
        self.state == StateE::DisConnected
    }

    pub fn set_state(&mut self, state: StateE) {
        self.state = state;
    }

    pub fn set_read_job(&mut self, job: ReadJob) {
        self.read_job = job;
    }

    pub fn set_writ_job(&mut self, job: WritJob) {
        self.writ_job = job;
    }

    pub fn set_enabled(&mut self, enable: bool) {
        self.read_enabled = enable;
        self.write_enabled = enable;
    }

    fn handle_read(&mut self) -> io::Result<()> {
        loop {
            let mut buf = self.read_buffer.take().unwrap();
            match self.tcp_stream.try_read_buf(&mut buf) {
                Ok(None) => {
                    warn!("Conn: spurious read wakeup");
                    self.read_buffer = Some(buf);
                    self.read_triggered = false; 
                    break;
                }
                Ok(Some(r)) => {
                    debug!("Conn: read {} bytes, {:?}", r, buf);
                    if r > 0 {
                        (self.read_job)(&mut buf);
                        self.read_buffer = Some(buf);
                    } else {
                        (self.read_job)(&mut buf);
                        self.read_buffer = Some(buf);
                        self.read_triggered = false; 
                        self.close_stream();
                        break;
                    }
                }
                Err(e) => {
                    warn!("not implemented client err: {:?}", e);
                    self.read_triggered = false; 
                    self.close_stream();
                    break;
                }
            };
        }
        Ok(())
    }

    fn write_data(&mut self) -> io::Result<()> {
        if self.state != Connected {
            error!("state not connected");
            return Ok(());
        }
        let mut buf_tmp = Some(BytesMut::with_capacity(1024)).unwrap();
        let mut buf_snd = self.write_buffer_sending.take().unwrap();
        if buf_snd.len() > 0 {
            buf_tmp = buf_snd.split();
        }
        if buf_tmp.len() == 0 {
            loop {
                let mut buf = self.write_buffer_waiting.take().unwrap();
                if buf.len() > 0 {
                    buf_tmp = buf.split();
                    self.write_buffer_waiting = Some(buf);
                    break;
                }
                // onWritten() // all data consumed done
                self.write_buffer_waiting = Some(buf);
                self.write_buffer_sending = Some(buf_snd);
                return Ok(())
            }
        }

        let mut buf = buf_tmp;
        //debug!("Conn {:?}: write1 ", self.tcp_stream);
        match self.tcp_stream.try_write_buf(&mut buf) {
            Ok(None) => {
                debug!("client flushing buf; WouldBlock");
                self.write_buffer_sending = Some(buf.split());
                self.write_triggered = false;
            }
            Ok(Some(_r)) => {
                //debug!("Conn {:?}: write2 {} bytes", self.tcp_stream, r);
                if buf.len() > 0 {
                    self.write_buffer_sending = Some(buf.split());
                    return Ok(());
                }
                self.write_triggered = false;
                let ret = (self.writ_job)();
                self.write_buffer_sending = Some(buf.split());
                if ret == false {
                    //debug!("close stream1");
                    self.close_stream();
                }
            }
            Err(e) => {
                debug!("not implemented; client err: {:?}", e);
                self.write_triggered = false;
                //self.close_stream();
            }
        }

        Ok(())
    }

    pub fn send(&mut self, bytes: BytesMut) -> io::Result<usize> {
        if self.state != Connected {
            return Ok(0);
        }
        // if write triggered TODO
        let len = bytes.len();
        if len == 0 {
            return Ok(0);
        }
        let mut buffer = self.write_buffer_waiting.take().unwrap();
        buffer.put(bytes);
        self.write_buffer_waiting = Some(buffer);
        self.write_data().unwrap();
        return Ok(len);
    }

    fn handle_write(&mut self) -> io::Result<()> {
        let mut empty_waiting: bool = false;
        let mut empty_sending: bool = false;
        if self.write_buffer_waiting.as_ref().unwrap().len() == 0 {
            empty_waiting = true;
        }
        if self.write_buffer_sending.as_ref().unwrap().len() == 0 {
            empty_sending = true;
        }
        if empty_waiting && empty_sending {
            // disable write
            //debug!("handle_write disable write TODO");
            // do nothing ?
        } else {
            self.write_data().unwrap();
        }
        return Ok(())
    }

    fn handle_close(&mut self) -> io::Result<()> {
        debug_assert(self.state == StateE::Connected ||
                self.state == StateE::Disconnecting);
        self.set_state(StateE::Disconnected);
        self.set_enabled(false);
        
        // disable channel 是否意味着 disable triggered 
        // close
        Ok(())
    }

    pub fn handle_event(&mut self, event: i64) -> io::Result<()> {
        // check closed
        let ready = ready_from_usize(event as usize);
        if ready.is_readable() {
            self.read_triggered = true; 
            self.handle_read().unwrap();
        }
        if ready.is_writable() {
            self.write_triggered = true; 
           self.handle_write().unwrap();
        }
        if ready.is_error() ||
                ready.is_hup() {
            self.read_triggered = true; 
            self.write_triggered = true; 
            self.handle_close().unwrap();
        }
        Ok(())
    }

    pub fn close_stream(&mut self) {
        self.event_loop.deregister(&self.tcp_stream).unwrap();
        debug!("close_stream {:?} deregister done", self.tcp_stream);
    }
}

impl Drop for TcpConnection {
    fn drop(&mut self) {
        // assert(self.state == StateE::Disconnected);
        debug!("drop for tcpconnection {:?}", self.tcp_stream)
    }
}

