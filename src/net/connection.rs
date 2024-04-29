use std::{io};
use net::{TryRead, TryWrite};
use bytes::{BufMut, BytesMut};
use {Ready, Token};
use event_imp::{ready_from_usize};
use net::{EventLoop, TcpStream};
use std::sync::{Arc, Mutex};

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

pub type ReadJob = Box<dyn FnMut(&mut BytesMut) + 'static + Send + Sync>;
pub type WritJob = Box<dyn FnMut()->bool + 'static + Send + Sync>;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct VIO {
    //buffer,
    //mutex,
    read_job: ReadJob,
    write_job: WritJob,
    nbytes: i64,
    ndone: i64,
    op: i64,
}

impl VIO {
    pub fn new() -> VIO {
        VIO {
            read_job: Box::new(move |bytes: &mut BytesMut| { println!("default read job"); }),
            writ_job: Box::new(move || { println!("default write job"); true }),
            nbytes: 0,
            ndone: 0,
            op: 0,
        }
    }
    pub fn set_read_job(&mut self, job: ReadJob) {
        self.read_job = job;
    }
    pub fn set_writ_job(&mut self, job: WritJob) {
        self.writ_job = job;
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct NetState {
    enabled: bool,
    triggered: bool,
    vio: Arc<Mutex<VIO>>,
}

impl NetStat {
    pub fn new() -> NetStat {
        NetStat {
            enabled: false,
            triggered: false,
            vio: Arc::new(Mutex::new(VIO::new())),
        }
    }
}

pub struct TcpConnection {
    token: Option<Token>,
    interest: Ready,

    event_loop: Arc<Mutex<EventLoop>>,
    pub sock: TcpStream,
    // timer
    read_buffer: Option<BytesMut>,
    write_buffer_sending: Option<BytesMut>,
    write_buffer_waiting: Option<BytesMut>,

    read_cb: fn(&mut BytesMut),
    write_cb: fn() -> bool,
    error_cb: fn(),
    read_job: ReadJob,
    writ_job: WritJob,

    read: NetState,
    write: NetState,

    is_closed: bool
}

fn default_read_cb(bytes: &mut BytesMut) {}
fn default_written_cb() -> bool { false }
fn default_err_cb() {}

impl TcpConnection {
    pub fn new(event_loop: Arc<Mutex<EventLoop>>, sock: TcpStream) -> TcpConnection {
        TcpConnection {
            token: None,
            interest: Ready::empty(),
            event_loop: event_loop,
            sock: sock,

            read_buffer: Some(BytesMut::with_capacity(1024)),
            write_buffer_sending: Some(BytesMut::with_capacity(1024)),
            write_buffer_waiting: Some(BytesMut::with_capacity(1024)),

            read_cb: default_read_cb,
            write_cb: default_written_cb,
            error_cb: default_err_cb,
            read_job: Box::new(move |bytes: &mut BytesMut| { println!("default read job"); }),
            writ_job: Box::new(move || { println!("default write job"); true }),

            read: NetState::new(),
            write: NetState::new(),
            is_closed: false
        }
    }

    pub fn set_read_cb(&mut self, cb: fn(bytes: &mut BytesMut)) {
        self.read_cb = cb;
    }

    pub fn set_write_cb(&mut self, cb: fn() -> bool) {
        self.write_cb = cb;
    }

    pub fn set_error_cb(&mut self, cb: fn()) {
        self.error_cb = cb;
    }

    pub fn set_read_job(&mut self, job: ReadJob) {
        self.read_job = job;
    }

    pub fn set_writ_job(&mut self, job: WritJob) {
        self.writ_job = job;
    }

    fn handleRead(&mut self) -> io::Result<()> {
        let mut buf = self.read_buffer.take().unwrap();
        match self.sock.try_read_buf(&mut buf) {
            Ok(None) => {
                println!("Conn: spurious read wakeup");
                self.read_buffer = Some(buf);
            }
            Ok(Some(r)) => {
                //println!("Conn: read {} bytes, {:?}", r, buf);
                // buf toto
                //(self.read_cb)(&mut buf);
                if r > 0 {
                    (self.read_job)(&mut buf);
                    self.read_buffer = Some(buf);
                } else {
                    //println!("r == 0");
                    (self.read_job)(&mut buf);
                    self.read_buffer = Some(buf);
                    self.close_stream();
                }
                //self.interest.remove(Ready::readable());
                //self.interest.insert(Ready::writable());
            }
            Err(e) => {
                println!("not implemented client err: {:?}", e);
                // deregister
                self.close_stream();
            }
        };
        Ok(())
    }

    fn writeData(&mut self) -> io::Result<()> {
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
                //println!("?-------------------------");
                // onWritten() // all data consumed done
                self.write_buffer_waiting = Some(buf);
                self.write_buffer_sending = Some(buf_snd);
                return Ok(())
            }
        }

        let mut buf = buf_tmp;
        //println!("Conn {:?}: write1 ", self.sock);
        match self.sock.try_write_buf(&mut buf) {
            Ok(None) => {
                //println!("client flushing buf; WouldBlock");
                self.write_buffer_sending = Some(buf.split());
            }
            Ok(Some(r)) => {
                //println!("Conn {:?}: write2 {} bytes", self.sock, r);
                if buf.len() > 0 {
                    self.write_buffer_sending = Some(buf.split());
                    return Ok(());
                }
                //(self.write_cb)();
                let ret = (self.writ_job)();
                self.write_buffer_sending = Some(buf.split());
                if ret == false {
                    //println!("close stream1");
                    self.close_stream();
                }
            }
            Err(e) => {
                println!("not implemented; client err: {:?}", e);
                //self.close_stream();
            }
        }

        Ok(())
    }

    pub fn send(&mut self, bytes: BytesMut) -> io::Result<usize> {
        let len = bytes.len();
        if len == 0 {
            return Ok(0);
        }
        let mut buffer = self.write_buffer_waiting.take().unwrap();
        buffer.put(bytes);
        self.write_buffer_waiting = Some(buffer);
        self.writeData();
        return Ok(len);
    }

    fn handleWrite(&mut self) -> io::Result<()> {
        println!("handleWrite");
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
            println!("handleWrite disable write TODO");
        } else {
            self.writeData();
        }
        return Ok(())
    }

    fn handleError(&mut self) -> io::Result<()> {
        Ok(())
    }

    pub fn handleEvent(&mut self, event: i64) -> io::Result<()> {                   // hankai3
        // check closed
        let ready = ready_from_usize(event as usize);
        println!("handleEvent ready----------->: {:?}", ready);
        if ready.is_readable() {
           self.handleRead();
        }
        if ready.is_writable() {
           self.handleWrite();
        }
        if ready.is_error() ||
                ready.is_hup() {
            self.handleError();
        }
        Ok(())
    }

    pub fn attachEvent(&mut self) {
        //self.event_loop.register(&self, SERVER, r|w|e, self.handleEvent) 
    }

    pub fn clone_stream(&mut self) {
    }
    pub fn close_stream(&mut self) {
        self.event_loop.lock().unwrap().deregister(&self.sock);
        println!("close_stream deregister done");
    }
    //pub fn reenable(&mut self, vio: Arc<Mutex<VIO>>)
    pub fn reenable(&mut self, NetState) {                                          // hankai2
        // 如果上层已经enabled 则返回
        // set NetState enabled
        // 如果NetState是read
        //     epoll_ctl READ
        //     if read.triggered  // 即底层已触发读事件
        //          挂到nh的ready list上
        //     else
        //          从nh的ready list摘下来
        // write same as ...
    }
    pub fn do_io_read(&mut self, job, nbytes: i64, buf) {
        // read.vio.op = READ;
        // read.vio.nbytes = nbytes;
        // read.vio.ndone = 0;
        // read.vio.mutex = job's
        // if buf                       // 注意buf是上层维护的
        //    read.vio.buff = buf
        //    if !read.enabled 如果上层没有enabled 则(依赖反转)调用上层的reenable   // hankai1
        // buf为空
        //    read.vio.buff = null
        //    read.enabled = 0   如果buf传空 即说明上层不想读数据了
        // return read.vio
    }
    pub fn do_io_write(&mut self, job, nbytes: i64, buf) {
    }
    pub fn do_io_close
    pub fn do_io_shutdown
}

impl Drop for TcpConnection {
    fn drop(&mut self) {
        //self.event_loop.lock().unwrap().deregister(&self.sock);
        //println!("---------------------drop for tcpconnection {:?}", self.sock)
    }
}

