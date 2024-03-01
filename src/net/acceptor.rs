

unsafe impl Send for Acceptor {}
unsafe impl Sync for Acceptor {}
struct Acceptor {
    tcp_listener: TcpListener,
    event_loop: Arc<EventLoop>,
    is_listening: bool,
    accept_cb: fn(TcpStream, SocketAddr)
}

fn default_accept_cb(stream: TcpStream, addr: SocketAddr) {}

impl Acceptor {
    fn new(event_loop: Arc<EventLoop>, addr: &String) -> Acceptor {
        Acceptor {
            tcp_listener: TcpListener::new(&(addr.parse().unwrap())),
            event_loop: event_loop,
            is_listening: false,
            accept_cb: default_accept_cb,
        }
    }
    fn set_accept_cb(&mut self, cb: fn(TcpStream, SocketAddr)) {
        self.accept_cb = cb;
    }
    pub fn handleRead(&self, val: i64) {
        let (stream, addr) = self.tcp_listener.accept().unwrap();
        println!("accept {}", addr);
        //(self.accept_cb)(stream, addr);
        //println!("---hello world-------------- {}", val);
        //let mut connection = TcpConnection::new(self.event_loop.clone(), stream);
        //self.event_loop.run(&mut connection);
        // 怎样注册事件? // 模拟server1.rs ?

        let mut conn = Arc::new(Mutex::new(TcpConnection::new(self.event_loop.clone(), stream)));
        let clone = conn.clone();
        let job = Box::new(move |val: i64| { clone.lock().unwrap().handleRead(val); });
        self.event_loop.register(&conn.lock().unwrap().sock, SERVER, Ready::readable(), 
                PollOpt::edge(), job);
    }
    fn bind(&self, job: Job) {
        self.event_loop.register(&self.tcp_listener, SERVER, Ready::readable(), 
                PollOpt::edge(), job);
        //self.is_listening = true;
    }
}
