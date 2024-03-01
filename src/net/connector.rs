
struct Connector {
    addr: String,
    connector: TcpStream,
    event_loop: Arc<EventLoop>,
    is_connected: bool,
    connect_cb: fn(&TcpStream)
}

impl Connector {
    fn attach(&mut self, event_loop: Arc<EventLoop>) {
        self.event_loop = event_loop;
        self.is_connected = false;
    }
    fn set_connect_cb(&mut self, cb: fn(&TcpStream)) {
        self.connect_cb = cb;
    }
    fn connect(&mut self, addr: &String) {
        let sock = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        self.connector = sock;
        let job = Box::new(move |val: i64| { println!("--------------"); });
        self.event_loop.register(&self.connector, CLIENT, Ready::writable(),
                PollOpt::edge() | PollOpt::oneshot(), job).unwrap();
    }
}
