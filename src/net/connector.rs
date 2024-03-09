
struct Connector {
    addr: String,
    connector: Option<TcpStream>,
    event_loop: Arc<Mutex<EventLoop>>,
    is_connected: bool,
    connect_cb: fn(&mut TcpStream)
}

fn default_connected_cb(stream: &mut TcpStream) {}

impl Connector {
    fn new(event_loop: Arc<Mutex<EventLoop>>, addr: &String) -> Connector {
        Connector {
            addr: addr,
            connector: Some(None),
            is_connected: false,
            connect_cb: default_connected_cb
        }
    }

    fn set_connect_cb(&mut self, cb: fn(&mut TcpStream)) {
        self.connect_cb = cb;
    }

    fn handleRead(&mut self) -> io::Result<()> {
        // check connect ret
        // del event
        // cb (new connection ?)
    }

    pub fn handleEvent(&mut self, event: i64) -> io::Result<()> {
        //self.event_loop.register(&self.connector, CLIENT, Ready::writable(),
                //PollOpt::edge() | PollOpt::oneshot(), job).unwrap();
    }
    fn connect(&mut self, addr: &String) {
        let sock = TcpStream::connect(&(addr.parse().unwrap())).unwrap();
        self.connector = Some(sock);
    }
}

// connector.connect
// event_loop.register(connector.handleEvent)
