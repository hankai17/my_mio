extern crate my_mio;

use my_mio::{Token, Ready, PollOpt};
use my_mio::deprecated::{unix, EventLoop, Handler};
use std::time::Duration;
//use my_mio::*;

pub struct BrokenPipeHandler;

impl Handler for BrokenPipeHandler {
    type Timeout = ();
    type Message = ();
    fn ready(&mut self, _: &mut EventLoop<Self>, token: Token, _: Ready) {
        println!("BrokenPipeHandler ready");
        if token == Token(1) {
            //panic!("Reveived ready() on a closed pipe.");
            println!("Reveived ready() on a closed pipe.");
        }
    }
}

struct TestHandler {
    tick: usize,
    state: usize,
}

impl TestHandler {
    fn new() -> TestHandler {
        TestHandler {
            tick: 0,
            state: 0,
        }
    }
}

impl Handler for TestHandler {
    type Timeout = usize;
    type Message = String;
    fn tick(&mut self, _event_loop: &mut EventLoop<TestHandler>) {
        println!("Handler::tick()");
        self.tick += 1;
        assert_eq!(self.state, 1);
        self.state = 0;
    }
    fn ready(&mut self, _event_loop: &mut EventLoop<TestHandler>, token: Token, events: Ready) {
        println!("Ready: {:?} - {:?}", token, events);
        if events.is_readable() {
            println!("Handler::ready() readable event");
            assert_eq!(token, Token(0));
            assert_eq!(self.state, 0);
            self.state = 1;
        }
    }
}

pub fn sleep_ms(ms: u64) {
    use std::thread;
    thread::sleep(Duration::from_millis(ms));
}

pub fn test1() {
    /*
    let (reader, _) = unix::pipe().unwrap();
    println!("-----------------------");
    // writer写成_ 析构是在该test1函数结束后执行
    */

    /*
    let (reader, _) = unix::pipe().unwrap(); // 关闭writer
    println!("11111111111111");
    println!("22222222222222");
    drop(reader); // 关闭reader
    println!("-----------------------");
    */

    /*
    let (reader, writer) = unix::pipe().unwrap();
    println!("11111111111111");
    println!("22222222222222");
    drop(reader); // 此行关闭reader // 函数结尾关闭writer
    println!("-----------------------");
    */

    let mut event_loop: EventLoop<BrokenPipeHandler> = EventLoop::new().unwrap();
    let (reader, _) = unix::pipe().unwrap();
    event_loop.register(&reader, Token(1), Ready::all(), PollOpt::edge()).unwrap();
    let mut handler = BrokenPipeHandler;
    drop(reader);       // drop/close后 epoll不会通知
    event_loop.run_once(&mut handler, Some(Duration::from_millis(1000))).unwrap();
    println!("test1 fun done");
}

pub fn test2() {
    println!("test tick") ;
    let mut event_loop = EventLoop::new().expect("Couldn't make event loop");
    // registe listener
    // registe client connect
    sleep_ms(250);
    println!("after sleep 250ms") ;
    let mut handler = TestHandler::new();
    for _ in 0..2 {
        event_loop.run_once(&mut handler, None).unwrap();
    }
    assert!(handler.tick == 2, "actual={}", handler.tick);
    assert!(handler.state == 0, "actual={}", handler.state);
}

pub fn main() {
    //test1();
    //println!("test1 fun done1");
    test2();
}
