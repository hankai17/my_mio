extern crate my_mio;

use my_mio::{Token, Ready, PollOpt};
use my_mio::deprecated::{unix, EventLoop, Handler};
use std::time::Duration;

pub struct BrokenPipeHandler;

impl Handler for BrokenPipeHandler {
    type Timeout = ();
    type Message = ();
    fn ready(&mut self, _: &mut EventLoop<Self>, token: Token, _: Ready) {
        if token == Token(1) {
            panic!("Reveived ready() on a closed pipe.");
        }
    }
}

pub fn main() {
    let mut event_loop: EventLoop<BrokenPipeHandler> = EventLoop::new().unwrap();
    let (reader, _) = unix::pipe().unwrap();
    event_loop.register(&reader, Token(1), Ready::all(), PollOpt::edge()).unwrap();
    let mut handler = BrokenPipeHandler;
    drop(reader);
    event_loop.run_once(&mut handler, Some(Duration::from_millis(1000))).unwrap();
}
