extern crate my_mio;

use my_mio::{channel, Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use my_mio::event::Evented;
use std::time::Duration;

fn main() {
    for _ in 0..2000 {
        let poll = Poll::new().unwrap(); 
        let mut events = Events::with_capacity(4);
        let (r, s) = Registration::new2();

        poll.register(&r, Token(0), Ready::readable(), PollOpt::edge()).unwrap();
        poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();

        drop(poll);
        drop(s);
        drop(r);
    } 
}
