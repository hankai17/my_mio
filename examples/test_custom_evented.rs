extern crate my_mio;

use my_mio::{Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use my_mio::event::Evented;
use std::time::Duration;

fn test1() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);
    let (r, set) = Registration::new2();

    r.register(&poll, Token(0), Ready::readable(), PollOpt::edge()).unwrap(); // 分配node 初始化之(记录监听的事件 queue指向poll中的queue)
    let n = poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();
    assert_eq!(n, 0);

    set.set_readiness(Ready::readable()).unwrap();
    let n = poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();
    assert_eq!(n, 1);

    assert_eq!(events.get(0).unwrap().token(), Token(0));
}

fn main() {
    test1();

}
