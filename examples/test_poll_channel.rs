extern crate my_mio;

use my_mio::{channel, Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use my_mio::event::Evented;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

fn test1() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let (tx, rx) = channel::channel();

    poll.register(&rx, Token(123), Ready::readable(), PollOpt::edge()).unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    tx.send("hello").unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(1, num);

    let event = events.get(0).unwrap();
    assert_eq!(event.token(), Token(123));
    assert_eq!(event.readiness(), Ready::readable());

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    assert_eq!("hello", rx.try_recv().unwrap());

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    tx.send("goodbye").unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(1, num);

    let event = events.get(0).unwrap();
    assert_eq!(event.token(), Token(123));
    assert_eq!(event.readiness(), Ready::readable());

    rx.try_recv().unwrap();

    drop(tx);
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(1, num);

    let event = events.get(0).unwrap();
    assert_eq!(event.token(), Token(123));
    assert_eq!(event.readiness(), Ready::readable());

    match rx.try_recv() {
        Err(TryRecvError::Disconnected) => {}
        no => panic!("unexpected value {:?}", no),
    }
}

fn test2() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let (tx, rx) = channel::channel();

    poll.register(&rx, Token(123), Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    tx.send("hello").unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(1, num);

    let event = events.get(0).unwrap();
    assert_eq!(event.token(), Token(123));
    assert_eq!(event.readiness(), Ready::readable());

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    assert_eq!("hello", rx.try_recv().unwrap());

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    tx.send("goodbye").unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();    // man epoll_ctl
    assert_eq!(0, num);

    for _ in 0..3 {
        poll.reregister(&rx, Token(123), Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
        let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
        assert_eq!(1, num);

        let event = events.get(0).unwrap();
        assert_eq!(event.token(), Token(123));
        assert_eq!(event.readiness(), Ready::readable());
    }

    assert_eq!("goodbye", rx.try_recv().unwrap());

    poll.reregister(&rx, Token(123), Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    poll.reregister(&rx, Token(123), Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);
}

fn test3() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let (tx, rx) = channel::channel();

    poll.register(&rx, Token(123), Ready::readable(), PollOpt::level()).unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    tx.send("hello").unwrap();

    for i in 0..5 {
        let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
        assert!(1 == num, "actual got {} on iteration {}", num, i);

        let event = events.get(0).unwrap();
        assert_eq!(event.token(), Token(123));
        assert_eq!(event.readiness(), Ready::readable());
    }
    assert_eq!("hello", rx.try_recv().unwrap());

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);
}

fn test4() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let (tx, rx) = channel::channel();

    poll.register(&rx, Token(123), Ready::writable(), PollOpt::edge()).unwrap();
    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);

    tx.send("hello").unwrap();

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);
}

fn main() {
    test1();
    //test2();
    //test3();
    //test4();
}




























