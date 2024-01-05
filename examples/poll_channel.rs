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

fn test5() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let (tx, rx) = channel::channel();
    poll.register(&rx, Token(123), Ready::readable(), PollOpt::edge()).unwrap();

    tx.send("hello").unwrap();
    drop(rx);

    let num = poll.poll(&mut events, Some(Duration::from_millis(300))).unwrap();
    assert_eq!(0, num);
}


fn test6() {
    use my_mio::net::{TcpListener, TcpStream};
    use my_mio::event::Event;
    use std::thread;

    fn expect_events(poll: &Poll, event_buffer: &mut Events, poll_try_count: usize, mut expected: Vec<Event>) {
        const MS: u64 = 1_000;
        for _ in 0..poll_try_count {
            poll.poll(event_buffer, Some(Duration::from_millis(MS))).unwrap();
            for event in event_buffer.iter() {
                let pos_opt = match expected.iter().position(|exp_event| {
                    (event.token() == exp_event.token()) &&
                    event.readiness().contains(exp_event.readiness())
                }) {
                    Some(x) => Some(x),
                    None => None,
                };
                if let Some(pos) = pos_opt { expected.remove(pos); }
            }
            if expected.is_empty() {
                break;
            }
        }
        assert!(expected.is_empty(), "The following expected events were not found: {:?}", expected);
    }

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let (tx, rx) = channel::channel();
    let l = TcpListener::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    poll.register(&l, Token(0), Ready::readable(), PollOpt::edge()).unwrap();
    poll.register(&rx, Token(1), Ready::readable(), PollOpt::edge()).unwrap();
    tx.send("hello").unwrap(); // rx收到读事件
    let s1 = TcpStream::connect(&l.local_addr().unwrap()).unwrap(); // l收到读事件
    poll.register(&s1, Token(2), Ready::readable(), PollOpt::edge()).unwrap(); // 监控vc读
    thread::sleep(Duration::from_millis(250));
    expect_events(&poll, &mut events, 2, vec![
        Event::new(Ready::empty(), Token(0)),
        Event::new(Ready::empty(), Token(1)),
    ]);
}

fn test7() {
    const ITERATIONS: usize = 20;
    const THREADS: usize = 5;
    use std::thread;

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    for _ in 0..ITERATIONS {
        let (tx, rx) = channel::channel();
        poll.register(&rx, Token(0), Ready::readable(), PollOpt::edge()).unwrap();
        for _ in 0..THREADS {
            let tx = tx.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(250));
                tx.send("ping").unwrap();
            });
        }
        let mut recv = 0;
        while recv < THREADS {
            let num = poll.poll(&mut events, None).unwrap();
            if num != 0 {
                assert_eq!(1, num);
                assert_eq!(events.get(0).unwrap().token(), Token(0));
                while let Ok(_) = rx.try_recv() {
                    recv += 1;
                }
            }
        }
    }
}

fn main() {
    //test1();
    //test2();
    //test3();
    //test4();
    test5();
    test6();
    test7();
}

