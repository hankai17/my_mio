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

fn test2() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);

    for _ in 0..5_000 {
        let (r, set) = Registration::new2();
        let b1 = Arc::new(Barrier::new(2));
        let b2 = b1.clone();

        let th = thread::spawn(move || {
            set.set_readiness(Ready::readable()).unwrap(); // 先初始化
            b2.wait();
        });

        b1.wait();
        poll.register(&r, Token(123), Ready::readable(), PollOpt::edge()).unwrap(); // 再注册
        loop {
            let n = poll.poll(&mut events, None).unwrap();
            if n == 0 {
                continue;
            }
            assert_eq!(n, 1);
            assert_eq!(events.get(0).unwrap().token(), Token(123));
            break;
        }
        th.join().unwrap();
    }
}

mod stress {
    use my_mio::{Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
    use my_mio::event::Evented;
    use std::time::Duration;

    fn single_thread_poll() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::{Acquire, Release};
        use std::thread;

        const NUM_ATTEMPTS: usize = 30;
        const NUM_ITERS: usize = 500;
        const NUM_THREADS: usize = 4;
        const NUM_REGISTRATIONS: usize = 128;

        for _ in 0..NUM_ATTEMPTS {
            let poll = Poll::new().unwrap();
            let mut events = Events::with_capacity(NUM_REGISTRATION);
            let registration: Vec<_> = (0..NUM_REGISTRATION).map(|i| {
                let (r, s) = Registration::new2();
                r.register(&poll, Token(i), Ready::readable(), PollOpt::edge()).unwrap();
                (r, s)
            }).collect();
            let mut ready: Vec<_> = (0..NUM_REGISTRATION).map(|_| Ready::empty()).collect();
            let remaining = Arc::new(AtomicUsize::new(NUM_THREADS));

            for _ in 0..NUM_THREADS {
                let remaining = remaining.clone();
                let set_readiness: Vec<SetReadiness> = registration.iter().map(|r| r.1.clone()).collect();
                thread::spawn(move || {
                    for _ in 0..NUM_ITERS {
                        for i in 0..NUM_REGISTRATIONS {
                            set_readiness[i].set_readiness(Ready::readable()).unwrap();
                            set_readiness[i].set_readiness(Ready::empty()).unwrap();
                            set_readiness[i].set_readiness(Ready::writable()).unwrap();
                            set_readiness[i].set_readiness(Ready::readable() | Ready::writable()).unwrap();
                            set_readiness[i].set_readiness(Ready::empty()).unwrap();
                        }
                    }
                    for i in 0..NUM_REGISTRATIONS {
                        set_readiness[i].set_readiness(Ready::readable()).unwrap();
                    }
                    remaining.fetch_sub(1, Release);
                });
            }



        }
    }


}

fn main() {
    test1();
    test2();
}
