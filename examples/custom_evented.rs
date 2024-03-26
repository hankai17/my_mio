extern crate my_mio;

use my_mio::{Events, Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use my_mio::event::Evented;
use std::time::Duration;

fn test1() {
    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(128);
    let (r, set) = Registration::new2();

    let job = Box::new(move |val: i64| { println!("--------------"); });
    r.register(&poll, Token(0), Ready::readable(), PollOpt::edge(), job).unwrap(); // 分配node 初始化之(记录监听的事件 queue指向poll中的queue)
    let n = poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();
    assert_eq!(n, 0);

    set.set_readiness(Ready::readable()).unwrap();  // 入node中的队列
    let n = poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();
    assert_eq!(n, 1);

    assert_eq!(events.get(0).unwrap().token(), Token(0));
}

/*
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
            set.set_readiness(Ready::readable()).unwrap(); // 先设置状态并没有入队
            b2.wait();
        });

        b1.wait();  // 阻塞直到线程中设置完毕
        poll.register(&r, Token(123), Ready::readable(), PollOpt::edge()).unwrap(); // 再注册 // 跟上面相反
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

fn test3() { // single_thread_poll
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
        let mut events = Events::with_capacity(NUM_REGISTRATIONS);
        let registrations: Vec<_> = (0..NUM_REGISTRATIONS).map(|i| {                        // 分配128个ready态node
            let (r, s) = Registration::new2();
            r.register(&poll, Token(i), Ready::readable(), PollOpt::edge()).unwrap();
            (r, s)
        }).collect();
        let mut ready: Vec<_> = (0..NUM_REGISTRATIONS).map(|_| Ready::empty()).collect();   // 分配128个空ready对象
        let remaining = Arc::new(AtomicUsize::new(NUM_THREADS));

        for _ in 0..NUM_THREADS {
            let remaining = remaining.clone();                                              // remaining只是引用计数+1 用的仍是最初的原子变量
            let set_readiness: Vec<SetReadiness> = registrations.iter()
                .map(|m| m.1.clone()).collect();                                            // 拷贝的是指针
            thread::spawn(move || {                                                         // 起4个线程 每个线程中将node都设置成可读
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

        while remaining.load(Acquire) > 0 { // 128个node处于薛定谔态
            for (i, &(ref r, _)) in registrations.iter().enumerate() {
                r.reregister(&poll, Token(i), Ready::writable(), PollOpt::edge()).unwrap();
            }
            poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();
            for event in &events {
                ready[event.token().0] = event.readiness();
            }
            for (i, &(ref r, _)) in registrations.iter().enumerate() {
                r.reregister(&poll, Token(i), Ready::readable(), PollOpt::edge()) .unwrap();
            }
        }

        loop {
            poll.poll(&mut events, Some(Duration::from_millis(0))).unwrap();
            if events.is_empty() {
                break;
            }
            for event in &events {          // 迭代器trait
                ready[event.token().0] = event.readiness();
            }
        }
        for ready in ready {
            assert_eq!(ready, Ready::readable());
        }
    }
}

fn test4() { // mlti_thread_poll
    use std::sync::{Arc, Barrier};
    use std::sync::atomic::{AtomicUsize};
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    use std::thread;

    const ENTRIES: usize    = 10_000;
    const PRE_ENTRY: usize  = 16;
    const THREADS: usize    = 4;
    const NUM: usize        = ENTRIES * PRE_ENTRY;

    struct Entry {
        registration: Registration,
        set_readiness: SetReadiness,
        num: AtomicUsize,
    }

    impl Entry {
        fn fire(&self) {
            self.set_readiness.set_readiness(Ready::readable()).unwrap();
        }
    }

    let poll = Arc::new(Poll::new().unwrap());
    let mut entries = vec![];

    for i in 0..ENTRIES {   // 分配10000个node 并初始化为ready
        let (registration, set_readiness) = Registration::new2();
        registration.register(&poll, Token(i), Ready::readable(), PollOpt::edge()).unwrap();
        entries.push(Entry{
            registration,
            set_readiness,
            num: AtomicUsize::new(0),
        });
    }
    let total = Arc::new(AtomicUsize::new(0));
    let entries = Arc::new(entries);
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut threads = vec![];

    for th in 0..THREADS {
        let poll = poll.clone();
        let total = total.clone();
        let entries = entries.clone();
        let barrier = barrier.clone();  // 何作用?

        threads.push(thread::spawn(move || {
            let mut events = Events::with_capacity(128);
            barrier.wait();
            let mut i = th;
            while i < ENTRIES {     // 0线程 0 4 8 12 下标的node都排入队列
                                    // 1线程 1 5 9 13
                                    // 2线程 2 6 10 14
                                    // 3线程 3 7 11 15
                                    // 即4个线程均分了10000个node
                entries[i].fire();
                i += THREADS;
            }
            let mut n = 0;
            while total.load(SeqCst) < NUM {    // 2500个node 经16次反复排入队列中
                n += poll.poll(&mut events, Some(Duration::from_millis(100))).unwrap();
                let mut num_this_tick = 0;
                for event in &events {
                    let e = &entries[event.token().0];
                    let mut num = e.num.load(Relaxed);
                    loop {
                        if num < PRE_ENTRY {
                            let actual = e.num.compare_and_swap(num, num + 1, Relaxed);
                            if actual == num {
                                num_this_tick += 1;
                                e.fire();
                                break;
                            }
                            num = actual;
                        } else {
                            break;
                        }
                    }
                }
                total.fetch_add(num_this_tick, SeqCst);
            }
            n
        }));
    }

    let _: Vec<_> = threads.into_iter()
        .map(|th| th.join().unwrap())
        .collect();
    for entry in entries.iter() {
        assert_eq!(PRE_ENTRY, entry.num.load(Relaxed));
    }
}

fn test5() { // with_small_events_collection
    const N: usize = 8;
    const ITER: usize = 1_000;

    use std::sync::{Arc, Barrier};
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::{Acquire, Release};
    use std::thread;

    let poll = Poll::new().unwrap();
    let mut registrations = vec![];
    let barrier = Arc::new(Barrier::new(N + 1));
    let done = Arc::new(AtomicBool::new(false));

    for i in 0..N {
        let (registration, set_readiness) = Registration::new2();
        poll.register(&registration, Token(i), Ready::readable(), PollOpt::edge()).unwrap();
        registrations.push(registration);
        let barrier = barrier.clone();
        let done = done.clone();

        thread::spawn(move || {
            barrier.wait();
            while !done.load(Acquire) {
                set_readiness.set_readiness(Ready::readable()).unwrap();    // 1死循环入队
            }
            set_readiness.set_readiness(Ready::readable()).unwrap();
        });
    }

    let mut events = Events::with_capacity(4);
    barrier.wait();
    for _ in 0..ITER {
        poll.poll(&mut events, None).unwrap();  // 1polling
    }
    done.store(true, Release);  // 子线程的最后 8个ready node重新已入队
    let mut final_ready = vec![false; N];

    for _ in 0..5 {
        poll.poll(&mut events, None).unwrap();
        for event in &events {
            final_ready[event.token().0] = true;
        }
        if final_ready.iter().all(|v| *v) {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("dead lock?");
}
*/

fn test6() {
    use std::thread;
    use std::sync::mpsc::channel;

    const THREADS: usize = 8;
    const ITERS: usize = 50_000;

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);
    let mut senders = Vec::with_capacity(THREADS);
    let mut token_index = 0;
    
    for _ in 0..THREADS { // 起8个线程 每个线程中起一个channel并阻塞读取reader端 线程中拿到node则排入队列
        let (tx, rx) = channel::<(Registration, SetReadiness)>();
        senders.push(tx);
        thread::spawn(move || {
            for (registration, set_readiness) in rx {
                let _ = set_readiness.set_readiness(Ready::readable());
                drop(registration);
                drop(set_readiness);
            }
        });
    }

    let mut index: usize = 0;
    for _ in 0..ITERS { // 50000个node 平均排入8个channel的writer端
        let (registration, set_readiness) = Registration::new2();
        let job = Box::new(move |val: i64| { println!("--------------"); });
        registration.register(&poll, Token(token_index), Ready::readable(), PollOpt::edge(), job).unwrap();
        let _ = senders[index].send((registration, set_readiness));
        token_index += 1;
        index += 1;
        if index == THREADS {   // 主线程时不时的分配node排入队列
            index = 0;
            let (registration, set_readiness) = Registration::new2();
            let job = Box::new(move |val: i64| { println!("--------------"); });
            registration.register(&poll, Token(token_index), Ready::readable(), PollOpt::edge(), job).unwrap();
            let _ = set_readiness.set_readiness(Ready::readable());
            drop(registration);
            drop(set_readiness);
            token_index += 1;
            thread::park_timeout(Duration::from_millis(0)) ;
            let _ = poll.poll(&mut events, None).unwrap();
        }
    }
}

fn main() {
    test1();
    //test2();
    //test3();
    //test4();
    //test5();
    //test6();
}

