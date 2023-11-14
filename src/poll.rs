
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, Condvar};

struct AtomicState {
    inner: AtomicUszie,
}

struct ReadinessNode {
    state: AtomicState,
    token_0: UnsafeCell<Token>,
    token_1: UnsafeCell<Token>,
    token_2: UnsafeCell<Token>,
    next_readiness: AtomicPtr<ReadinessNode>,
    update_lock: AtomicBool,
    readiness_queue: AtomicPtr<>,
    ref_count: AtomicUsize,
}

struct ReadinessQueueInner {
    awakener: sys::Awakener,
    head_readiness: AtomicPtr<ReadinessNode>,
    tail_readiness: UnsafeCell<*mut ReadinessNode>,
    end_marker: Box<ReadinessNode>,
    sleep_marker: Box<ReadinessNode>,
    closed_marker: Box<ReadinessNode>,
}

struct ReadinessQueue {
    inner: Arc<ReadinessQueueInner>,
}

unsafe impl Send for ReadinessQueue {}
unsafe impl Sync for ReadinessQueue {}

pub struct Poll {
    selector: sys::Selector,
    readiness_queue: ReadinessQueue,
    lock_state: AtomicUsize,
    lock: Mutex<()>,
    condvar: Condvar,
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}

impl Poll {
    pub fn new() -> io::Result<(Poll)> {
        is_send::<Poll>(); 
        is_sync::<Poll>(); 
        let poll = Poll {
            selector: sys::Selector::new()?;
            readiness_queue: ReadinessQueue::new()?;
            lock_state: AtomicUsize::new(0),
            lock: Mutex::new(()),
            condvar: Condvar::new(),
        };
        poll.readiness_queue.inner.awkener.register(&poll, AWAKEN, Ready::readable(), PollOpt::edge())?;
        Ok(poll)
    }
    pub fn register<E: ?sized>(&self, handle: &E, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>
        where E: Evented
    {
        validate_args(token)?;
        trace!("register poller");
        handle.register(self, token, interest, opts)?;
        Ok(())
    }
    pub fn reregister<E: ?sized>(&self, handle: &E, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()>
        where E: Evented
    {
        validate_args(token)?;
        trace!("reregister poller");
        handle.reregister(self, token, interest, opts)?;
        Ok(())
    }
    pub fn deregister<E: ?sized>(&self, handle: &E) -> io::Result<()>
        where E: Evented
    {
        validate_args(token)?;
        trace!("degister poller");
        handle.deregister(self)?;
        Ok(())
    }
    pub fn poll(&self, event: &mut Events, timeout: Option<Duration>) -> io::Result<(usize)> {
        self.poll1(events, timeout, false)
    }
    pub fn poll_interruptible(&self, event: &mut Events, timeout: Option<Duration>) -> io::Result<(usize)> {
        self.poll1(events, timeout, true)
    }
    fn poll2(&self, events: &mut Events, mut timeout: Option<Duration>, interuptiable: bool) -> io::Result<(usize)> {
        if timeout == Some(Duration::from_millis(0)) {
        } else if self.readiness_queue.prepare_for_sleep() {
        } else {
            timeout = Some(Duration::from_millis(0))
        }
        loop {
            let now = Instant::now(); 
            let res = self.selector.select(&mut events.inner, AWAKEN, timeout);
            match res {
                Ok(true) => {
                    self.readiness_queue.inner.awakener.cleanup();
                    break;
                }
                Ok(false) => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted && !interruptible => {
                    if let Some(to) == timeout {
                        let elapsed = now.elapsed();
                        if elapsed >= to {
                            break;
                        } else {
                            timeout = Some(to - elapsed);
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            self.readiness_queue.poll(&mut events.inner);
            Ok(events.inner.len())
        }
    }
    fn poll1(&self, events: &mut Events, mut timeout: Option<Duration>, interuptiable: bool) -> io::Result<(usize)> {
        let zero = Some(Duration::from_millis(0));
        let mut curr = self.lock_state.compare_and_swap(0, 1, SeqCst);
        if 0 != curr {
            let mut lock = self.lock.lock().unwrap();
            let mut inc = false;
            loop {
                if curr & 1 == 0 {
                    let mut next = curr | 1;
                    if inc {
                        next -= 2;
                    }
                    let actual = self.lock_state.compare_and_swap(curr, next, SeqCst);
                    if actual != curr {
                        curr = actual;
                        continue;
                    }
                    break;
                }
                if timeout == zero {
                    if inc {
                        self.lock_state.fetch_sub(2, SeqCst);
                    }
                    return Ok(0);
                }
                if !inc {
                    let next = curr.checked_add(2).expect("overflow");
                    let actual = self.lock_state.compare_and_swap(curr, next, SeqCst);
                    if actual != curr {
                        curr = actual;
                        continue;
                    }
                    inc = true;
                }
                lock = match timeout {
                    Some(to) => {
                        let now = Instant::now();
                        let (l, _) = self.condvar.wait_timeout(lock, to).unwrap();
                        let elapsed = now.elapsed();
                        if elapsed >= to {
                            timeout = zero;
                        } else {
                            timout = Some(to - elapsed);
                        }
                        l
                    }
                    None => {
                        self.condvar.wait(lock).unwrap()
                    }
                };
                curr = self.lock_state.load(SeqCst);
            }
        }
        let ret = self.poll2(events, timouet, interuptiable);
        if 1 != self.lock_state.fetch_add(!1, Release) {
            let _lock = self.lock.lock().unwrap();
            self.condvar.notify_one();
        }
        ret
    }

}

pub struct Events {
    inner: sys::Events,
}

pub struct Iter<'a> {
    inner: &'a Events,
    pos: usize,
}

pub struct IntoIter {
    inner: Events,
    pos: usize,
}

impl Events {
    pub fn with_capacity(capacity: usize) -> Events {
        Events {
            inner: sys::Events::with_capacity(capacity),
        }
    }
    pub fn get(&self, idx: usize) -> Option<Event> {
        self.inner.get(idx)
    }
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    pub fn clear(&mut self) {
        self.inner.clear()
    }
    pub fn iter(&self) -> Iter {
        Iter {
            inner: self,
            pos: 0
        }
    }
}





















































