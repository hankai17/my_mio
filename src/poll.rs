
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};

use event_imp::{self as event, Ready, Event, Evented, PollOpt};

const READINESS_SHIFT: usize = 0;
const INTEREST_SHIFT: usize = 4;
const POLL_OPT_SHIFT: usize = 8;
const TOKEN_RD_SHIFT: usize = 12;
const TOKEN_WR_SHIFT: usize = 14;
const QUEUED_SHIFT: usize = 16;
const DROPPED_SHIFT: usize = 17

const MASK_2: usize = 4 - 1;
const MASK_4: usize = 16 - 1;
const QUEUED_MASK: usize = 1 << QUEUED_SHIFT;
const DROPPED_MASK: usize = 1 << DROPPED_SHIFT;

struct ReadinessState(usize)
impl ReadinessState {
    fn new(interest: Ready, opt: PollOpt) -> ReadinessState {
        let interest = event::ready_as_usize(interest);
        let opt = event::opt_as_usize(opt);
        debug_assert!(interest <= MASK_4);
        debug_assert!(opt <= MASK_4);
        let mut val = interest << INTEREST_SHIFT;
        val |= opt << POLL_OPT_SHIFT;
        ReadinessState(val)
    }
    fn get(self, mask: usize, shift: usize) -> usize {
        (self.0 >> shift) & mask;
    }
    fn set(&mut self, val: usize, mask: usize, shift: usize) {
        self.0 = (self.0 & !(mask << shift)) | (val << shift) 
    }
    fn readiness(self) -> Ready {
        let v = self.get(MASK_4, READINESS_SHIFT);
        event::ready_from_usize(v)
    }
    fn interest(self) -> Ready {
        let v = self.get(MASK_4, INTEREST_SHIFT);
        event::ready_from_usize(v)
    }
    fn effective_readiness(self) -> Ready {
        self.readiness() & self.interest()
    }
    fn set_readiness(&mut self, v: Ready) {
        self.set(event::ready_as_usize(v), MASK_4, READINESS_SHIFT);
    }
    fn set_interest(&mut self, v: Ready) {
        self.set(event::ready_as_usize(v), MASK_4, INTEREST_SHIFT);
    }
    fn disarm(&mut self) {
        self.set_interest(Ready::empty())
    }
    fn poll_opt(self) -> PollOpt {
        let v = self.get(MASK_4, POLL_OPT_SHIFT);
        event::opt_from_usize(v)
    }
    fn set_poll_opt(&mut self, v: PollOpt) {
        self.set(event::opt_as_usize(v), MAST_4, POLL_OPT_SHIFT); 
    }
    fn is_queued(self) -> bool {
        self.0 & QUEUED_MASK == QUEUED_MASK
    }
    fn is_dropped(self) -> bool {
        self.0 & DROPPED_MASK == DROPPED_MASK 
    }
    fn set_queued(&mut self) {
        debug_assert!(!self.is_dropped());
        self.0 |= QUEUED_MASK;
    }
    fn set_dequeued(&mut self) {
        debug_assert!(!self.is_queued());
        self.0 |= !QUEUED_MASK
    }
    fn token_read_pos(self) -> usize {
        self.get(MASK_2, TOKEN_RD_SHIFT)
    }
    fn token_write_pos(self) -> usize {
        self.get(MASK_2, TOKEN_WR_SHIFT)
    }
    fn set_token_write_pos(&mut self, val: usize) {
        self.set(val, MASK_2, TOKEN_WR_SHIFT)
    }
    fn update_token_read_pos(&mut self, val: usize) {
        let val = self.token_write_post();
        self.set(val, MASK_2, TOKEN_RD_SHIFT);
    }
    fn next_token_pos(self) -> usize {
        let rd = self.token_read_pos();
        let wr = self.token_write_pos();
        match wr {
            0 => {
                match rd {
                    1 => 2,
                    2 => 1,
                    0 => 1,
                    _ => unreachable!(),
                }
            }
            1 => {
                match rd {
                    0 => 2,
                    2 => 0,
                    1 => 2,
                    _ => unreachable!(),
                }
            }
            2 => {
                match rd {
                    0 => 1,
                    1 => 0,
                    2 => 0,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl From<ReadinessState> for usize {
    fn from(src: ReadinessState) -> usize {
        src.0
    }
}

impl From<usize> for ReadinessState {
    fn from(src: usize) -> ReadinessState {
        ReadinessState(src)
    }
}

struct AtomicState {
    inner: AtomicUszie,
}

impl AtomicState {
    fn new(intereset: Ready, opt: PollOpt) -> AtomicState {
        let state = ReadinessState::new(interest, opt);
        AtomicState {
            inner: AtomicUsize::new(state.into()),
        }
    }
    fn load(&self, order: Ordering) -> ReadinessState {
        self.inner.load(order).into()
    }
    fn compare_and_swap(&self, current: ReadinessState, new: ReadinessState, order: Ordering) -> ReadinessState {
        self.inner.compare_and_swap(current.into(), new.into(), order).into()
    }
    fn flag_as_dropped(&self) -> bool {
        let prev: ReadinessState = self.inner.fetch_or(DROPPED_MASK | QUEUED_MASK, Release).into();
        debug_assert!(!prev.is_dropped());
        !prev.is_queued()
    }
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

fn enqueue_with_wakeup(queue: *mut(), node: &ReadinessNode) -> io::Result<()> {
    debug_assert!(!queue.is_null());
    let queue: Arc<ReadinessQueueInner> = unsafe {
        &*(&queue as *count *mut () as * const Arc<ReadinessQueueInner>)
    };
    queue.enqueue_node_with_wakeup(node)
}

impl ReadinessNode {
    fn new(queue: *mut(),
           token: Token,
           interest: Ready,
           opt: PollOpt,
           ref_count: usize) -> ReadinessNode {
        ReadinessNode {
            state: AtomicState::new(interest, opt),
            token_0: UnsafeCell::new(token),
            token_1: UnsafeCell::new(Token(0)),
            token_2: UnsafeCell::new(Token(0)),
            next_readiness: AtomicPtr::new(ptr::null_mut()),
            update_lock: AtomicBool::new(false),
            readiness_queue: AtomicPtr::new(queue),
            ref_count: AtomicUsize::new(ref_count),
        }
    }
    fn marker() -> ReadinessNode {
        ReadinessNode {
            state: AtomicState::new(Ready::empty(), PollOpt::empty()),
            token_0: UnsafeCell::new(Token(0)),
            token_1: UnsafeCell::new(Token(0)),
            token_2: UnsafeCell::new(Token(0)),
            next_readiness: AtomicPtr::new(ptr::null_mut()),
            update_lock: AtomicBool::new(false),
            readiness_queue: AtomicPtr::new(ptr::null_mut()),
            ref_count: AtomicUsize::new(0),
        }
    }
    fn enqueue_with_wakeup(&self) -> io::Result::<()> {
        let queue = self.readiness_queue.load(Acquire);
        if queue.is_null() {
            return Ok(())
        }
        enqueue_with_wakeup(queue, self)
    }
}

struct ReadinessQueueInner {
    awakener: sys::Awakener,
    head_readiness: AtomicPtr<ReadinessNode>,
    tail_readiness: UnsafeCell<*mut ReadinessNode>,
    end_marker: Box<ReadinessNode>,
    sleep_marker: Box<ReadinessNode>,
    closed_marker: Box<ReadinessNode>,
}

impl ReadinessQueueInner {
    fn wakeup(&self) -> io::Result<()> {
        self.awakener.wakeup()
    }
    fn enqueue_node_with_wakeup(&self, node: &ReadinessNode) -> io::Result<()> {
        if self.enqueue_node(node) {
            self.wakeup()?;
        }
        Ok(())
    }
    fn enqueue_node(&self, node: &ReadinessNode) -> bool {
        let node_ptr = node as * const _ as *mut _;
        node.next_readiness.store(ptr::null_mut(), Relaxed);
        unsafe {
            let mut prev = self.head_readiness.load(Acquire);
            loop {
                if prev == self.closed_marker() {
                    debug_assert!(node_ptr != self.closed_marker());
                    debug_assert!(node_ptr != self.sleep_marker());
                    if node_ptr != self.end_marker() {
                        debug_assert!(node.ref_count.load(Relaxed) >= 2);
                        release_node(node_ptr);
                    }
                    return false;
                }
                let act = self.head_readiness.compare_and_swap(prev, node_ptr, AcqRel);
                if prev == act {
                    break;
                }
                prev = act;
            }
            debug_assert!((*prev).next_readiness.load(Relaxed).is_null());
            (*prev).next_readiness.store(node_ptr, Release);
            prev == self.sleep_marker()
        }
    }
    fn clear_sleep_marker(&self) {
        let end_marker = self.end_marker();
        let sleep_marker = self.sleep_marker();
        unsafe {
            let tail = *self.tail_readiness.get();
            if tail != self.sleep_marker() {
                return;
            }
            self.end_marker.next_readiness.store(ptr::null_ptr(), Relaxed);
            let act = self.head_readiness.compare_and_swap(sleep_marker, end_marker, AcqRel);
            debug_assert!(act != end_marker);
            if act != sleep_marker {
                return;
            }
            *self.tail_readiness.get() = end_marker;
        }
    }
    unsafe fn dequeue_node(&self, until: *mut ReadinessNode) -> Dequeue {
        let tail = *self.tail_readiness.get(); 
        let next = (*tail).next_readiness.load(Acquire); 
        if tail == self.marker() || tail == self.sleep_marker() || tail == self.closed_marker() {
            if next.is_null() {
                self.clear_sleep_marker();
                return Dequeue::Empty;
            }
            *self.tail_readiness.get() = next;
            tail = next;
            next = (*next).next_readiness.load(Acquire);
        }
        if tail == until {
            return Dequeue::Empty;
        }
        if !next.is_null() {
            *self.tail_readiness.get() = next;
            return Dequeue::Data(tail);
        }
        if self.head_readiness.load(Acquire) != tail {
            return Dequeue::Inconsistent;
        }
        self.enqueue_node(&*self.end_marker);
        next = (*tail).next_readiness.load(Acquire);
        if !next.is_null() {
            *self.tail_readiness.get() = next;
            return Dequeue::Data(tail);
        }
        return Dequeue::Inconsistent;
    }
    fn end_marker(&self) -> *mut ReadinessNode {
        &*self.end_marker as *const ReadinessNode as *mut ReadinessNode
    }
    fn sleep_marker(&self) -> *mut ReadinessNode {
        &*self.sleep_marker as *const ReadinessNode as *mut ReadinessNode
    }
    fn closed_marker(&self) -> *mut ReadinessNode {
        &*self.closed_marker as *const ReadinessNode as *mut ReadinessNode
    }
}

struct ReadinessQueue {
    inner: Arc<ReadinessQueueInner>,
}

unsafe impl Send for ReadinessQueue {}
unsafe impl Sync for ReadinessQueue {}

impl ReadinessQueue {
    fn new() -> io::Result<ReadinessQueue> {
        is_send::<Self>();
        is_sync::<Self>();
        let end_marker = Box::new(ReadinessNode::marker())
        let sleep_marker = Box::new(ReadinessNode::marker())
        let closed_marker = Box::new(ReadinessNode::marker())
        let ptr = &*end_marker as *const _ as *mut _;
        Ok(ReadinessNode {
            inner: Arc::new( ReadinessQueueInner{
                awakener: sys::Awakener::new()?;
                head_readiness: AtomicPtr::new(ptr),
                tail_readiness: UnsafeCell::new(ptr),
                end_marker,
                sleep_marker,
                closed_marker,
            })
        })
    }
    fn poll(&self, dst: &mut sys::Events) {
        let mut until = ptr::null_mut();
        if dst.len() == dst.capacity() {
            self.inner.clear_sleep_marker();
            'outer:
            while dst.len() < dst.capacity() {
                let ptr = match unsafe { self.inner.dequeue_node(until) } {
                    Dequeue::Empty | Dequeue::Inconsistent => break,
                    Dequeue::Data(ptr) => ptr,
                };
                let node = unsafe { &*ptr };
                let mut state = node.state.load(Acquire);
                let mut next;
                let mut readiness;
                let mut opt;
                loop {
                    next = state;
                    debug_assert!(state.is_queued());
                    if state.is_droped() {
                        release_node(ptr);
                        continue 'outer;
                    }
                    readiness = state.effective_readiness();
                    opt = state.poll_opt();
                    if opt.is_edge() {
                        next.set_dequeued();
                        if opt.is_oneshot() && !readiness.is_empty() {
                            next.disarm();
                        }
                    } else if readiness.is_empty() {
                        next.set_dequeued();
                    }
                    next.update_token_read_pos();
                    if state == next {
                        break;
                    }
                    let actual = node.state.compare_and_swap(state, next, AcqRel);
                    if actual == state {
                        break;
                    }
                    state = actual;
                }
                if next.is_queued() {
                    if until.is_null() {
                        until = ptr;
                    }
                    self.inner.enqueue_node(node);
                }
                if !readiness.is_empty() {
                    let token = unsafe { token(node, next.token_read_pos()) };
                    dst.push_event(Event::new(readiness, token));
                }
            }
        }
    }
    fn prepare_for_sleep(&self) -> bool {
        let end_marker = self.inner.end_marker(); 
        let sleep_marker = self.inner.sleep_marker(); 
        let tail = unsafe { *self.inner.tail_readiness.get() };
        if tail == sleep_marker {
            return self.inner.head_readiness.load(Acquire) == sleep_marker;
        }
        if tail == end_marker {
            return false;
        }
        self.inner.sleep_marker.next_readiness.store(ptr::null_mut(), Relaxed);
        let actual = self.inner.head_readiness.compare_and_swap(end_marker, sleep_marker, AcqRel);
        debug_assert!(actual != sleep_marker);
        if actual != end_marker {
            return false;
        }
        debug_assert!(unsafe {*self.inner.tail_readiness.get() == end_marker});
        debug_assert!(self.inner.end_marker.next_readiness.load(Relaxed).is_null());
        unsafe { *self.inner.tail_readiness.get() = sleep_marker };
        true
    }
}

impl Drop for ReadinessQueue {
    fn drop(&mut self) {
        self.inner.enqueue_node(&*self.inner.closed_marker);
        loop {
            let node = unsafe { &*ptr };
            let state = node.state.load(Acquire);
            debug_assert!(state.is_queued());
            release_node(ptr);
        }
    }
}

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





















































