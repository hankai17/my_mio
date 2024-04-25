use std::{fmt, io, ptr, usize};
use std::{mem, ops, isize};
use std::os::unix::io::{AsRawFd, RawFd};
use std::process;
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::cell::RefCell;

use event_imp::{self as event, Ready, Event, Evented, PollOpt, Job, JobEntry, TokenEntry, TokenType};
use {Token, sys};

const READINESS_SHIFT: usize = 0;
const INTEREST_SHIFT: usize = 4;
const POLL_OPT_SHIFT: usize = 8;
const TOKEN_RD_SHIFT: usize = 12;
const TOKEN_WR_SHIFT: usize = 14;
const QUEUED_SHIFT: usize = 16;
const DROPPED_SHIFT: usize = 17;

const MASK_2: usize = 4 - 1;
const MASK_4: usize = 16 - 1;
const QUEUED_MASK: usize = 1 << QUEUED_SHIFT;
const DROPPED_MASK: usize = 1 << DROPPED_SHIFT;

const AWAKEN: Token = Token(usize::MAX);
const MAX_REFCOUNT: usize = (isize::MAX) as usize;

pub struct IdAllocator {
    counter: AtomicUsize,
    free: Mutex<Vec<usize>>,
}

impl IdAllocator {
    pub fn new() -> Self {
        IdAllocator {
            counter: AtomicUsize::new(0),
            free: Mutex::new(Vec::new()),
        }
    }
    pub fn alloc(&self) -> usize {
        self.free
            .lock()
            .and_then(|mut free| {
                match free.pop() {
                    Some(v) => Ok(v),
                    None => Ok(self.counter.fetch_add(1, Ordering::Relaxed))
                }
            })
            .unwrap()
    }
    pub fn kill(&self, id: usize) {
        self.free.lock().unwrap().push(id);
    }
}

pub struct TokenAllocator {
    id: IdAllocator,
    token_map: HashMap<usize, TokenEntry>,
}

impl TokenAllocator {
    pub fn new() -> Self {
        TokenAllocator {
            id: IdAllocator::new(),
            token_map: HashMap::new(),
        }
    }
    pub fn alloc(&mut self, ttype: TokenType, token: Token) -> usize {
        let id = self.id.alloc();
        // thread safe TODO
        let mut entry = TokenEntry {
            ttype,
            token,
        };
        self.token_map.insert(id, entry);
        id
    }
    pub fn get(&mut self, id: usize) -> TokenEntry {
        *self.token_map.get(&id).unwrap()
    }
    pub fn get_entry(&mut self, id: usize) -> Option<TokenEntry> {
        self.token_map.get(&id).copied()
    }
    pub fn dealloc(&mut self, id: usize) {
        self.token_map.remove(&id);
        self.id.kill(id);
    }
}

fn validate_args(token: Token) -> io::Result<()> {
    if token == AWAKEN {
        return Err(io::Error::new(io::ErrorKind::Other, "invalid token"));
    }
    Ok(())
}

pub struct SelectorId {
    id: AtomicUsize,
}

impl SelectorId {
    pub fn new() -> SelectorId {
        SelectorId {
            id: AtomicUsize::new(0)
        }
    }
    pub fn associate_selector(&self, poll: &Poll) -> io::Result<()> {
        let selector_id = self.id.load(Ordering::SeqCst);
        if selector_id != 0 && selector_id != poll.selector.id() {
            Err(io::Error::new(io::ErrorKind::Other, "Socket already registered"))
        } else {
            self.id.store(poll.selector.id(), Ordering::SeqCst);
            Ok(())
        }
    }
}

impl Clone for SelectorId {
    fn clone(&self) -> SelectorId {
        SelectorId {
            id: AtomicUsize::new(self.id.load(Ordering::SeqCst))
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct ReadinessState(usize);   // | queue |  RW  | opt | interest | readiness
                                // 20      16     12    8          4      <--0
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
        (self.0 >> shift) & mask
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
    fn poll_opt(self) -> PollOpt {
        let v = self.get(MASK_4, POLL_OPT_SHIFT);
        event::opt_from_usize(v)
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
    fn set_poll_opt(&mut self, v: PollOpt) {
        self.set(event::opt_as_usize(v), MASK_4, POLL_OPT_SHIFT); 
    }
    fn disarm(&mut self) {
        self.set_interest(Ready::empty())
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
        debug_assert!(self.is_queued());
        self.0 &= !QUEUED_MASK
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
    fn update_token_read_pos(&mut self) {
        let val = self.token_write_pos();
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
    inner: AtomicUsize,
}

impl AtomicState {
    fn new(interest: Ready, opt: PollOpt) -> AtomicState {
        let state = ReadinessState::new(interest, opt);
        AtomicState {
            inner: AtomicUsize::new(state.into()),
        }
    }
    fn load(&self, order: Ordering) -> ReadinessState {
        self.inner.load(order).into()
    }
    fn compare_and_swap(&self, current: ReadinessState, 
            new: ReadinessState, order: Ordering) -> ReadinessState {
        let success_order = order;
        let mut fail_order = order;
        if order == Release {
            fail_order = Relaxed;
        } else if order == AcqRel {
            fail_order = Acquire;
        }
        let res = self.inner.compare_exchange(current.into(), new.into(), 
                success_order, fail_order);
        match res {
            Ok(val) => val.into(),
            Err(val) => val.into(),
        }
    }
    fn flag_as_dropped(&self) -> bool {
        let prev: ReadinessState = self.inner.fetch_or(DROPPED_MASK | QUEUED_MASK, Release).into();
        debug_assert!(!prev.is_dropped());
        !prev.is_queued()
    }
}

struct ReadinessNode {  // 三剑客 + next指针 + queue
    state: AtomicState,
    token_0: UnsafeCell<TokenEntry>,
    token_1: UnsafeCell<TokenEntry>,
    token_2: UnsafeCell<TokenEntry>,
    next_readiness: AtomicPtr<ReadinessNode>,
    update_lock: AtomicBool,
    readiness_queue: AtomicPtr<()>,
    ref_count: AtomicUsize,
    job: Job,
}

enum Dequeue {
    Data(*mut ReadinessNode),
    Empty,
    Inconsistent,
}

fn enqueue_with_wakeup(queue: *mut(), node: &ReadinessNode) -> io::Result<()> {
    debug_assert!(!queue.is_null());
    let queue: &Arc<ReadinessQueueInner> = unsafe {
        //&*(&queue as *const *mut () )                                     // expected reference `&Arc<ReadinessQueueInner>` found reference `&*mut ()`
        &*(&queue as *const *mut () as * const Arc<ReadinessQueueInner>)
    };
    queue.enqueue_node_with_wakeup(node)                                    // 为何强转为&Arc<>类型?
}

impl ReadinessNode {
    fn new(queue: *mut(), token: TokenEntry, interest: Ready,                    // 表达的意思是queue内部可变(非const queue)
            opt: PollOpt, ref_count: usize) -> ReadinessNode {
        ReadinessNode {
            state: AtomicState::new(interest, opt),
            token_0: UnsafeCell::new(token),
            token_1: UnsafeCell::new(TokenEntry { token: Token(0), ttype: TokenType::TOKEN_EVENT} ),
            token_2: UnsafeCell::new(TokenEntry { token: Token(0), ttype: TokenType::TOKEN_EVENT} ),
            next_readiness: AtomicPtr::new(ptr::null_mut()),
            update_lock: AtomicBool::new(false),
            readiness_queue: AtomicPtr::new(queue),
            ref_count: AtomicUsize::new(ref_count),
            job: Arc::new(Mutex::new(move |val: i64| { println!("null ReadinessNode")})),
        }
    }
    fn marker() -> ReadinessNode {
        ReadinessNode {
            state: AtomicState::new(Ready::empty(), PollOpt::empty()),
            token_0: UnsafeCell::new(TokenEntry { token: Token(0), ttype: TokenType::TOKEN_EVENT}),
            token_1: UnsafeCell::new(TokenEntry { token: Token(0), ttype: TokenType::TOKEN_EVENT}),
            token_2: UnsafeCell::new(TokenEntry { token: Token(0), ttype: TokenType::TOKEN_EVENT}),
            next_readiness: AtomicPtr::new(ptr::null_mut()),
            update_lock: AtomicBool::new(false),
            readiness_queue: AtomicPtr::new(ptr::null_mut()),
            ref_count: AtomicUsize::new(0),
            job: Arc::new(Mutex::new(move |val: i64| { println!("null ReadinessNode")})),
        }
    }
    fn enqueue_with_wakeup(&self) -> io::Result::<()> { // node排入队列 队列一般是Poll中的
        let queue = self.readiness_queue.load(Acquire);
        if queue.is_null() {
            return Ok(())
        }
        enqueue_with_wakeup(queue, self)
    }
}

struct ReadinessQueueInner {    // 无锁队列 三剑客
    awakener: sys::Awakener,
    head_readiness: AtomicPtr<ReadinessNode>,
    tail_readiness: UnsafeCell<*mut ReadinessNode>,
    end_marker: Box<ReadinessNode>,
    sleep_marker: Box<ReadinessNode>,
    closed_marker: Box<ReadinessNode>,
}

fn release_node(ptr: *mut ReadinessNode) {
    unsafe {
        if (*ptr).ref_count.fetch_sub(1, AcqRel) != 1 {
            return;
        }
        let node = Box::from_raw(ptr);
        let queue = node.readiness_queue.load(Acquire);
        if queue.is_null() {
            return;
        }
        let _: Arc<ReadinessQueueInner> = mem::transmute(queue);    // 偷着引用计数-1
    }
}

impl ReadinessQueueInner {
    fn wakeup(&self) -> io::Result<()> {
        self.awakener.wakeup()
    }
    fn enqueue_node_with_wakeup(&self, node: &ReadinessNode) -> io::Result<()> {    // 跨线程
        if self.enqueue_node(node) {
            println!("need wakeup");
            self.wakeup()?;
        }
        Ok(())
    }
    fn enqueue_node(&self, node: &ReadinessNode) -> bool {
        let node_ptr = node as * const _ as * mut _;                // 拿到node裸指针
        node.next_readiness.store(ptr::null_mut(), Relaxed);
        unsafe {
            //let mut prev: *mut ReadinessNode  = self.head_readiness.load(Acquire); // OK 但是太繁琐了 直接用let自动推导
            let mut prev = self.head_readiness.load(Acquire);       // pub fn load(&self, order: Ordering) -> *mut T // 返回*mut ReadinessNode类型
                                                                    // 下面要改变prev 所以加上mut  加上的这个mut跟 *mut ReadinessNode中的mut不是一个意思
                                                                    // 这里加上mut后 即是mut *mut ReadinessNode
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
                let res = self.head_readiness.compare_exchange(prev, node_ptr, AcqRel, Acquire);
                match res {
                    Ok(_) => break,
                    Err(val) => prev = val,
                }
            }
            debug_assert!((*prev).next_readiness.load(Relaxed).is_null());
            (*prev).next_readiness.store(node_ptr, Release);        // 尾插 头在尾
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
            self.end_marker.next_readiness.store(ptr::null_mut(), Relaxed);
            let res = self.head_readiness.compare_exchange(sleep_marker, end_marker, AcqRel, Acquire);
            match res {
                Ok(val) => {
                    debug_assert!(val != end_marker);
                    *self.tail_readiness.get() = end_marker;
                },
                Err(val) => {
                    debug_assert!(val != end_marker);
                    return;
                },
            }
        }
    }
    unsafe fn dequeue_node(&self, until: *mut ReadinessNode) -> Dequeue {
        let mut tail = *(self.tail_readiness.get());    // pub const fn get(&self) -> *mut T // get得到的类型是*mut (*mut ReadinessNode)
                                                        // tail是 *mut ReadinessNode类型
        //let mut next = (tail).next_readiness.load(Acquire); // `(tail)` is a raw pointer; try dereferencing it:
        let mut next = (*tail).next_readiness.load(Acquire);    // pub fn load(&self, order: Ordering) -> *mut T
        if tail == self.end_marker() || tail == self.sleep_marker() || tail == self.closed_marker() {
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

#[derive(Clone)]
struct ReadinessQueue {
    inner: Arc<ReadinessQueueInner>,
}

unsafe impl Send for ReadinessQueue {}
unsafe impl Sync for ReadinessQueue {}
unsafe fn token(node: &ReadinessNode, pos: usize) -> TokenEntry {
    match pos {
        0 => *node.token_0.get(),
        1 => *node.token_1.get(),
        2 => *node.token_2.get(),
        _ => unreachable!(),
    }
}

impl ReadinessQueue {
    fn new() -> io::Result<ReadinessQueue> {    // 初始化无锁队列 + 三marker + poll
        is_send::<Self>();
        is_sync::<Self>();
        let end_marker = Box::new(ReadinessNode::marker());
        let sleep_marker = Box::new(ReadinessNode::marker());
        let closed_marker = Box::new(ReadinessNode::marker());
        let ptr = &(*end_marker) as *const _ as *mut _;
        Ok(ReadinessQueue {
            inner: Arc::new(ReadinessQueueInner {
                awakener: sys::Awakener::new()?,
                head_readiness: AtomicPtr::new(ptr),
                tail_readiness: UnsafeCell::new(ptr),
                end_marker,
                sleep_marker,
                closed_marker,
            })
        })
    }
    fn poll(&self, dst: &mut sys::Events) {     // 从无锁队列弹出一个节点 排入到events中
        let mut until = ptr::null_mut();
        if dst.len() == dst.capacity() {
            self.inner.clear_sleep_marker();
        }
        'outer:
        while dst.len() < dst.capacity() {
            let ptr = match unsafe { self.inner.dequeue_node(until) } {
                Dequeue::Empty | Dequeue::Inconsistent => break,
                Dequeue::Data(ptr) => ptr,
            };
            //let node = unsafe { &*ptr };
            let node = unsafe { &mut *ptr };
            let mut state = node.state.load(Acquire);
            let mut next;
            let mut readiness;
            let mut opt;
            loop {
                next = state;
                debug_assert!(state.is_queued());
                if state.is_dropped() {
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
                dst.push_event(Event::new(readiness, token), node.job.clone());
                node.job = Arc::new(Mutex::new(move |val: i64| { println!("deref ReadinessNode")}));
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
        if tail != end_marker {
            return false;
        }
        self.inner.sleep_marker.next_readiness.store(ptr::null_mut(), Relaxed);
        let res = self.inner.head_readiness.compare_exchange(end_marker, sleep_marker, AcqRel, Acquire);
        match res {
            Ok(val) => {
                debug_assert!(val != sleep_marker);
                debug_assert!(unsafe {*self.inner.tail_readiness.get() == end_marker});
                debug_assert!(self.inner.end_marker.next_readiness.load(Relaxed).is_null());
                unsafe { *self.inner.tail_readiness.get() = sleep_marker };
                true
            },
            Err(val) => {
                debug_assert!(val != sleep_marker);
                return false;
            },
        }
    }
}

impl Drop for ReadinessQueue {
    fn drop(&mut self) {
        self.inner.enqueue_node(&*self.inner.closed_marker);
        loop {
            let ptr = match unsafe { self.inner.dequeue_node(ptr::null_mut()) } {
                Dequeue::Empty => break,
                Dequeue::Inconsistent => {
                    continue;
                }
                Dequeue::Data(ptr) => ptr,
            };
            let node = unsafe { &*ptr };
            let state = node.state.load(Acquire);
            debug_assert!(state.is_queued());
            release_node(ptr);
        }
    }
}

pub struct Poll {
    selector: sys::Selector,
    readiness_queue: ReadinessQueue,    // poller + 无锁队列
    lock_state: AtomicUsize,
    lock: Mutex<()>,
    condvar: Condvar,
}

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}

pub fn selector(poll: &Poll) -> &sys::Selector {
    &poll.selector
}

thread_local! {
    pub static current_token_allocator: RefCell<Arc<Mutex<TokenAllocator>>> = panic!("!");
}

impl Poll {
    pub fn get_current_token_allocator() -> Arc<Mutex<TokenAllocator>> {
        let ptr = current_token_allocator.with(|allocator| -> *mut Arc<Mutex<TokenAllocator>> {return allocator.as_ptr()});
        unsafe {
            let clone = (*ptr).clone();
            clone
        }
    }
    pub fn new() -> io::Result<Poll> {
        is_send::<Poll>(); 
        is_sync::<Poll>(); 
        let poll = Poll {
            selector: sys::Selector::new()?,
            readiness_queue: ReadinessQueue::new()?,
            lock_state: AtomicUsize::new(0),
            lock: Mutex::new(()),
            condvar: Condvar::new(),
        };
        poll.readiness_queue.inner.awakener.register(
                &poll,
                TokenEntry {
                    ttype: TokenType::NOTIFY_EVENT, 
                    token: Token(0)
                }, 
                Ready::readable(),
                PollOpt::edge(),
                Arc::new(Mutex::new(move |val: i64| {}))
        )?;
        Ok(poll)
    }
    pub fn register<E: ?Sized>(&self, handle: &E, token: TokenEntry, interest: Ready, opts: PollOpt, job: Job) -> io::Result<()>
        where E: Evented
    {
        //validate_args(token.ttype)?;
        log::trace!("register poller");
        handle.register(self, token, interest, opts, job)?;      // 为何不直接用self.selector 要用参数handle的register?
                                                            // 依赖反转 只是提供一个接口而已 不同类型的Evented(eg: channel:ReceiverCtl eg: unix/eventedfd Io)有不同的register
        Ok(())
    }
    pub fn reregister<E: ?Sized>(&self, handle: &E, token: TokenEntry, interest: Ready, opts: PollOpt) -> io::Result<()>
        where E: Evented
    {
        //validate_args(token.ttype)?;
        log::trace!("reregister poller");
        handle.reregister(self, token, interest, opts)?;
        Ok(())
    }
    pub fn deregister<E: ?Sized>(&self, handle: &E) -> io::Result<()>
        where E: Evented
    {
        log::trace!("degister poller");
        handle.deregister(self)?;
        Ok(())
    }
    pub fn poll(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        self.poll1(events, timeout, false)
    }
    pub fn poll_interruptible(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        self.poll1(events, timeout, true)
    }
    fn poll2(&self, events: &mut Events, mut timeout: Option<Duration>, interruptible: bool) -> io::Result<usize> {
        if timeout == Some(Duration::from_millis(0)) {
        } else if self.readiness_queue.prepare_for_sleep() {
        } else {
            timeout = Some(Duration::from_millis(0))
        }
        loop {
            let now = Instant::now();
            let res = self.selector.select(&mut events.inner, timeout);
            match res {
                Ok(true) => {
                    self.readiness_queue.inner.awakener.cleanup();
                    break;
                }
                Ok(false) => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted && !interruptible => {
                    if let Some(to) = timeout {
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
        }
        self.readiness_queue.poll(&mut events.inner);
        //println!("after queue poll len: {}", events.inner.len());
        Ok(events.inner.len())
    }
    fn poll1(&self, events: &mut Events, mut timeout: Option<Duration>, interruptible: bool) -> io::Result<usize> {
        let zero = Some(Duration::from_millis(0));
        let mut curr = 0;
        let res = self.lock_state.compare_exchange(0, 1, SeqCst, SeqCst);
        match res {
            Ok(val) => curr = val,
            Err(val) => curr = val,
        }
        if 0 != curr {      // 其它线程已对poll上锁 
            let mut lock = self.lock.lock().unwrap();
            let mut inc = false;
            loop {                          // 这个loop确保polling 顺序执行
                if curr & 1 == 0 {
                    let mut next = curr | 1;
                    if inc {
                        next -= 2;
                    }
                    let mut actual = curr;
                    let res = self.lock_state.compare_exchange(curr, next, SeqCst, SeqCst);
                    match res {
                        Ok(val) => actual = val,
                        Err(val) => actual = val,
                    }
                    if actual != curr {
                        curr = actual;
                        continue;
                    }
                    break;              // 唯一出路
                }
                if timeout == zero {    // 是个很紧急的poll 但是拿不到锁 直接返回Ok // 这个设计不合理
                    if inc {
                        self.lock_state.fetch_sub(2, SeqCst);
                    }
                    return Ok(0);
                }
                if !inc {
                    let next = curr.checked_add(2).expect("overflow");
                    let mut actual = curr;
                    let res = self.lock_state.compare_exchange(curr, next, SeqCst, SeqCst);  // 确保每个线程按序 +2
                    match res {
                        Ok(val) => actual = val,
                        Err(val) => actual = val,
                    }
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
                        } else {    // 虚假超时
                            timeout = Some(to - elapsed);
                        }
                        l
                    }
                    None => {
                        self.condvar.wait(lock).unwrap()    // 4个线程 0线程一直polling 1~3线程wait
                    }
                };
                curr = self.lock_state.load(SeqCst);
            }
        }
        let ret = self.poll2(events, timeout, interruptible);
        if 1 != self.lock_state.fetch_and(!1, Release) {    // 老值不为1 说明polling有竞争 // lock_stat & fff1110 
            let _lock = self.lock.lock().unwrap();
            self.condvar.notify_one();
        }
        ret
    }

}

impl fmt::Debug for Poll {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Poll").finish()
    }
}

impl AsRawFd for Poll {
    fn as_raw_fd(&self) -> RawFd {
        self.selector.as_raw_fd()
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
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut JobEntry> {
        self.inner.get_mut(idx)
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

/*
impl fmt::Debug for Events<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Events")
                .field("capacity", &self.capacity())
                .finish()
    }
}

impl IntoIterator for Events<'_> {
    type Item = Event;
    type IntoIter<'a> = IntoIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self,
            pos: 0,
        }
    }
}

impl<'a> IntoIterator for &'a Events<'_> {
    type Item = Event;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = Event;
    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}

impl Iterator for IntoIter<'_> {
    type Item = Event;
    fn next(&mut self) -> Option<Event> {
        let ret = self.inner.inner.get(self.pos);
        self.pos += 1;
        ret
    }
}
*/

struct RegistrationInner {
    node: *mut ReadinessNode,
}

impl RegistrationInner {
    fn readiness(&self) -> Ready {
        self.state.load(Relaxed).readiness()
    }
    fn set_readiness(&self, ready: Ready) -> io::Result<()> {   // 重置当前节点状态 如果有感兴趣的事件到来则入队
        let mut state = self.state.load(Acquire);
        let mut next;
        loop {
            next = state;
            if state.is_dropped() {
                return Ok(());
            }
            next.set_readiness(ready);
            if !next.effective_readiness().is_empty() {
                next.set_queued(); // 如果next有事件到来  那么会排入队列中 在当poll时传出到event数组中
            }
            let actual = self.state.compare_and_swap(state, next, AcqRel);
            if state == actual {
                break;
            }
            state = actual;
        }
        if !state.is_queued() && next.is_queued() {
            self.enqueue_with_wakeup()?;
        }
        Ok(())
    }
    fn update(&self, poll: &Poll, token: TokenEntry, interest: Ready, opt: PollOpt, job: Job) -> io::Result<()> {
        let mut queue = self.readiness_queue.load(Relaxed);
        let other: &*mut () = unsafe {
            &*(&poll.readiness_queue.inner as *const _ as *const *mut()) // inner: Arc<ReadinessQueueInner>,
        };
        let other = *other;
        debug_assert!(mem::size_of::<Arc<ReadinessQueueInner>>() == mem::size_of::<*mut ()>());
        if queue.is_null() {
            let mut actual = other;
            let res = self.readiness_queue.compare_exchange(queue, other, Release, Relaxed);      // node中的queue 指向poll中的queue
            match res {
                Ok(val) => actual = val,
                Err(val) => actual = val,
            }
            if actual.is_null() {
                self.ref_count.fetch_add(1, Relaxed);
                mem::forget(poll.readiness_queue.clone());  // 平白无故让引用计数+1 // 为什么不设计一个强引用呢?
            } else {
                if actual != other {
                    return Err(io::Error::new(io::ErrorKind::Other, "Registration handle associated with another Poll instance"));
                }
            }
            queue = other;
        } else if queue != other {
            return Err(io::Error::new(io::ErrorKind::Other, "Registration handle associated with another Poll instance"));
        }
        unsafe {
            let actual = &poll.readiness_queue.inner as *const _ as *const usize;
            debug_assert_eq!(queue as usize, *actual);
        }
        if self.update_lock.compare_and_swap(false, true, Acquire) {
            return Ok(());
        }
        /*
        let res = self.update_lock.compare_and_swap(false, true, Acquire);
        match res {
            Ok(val) => return Ok(()),
            Err(val) => 
        }
        */
        let mut state = self.state.load(Relaxed);
        let mut next;
        let curr_token_pos = state.token_write_pos();
        let curr_token = unsafe { self::token(self, curr_token_pos) };
        let mut next_token_pos = curr_token_pos;
        if token != curr_token {
            next_token_pos = state.next_token_pos();
            match next_token_pos {
                0 => unsafe { *self.token_0.get() = token },
                1 => unsafe { *self.token_1.get() = token },
                2 => unsafe { *self.token_2.get() = token },
                _ => unreachable!(),
            }
        }
        loop {
            next = state;
            debug_assert!(!state.is_dropped());
            next.set_token_write_pos(next_token_pos);
            next.set_interest(interest);
            next.set_poll_opt(opt);
            if !next.effective_readiness().is_empty() {
                next.set_queued();
            }
            let actual = self.state.compare_and_swap(state, next, Release);             // 更新node中state的值
            if actual == state {
                break;
            }
            debug_assert_eq!(curr_token_pos, actual.token_write_pos());
            state = actual;
        }
        self.update_lock.store(false, Release);
        if !state.is_queued() && next.is_queued() {
            unsafe { 
                (*self.node).job = job;
            }
            enqueue_with_wakeup(queue, self)?;
            /*
            let res = enqueue_with_wakeup(queue, self);
            match res {
                Ok(_) => println!("enqueue with wakeup ok"),
                Err(e) => println!("wakeup failed"),
            };
            */
        }
        Ok(())
    }
}

impl ops::Deref for RegistrationInner {
    type Target = ReadinessNode;
    fn deref(&self) -> &ReadinessNode {
        unsafe { &*self.node }
    }
}

impl Clone for RegistrationInner {
    fn clone(&self) -> RegistrationInner {
        let old_size = self.ref_count.fetch_add(1, Relaxed);
        if old_size & !MAX_REFCOUNT != 0 {
            process::abort();
        }
        RegistrationInner {
            node: self.node,
        }
    }
}

impl Drop for RegistrationInner {
    fn drop(&mut self) {
        release_node(self.node);
    }
}

#[derive(Clone)]
pub struct SetReadiness {
    inner: RegistrationInner,
}

unsafe impl Send for SetReadiness {}
unsafe impl Sync for SetReadiness {}

pub struct Registration {
    inner: RegistrationInner,
}

impl SetReadiness {
    pub fn readiness(&self) -> Ready {
        self.inner.readiness()
    }
    pub fn set_readiness(&self, ready: Ready) -> io::Result<()> {
        self.inner.set_readiness(ready)
    }
}

impl fmt::Debug for SetReadiness {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SetReadiness").finish()
    }
}


unsafe impl Send for Registration {}
unsafe impl Sync for Registration {}

pub fn new_registration(poll: &Poll, token: TokenEntry, ready: Ready, opt: PollOpt) 
        -> (Registration, SetReadiness)
{
    Registration::new_priv(poll, token, ready, opt)
}

impl Registration {
    pub fn new2() -> (Registration, SetReadiness) {
        let node = Box::into_raw(Box::new(ReadinessNode::new(
                ptr::null_mut(), TokenEntry {token: Token(0), ttype: TokenType::TOKEN_EVENT}, Ready::empty(), PollOpt::empty(), 2)));
        let registration = Registration {
            inner: RegistrationInner {
                node,
            },
        };
        let set_readiness = SetReadiness {
            inner: RegistrationInner {
                node,
            },
        };
        (registration, set_readiness)
    }
    pub fn new(poll: &Poll, token: TokenEntry, interest: Ready, opt: PollOpt)
            -> (Registration, SetReadiness)
    {
        let (r, s) = Registration::new_priv(poll, token, interest, opt);
        (r, s)
    }
    fn new_priv(poll: &Poll, token: TokenEntry, interest: Ready, opt: PollOpt)
            -> (Registration, SetReadiness)
    {
        is_send::<Registration>();
        is_sync::<Registration>();
        is_send::<SetReadiness>();
        is_sync::<SetReadiness>();
        let queue = poll.readiness_queue.inner.clone();
        let queue1: *mut () = unsafe { mem::transmute(queue) };
        let node = Box::into_raw(Box::new(ReadinessNode::new(queue1, token, interest, opt, 3)));
        let registration = Registration {
            inner: RegistrationInner {
                node,
            },
        };
        let set_readiness = SetReadiness {
            inner: RegistrationInner {
                node,
            },
        };
        (registration, set_readiness)
    }
    pub fn update(&self, poll: &Poll, token: TokenEntry, interest: Ready, opt: PollOpt, job: Job) -> io::Result<()> {
        self.inner.update(poll, token, interest, opt, job)
    }
    pub fn deregister(&self, poll: &Poll) -> io::Result<()> {
        let job = Arc::new(Mutex::new(move |val: i64| { println!("null1 deregister for Registration") }));
        self.inner.update(poll, TokenEntry{ ttype: TokenType::TOKEN_EVENT, token: Token(0) }, Ready::empty(), PollOpt::empty(), job)
    }
}

impl Evented for Registration {
    fn register(&self, poll: &Poll, token: TokenEntry, interest: Ready, opts: PollOpt, job: Job) -> io::Result<()> {
        self.inner.update(poll, token, interest, opts, job)
    }
    fn reregister(&self, poll: &Poll, token: TokenEntry, interest: Ready, opts: PollOpt) -> io::Result<()> {
        let job = Arc::new(Mutex::new(move |val: i64| { println!("null reregister for Registration") }));
        self.inner.update(poll, token, interest, opts, job)
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        let job = Arc::new(Mutex::new(move |val: i64| { println!("null deregister for Registration") }));
        self.inner.update(poll, TokenEntry{ ttype: TokenType::TOKEN_EVENT, token: Token(0) }, Ready::empty(), PollOpt::empty(), job)
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if self.inner.state.flag_as_dropped() {
            let _ = self.inner.enqueue_with_wakeup();
        }
    }
}

impl fmt::Debug for Registration {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Registration").finish()
    }
}

