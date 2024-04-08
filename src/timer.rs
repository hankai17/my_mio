use {convert, io, Ready, Poll, PollOpt, Token, Registration, SetReadiness, Job};
use lazycell::LazyCell;
use slab::Slab;
use std::{cmp, fmt, u64, usize, iter, thread};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use event::Evented;

type Tick = u64;

pub struct Timeout {
    token: Token,
    tick: u64,
}

pub struct Builder {
    tick: Duration,
    num_slots: usize,
    capacity: usize,
}

impl Builder {
    pub fn tick_duration(mut self, duration: Duration) -> Builder {
        self.tick = duration;
        self
    }
    pub fn num_slots(mut self, num_slots: usize) -> Builder {
        self.num_slots = num_slots;
        self
    }
    pub fn capacity(mut self, capacity: usize) -> Builder {
        self.capacity = capacity;
        self
    }
    pub fn build<T>(self) -> Timer<T> {
        Timer::new (
            convert::millis(self.tick),
            self.num_slots,
            self.capacity,
            Instant::now()
        )
    }
}

impl Default for Builder {
    fn default() -> Builder {
        Builder {
            tick: Duration::from_millis(100),
            num_slots: 256,
            capacity: 65_536,
        }
    }
}

pub struct TimerError;

pub enum TimerErrorKind {
    TimerOverflow,
}

#[derive(Copy, Clone)]
struct EntryLinks {
    tick: Tick,
    prev: Token,
    next: Token,
}

struct Entry<T> {
    state: T,
    links: EntryLinks,
}

pub type Result<T> = ::std::result::Result<T, TimerError>;
pub type TimerResult<T> = Result<T>;

const EMPTY: Token = Token(usize::MAX);

impl<T> Entry<T> {
    fn new(state: T, tick: u64, next: Token) -> Entry<T> {
        Entry {
            state,
            links: EntryLinks {
                tick,
                prev: EMPTY,
                next,
            },
        }
    }
}

#[derive(Copy, Clone, Debug)]     // repeat need Clone
struct WheelEntry {
    next_tick: Tick,
    head: Token,
}

type WakeupState = Arc<AtomicUsize>;

struct Inner {
    registration:   Registration,
    set_readiness:  SetReadiness,
    wakeup_state:   WakeupState,
    wakeup_thread:  thread::JoinHandle<()>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Inner")
            .field("registration", &self.registration)
            .field("wakeup_state", &self.wakeup_state.load(Ordering::Relaxed))
            .finish()
    }
}

const TERMINATE_THREAD: usize = 0;
impl Drop for Inner {
    fn drop(&mut self) {
        self.wakeup_state.store(TERMINATE_THREAD, Ordering::Release);
        self.wakeup_thread.thread().unpark();
    }
}

pub struct Timer<T> {
    tick_ms:    u64,
    entries:    Slab<Entry<T>>,
    wheel:      Vec<WheelEntry>,
    start:      Instant,
    tick:       Tick,
    next:       Token,
    mask:       u64,
    inner:      LazyCell<Inner>,
}

fn duration_to_tick(elapsed: Duration, tick_ms: u64) -> Tick {      // elapsed是多少个tick_ms组成 向上取整
    let elapsed_ms = convert::millis(elapsed);
    elapsed_ms.saturating_add(tick_ms / 2) / tick_ms
}

fn current_tick(start: Instant, tick_ms: u64) -> Tick {
    duration_to_tick(start.elapsed(), tick_ms)
}

const TICK_MAX: Tick = u64::MAX;
impl<T> Timer<T> {
    fn new(tick_ms: u64, num_slots: usize, capacity: usize, start: Instant) -> Timer<T> {
        let num_slots = num_slots.next_power_of_two();              // 取2的幂 向上取
        let capacity = capacity.next_power_of_two();
        let mask = (num_slots as u64) - 1;
        let wheel = iter::repeat(WheelEntry { next_tick: TICK_MAX, head: EMPTY })
                .take(num_slots).collect();
        Timer {
            tick_ms,
            entries: Slab::with_capacity(capacity),
            wheel,
            start,
            tick: 0,
            next: EMPTY,
            mask,
            inner: LazyCell::new(),
        }
    }
    pub fn set_timeout(&mut self, delay_from_now: Duration, state: T) -> Result<Timeout> {
        let delay_from_start = self.start.elapsed() + delay_from_now;
        self.set_timeout_at(delay_from_start, state)
    }
    fn set_timeout_at(&mut self, delay_from_start: Duration, state: T) -> Result<Timeout> {
        let mut tick = duration_to_tick(delay_from_start, self.tick_ms);
        println!("setting timeout; delay: {:?}; tick: {:?}; current-tick: {:?}", 
                delay_from_start, tick, self.tick);
        if tick < self.tick {               // 最小定时个数为self.tick
            tick = self.tick + 1;
        }
        self.insert(tick, state)
    }
    fn insert(&mut self, tick: Tick, state: T) -> Result<Timeout> {
        let slot = (tick & self.mask) as usize;
        let curr = self.wheel[slot];
        let entry = Entry::new(state, tick, curr.head);
        let token = Token(self.entries.insert(entry));
        if curr.head != EMPTY {
            self.entries[curr.head.into()].links.prev = token;      // 前插法 // 只用到了prev指针
        }
        self.wheel[slot] = WheelEntry {
            next_tick: cmp::min(tick, curr.next_tick),
            head: token,
        };
        self.schedule_readiness(tick);
        println!("insert timeout; slot: {}; token: {:?}", slot, token);
        Ok(Timeout {
            token,
            tick
        })
    }
    pub fn cancel_timeout(&mut self, timeout: &Timeout) -> Option<T> {
        let links = match self.entries.get(timeout.token.into()) {
            Some(e) => e.links,
            None => return None
        };
        if links.tick != timeout.tick {
            return None;
        }
        self.unlink(&links, timeout.token);
        Some(self.entries.remove(timeout.token.into()).state)
    }
    pub fn poll(&mut self) -> Option<T> {
        let target_tick = current_tick(self.start, self.tick_ms);
        self.poll_to(target_tick)
    }
    fn poll_to(&mut self, mut target_tick: Tick) -> Option<T> {
        println!("tick_to; target_tick: {}; current_tick: {}", 
                target_tick, self.tick);
        if target_tick < self.tick {
            target_tick = self.tick;
        }
        while self.tick <= target_tick {
            let curr = self.next;
            println!("ticking; curr: {:?}", curr);
            if curr == EMPTY {
                self.tick += 1;
                let slot = self.slot_for(self.tick);        // 依次遍历槽位
                self.next = self.wheel[slot].head;          // hankai1 self.next指向槽位头节点
                if self.next == EMPTY {
                    self.wheel[slot].next_tick = TICK_MAX;
                }
            } else {
                let slot = self.slot_for(self.tick);
                if curr == self.wheel[slot].head {
                    self.wheel[slot].next_tick = TICK_MAX;  // hankai2 重置槽位头 为TICK_MAX
                }
                let links = self.entries[curr.into()].links;// 拿到头节点的links
                if links.tick <= self.tick {                // 真过期
                    println!("triggering; token: {:?}", curr);
                    self.unlink(&links, curr);
                    return Some(self.entries.remove(curr.into()).state);
                } else {                                    // 假过期 取下一个
                    let next_tick = self.wheel[slot].next_tick;
                    self.wheel[slot].next_tick = cmp::min(next_tick, links.tick);
                    self.next = links.next;
                }
            }
        }
        if let Some(inner) = self.inner.borrow() {
            println!("unsetting readiness");
            let _ = inner.set_readiness.set_readiness(Ready::empty());
            if let Some(tick) = self.next_tick() {
                self.schedule_readiness(tick);
            }
        }
        None
    }
    fn unlink(&mut self, links: &EntryLinks, token: Token) {
        println!("unlinking timeout; slot: {}; token: {:?}", 
                self.slot_for(links.tick), token);
        if links.prev == EMPTY {
            let slot = self.slot_for(links.tick);
            self.wheel[slot].head = links.next;
        } else {
            self.entries[links.prev.into()].links.next = links.next;
        }
        if links.next != EMPTY {
            self.entries[links.next.into()].links.prev = links.prev;
            if token == self.next {
                self.next = links.next;
            }
        } else if token == self.next {
            self.next = EMPTY;
        }
    }
    fn schedule_readiness(&self, tick: Tick) {              // 传参为 定时器唤醒的时间  // 确保传入的时间要小于stat定时时间
        if let Some(inner) = self.inner.borrow() {
            let mut curr = inner.wakeup_state.load(Ordering::Acquire);
            loop {
                if curr as Tick <= tick {
                    return;
                }
                println!("advancing the wakeup time; target: {}; curr: {}", tick, curr);
                let actual = inner.wakeup_state.compare_and_swap(curr, tick as usize, Ordering::Release);
                if actual == curr {
                    println!("unparking wakeup thread");
                    inner.wakeup_thread.thread().unpark();  // 如果传入的时间小于stat定时时间 则唤醒线程
                    return;
                }
                curr = actual;
            }
        }
    }
    fn next_tick(&self) -> Option<Tick> {
        if self.next != EMPTY {
            let slot = self.slot_for(self.entries[self.next.into()].links.tick);
            if self.wheel[slot].next_tick == self.tick {
                return Some(self.tick);
            }
        }
        self.wheel.iter().map(|e| e.next_tick).min()
    }
    fn slot_for(&self, tick: Tick) -> usize {
        (self.mask & tick) as usize
    }
}

impl<T> Default for Timer<T> {
    fn default() -> Timer<T> {
        Builder::default().build()
    }
}

fn spawn_wakeup_thread(state: WakeupState, set_readiness: SetReadiness, 
        start: Instant, tick_ms: u64) -> thread::JoinHandle<()> {                       // 不断判断 stat定时器 是否到期 // 到期则入队
    thread::spawn(move || {
        let mut sleep_until_tick = state.load(Ordering::Acquire) as Tick;
        loop {
            if sleep_until_tick == TERMINATE_THREAD as Tick {
                return;
            }
            let now_tick = current_tick(start, tick_ms);
            println!("wakeup thread, sleep_until_tick: {:?}; now_tick: {:?}", sleep_until_tick, now_tick);
            if now_tick < sleep_until_tick {                                            // 定时未到 睡眠
                match tick_ms.checked_mul(sleep_until_tick - now_tick) {
                    Some(sleep_duration) => {
                        println!("sleeping, tick_ms: {}; now_tick: {}; sleep_until_tick: {}; duration: {:?}",
                                tick_ms, now_tick, sleep_until_tick, sleep_duration);
                        thread::park_timeout(Duration::from_millis(sleep_duration));    // 定时器定多久 线程就睡眠多久
                    }
                    None => {
                        println!("sleeping, tick_ms: {}; now_tick: {}; blocking sleep",
                                tick_ms, now_tick);
                        thread::park();
                    }
                }
                sleep_until_tick = state.load(Ordering::Acquire) as Tick;               // 定时到 或 被唤醒 // 更新睡眠时间
            } else {                                                                    // 定时到
                let actual = state.compare_and_swap(sleep_until_tick as usize, usize::MAX, Ordering::AcqRel) as Tick;
                if actual == sleep_until_tick {
                    println!("setting readiness from wakeup thread");
                    let _ = set_readiness.set_readiness(Ready::readable());             // 定时到 入队
                    sleep_until_tick = usize::MAX as Tick;
                } else {
                    sleep_until_tick = actual as Tick;
                }
            }
        }
    })
}

impl<T> Evented for Timer<T> {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt, job: Job) -> io::Result<()> {
        if self.inner.borrow().is_some() {
            return Err(io::Error::new(io::ErrorKind::Other, "timer alreay registered"));
        }
        let (registration, set_readiness) = Registration::new(poll, token, interest, opts);
        let wakeup_state = Arc::new(AtomicUsize::new(usize::MAX));
        let thread_handle = spawn_wakeup_thread(
            wakeup_state.clone(),
            set_readiness.clone(),
            self.start,
            self.tick_ms
        );
        self.inner.fill(Inner {
            registration,
            set_readiness,
            wakeup_state,
            wakeup_thread: thread_handle,
        }).expect("timer already registed");
        if let Some(next_tick) = self.next_tick() {
            self.schedule_readiness(next_tick);
        }
        Ok(())
    }
    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        match self.inner.borrow() {
            Some(inner) => inner.registration.update(poll, token, interest, opts),
            None => Err(io::Error::new(io::ErrorKind::Other, "receiver not register")),
        }
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        match self.inner.borrow() {
            Some(inner) => inner.registration.deregister(poll),
            None => Err(io::Error::new(io::ErrorKind::Other, "receiver not register")),
        }
    }
}

// 设计理念是 首先有一个最小定时器 stat 用于记录最接近的要发生的定时器时间 仅仅用于readiness触发es
// 触发后 则进行具体的poll_to

