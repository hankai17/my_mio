use lazycell::LazyCell;
use slab::Slab;
use std::{cmp, fmt, u64, usize, iter, thread};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use event::Evented;
use {convert, io, Ready, Poll, PollOpt, Token, Registration, SetReadiness, Job, TokenEntry};

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

#[derive(Copy, Clone, Debug)]
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

fn duration_to_tick(elapsed: Duration, tick_ms: u64) -> Tick {                  // elapsed是多少个tick_ms组成 向上取整
    let elapsed_ms = convert::millis(elapsed);
    elapsed_ms.saturating_add(tick_ms / 2) / tick_ms
}

fn current_tick(start: Instant, tick_ms: u64) -> Tick {
    duration_to_tick(start.elapsed(), tick_ms)
}

const TICK_MAX: Tick = u64::MAX;
impl<T> Timer<T> {
    fn new(tick_ms: u64, num_slots: usize, capacity: usize,
            start: Instant) -> Timer<T> {
        let num_slots = num_slots.next_power_of_two();                          // 取2的幂 向上取
        let capacity = capacity.next_power_of_two();
        let mask = (num_slots as u64) - 1;
        let wheel = iter::repeat(WheelEntry {                                   // 迭代器组合子链式调用: repeat是无限调用 taken(n)是取前n个 collect为收集成集合类型
            next_tick: TICK_MAX,
            head: EMPTY
        }).take(num_slots).collect();
        Timer {
            tick_ms,
            entries: Slab::with_capacity(capacity),
            wheel,
            start,
            tick: 0,                                                            // 从定时器开始 到现在经过了多少tick
            next: EMPTY,
            mask,
            inner: LazyCell::new(),
        }
    }

    pub fn set_timeout(&mut self, delay_from_now: Duration,
            state: T) -> Result<Timeout> {
        let delay_from_start = self.start.elapsed() + delay_from_now;           // elapsed(): 从start到现在过了多久
        self.set_timeout_at(delay_from_start, state)
    }

    fn set_timeout_at(&mut self, delay_from_start: Duration,
            state: T) -> Result<Timeout> {
        let mut tick = duration_to_tick(delay_from_start, self.tick_ms);
        if tick < self.tick {                                                   // 最小定时个数为self.tick
            tick = self.tick + 1;
        }
        self.insert(tick, state)
    }

    fn insert(&mut self, tick: Tick, state: T) -> Result<Timeout> {             // hankai2 添加一个定时器 返回定时器句柄
        let slot = (tick & self.mask) as usize;
        let curr = self.wheel[slot];
        let entry = Entry::new(state, tick, curr.head);                         // next
        let token = Token(self.entries.insert(entry));                          // slab.insert 返回的是 新插入元素在 Slab 中的索引位置
        if curr.head != EMPTY {
            self.entries[curr.head.into()].links.prev = token;                  // prev
        }
        self.wheel[slot] = WheelEntry {
            next_tick: cmp::min(tick, curr.next_tick),
            head: token,
        };
        self.schedule_readiness(tick);                                          // hankai2.1 可能立即唤醒线程 线程中让node入队
        Ok(
            Timeout {
                token,
                tick
            }
        )
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

    fn poll_to(&mut self, mut target_tick: Tick) -> Option<T> {                 // hankai2.2 如约唤醒
        if target_tick < self.tick {
            target_tick = self.tick;
        }
        while self.tick <= target_tick {
            let curr = self.next;
            if curr == EMPTY {
                self.tick += 1;
                let slot = self.slot_for(self.tick);                            // 依次遍历槽位
                self.next = self.wheel[slot].head;                              // 1 self.next指向槽位头节点
                if self.next == EMPTY {
                    self.wheel[slot].next_tick = TICK_MAX;
                }
            } else {
                let slot = self.slot_for(self.tick);
                if curr == self.wheel[slot].head {
                    self.wheel[slot].next_tick = TICK_MAX;                      // 2 重置槽位头 为TICK_MAX
                }
                let links = self.entries[curr.into()].links;                    // 拿到头节点的links
                if links.tick <= self.tick {                                    // 真过期
                    self.unlink(&links, curr);
                    return Some(self.entries.remove(curr.into()).state);
                } else {                                                        // 假过期 取下一个
                    let next_tick = self.wheel[slot].next_tick;
                    self.wheel[slot].next_tick = cmp::min(next_tick,
                            links.tick);
                    self.next = links.next;
                }
            }
        }
        if let Some(inner) = self.inner.borrow() {
            let _ = inner.set_readiness.set_readiness(Ready::empty());
            if let Some(tick) = self.next_tick() {
                self.schedule_readiness(tick);
            }
        }
        None
    }

    fn unlink(&mut self, links: &EntryLinks, token: Token) {
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

    fn schedule_readiness(&self, tick: Tick) {                      // 传参为 定时器要唤醒的时间 确保此值需小于stat定时时间 // hankai1.1
        if let Some(inner) = self.inner.borrow() {
            let mut curr = inner
                            .wakeup_state
                            .load(Ordering::Acquire);
            loop {
                if curr as Tick <= tick {
                    return;
                }
                let res = inner.wakeup_state.compare_exchange(curr, tick as usize,
                        Ordering::Release, Ordering::Relaxed);
                match res {
                    Ok(_) => {
                        inner.wakeup_thread.thread().unpark();      // 如果传入的时间小于stat定时时间 则唤醒线程
                        return;
                    },
                    Err(v) => curr = v,
                }
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
        self.wheel.iter().map(|e| e.next_tick).min()                // 遍历所有桶 拿到最小值
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

fn spawn_wakeup_thread(state: WakeupState, s: SetReadiness, 
        start: Instant, tick_ms: u64) -> thread::JoinHandle<()> {                       // 线程循环判断 stat定时器 到期则入队 // hankai0
    thread::Builder::new()
    .name("timer".to_string())
    .spawn(move || {
        let mut sleep_until_tick = state.load(Ordering::Acquire) as Tick;
        loop {
            if sleep_until_tick == TERMINATE_THREAD as Tick {
                return;
            }
            let now_tick = current_tick(start, tick_ms);
            if now_tick < sleep_until_tick {                                            // 定时未到睡眠
                match tick_ms.checked_mul(sleep_until_tick - now_tick) {
                    Some(sleep_duration) => {
                        thread::park_timeout(Duration::from_millis(sleep_duration));    // 定时器定多久 线程就睡眠多久
                    }
                    None => {
                        thread::park();
                    }
                }
                sleep_until_tick = state.load(Ordering::Acquire) as Tick;               // 定时到 或 被唤醒 // 更新睡眠时间
            } else {                                                                    // 定时到
                let res = state.compare_exchange(sleep_until_tick as usize,
                        usize::MAX, Ordering::AcqRel, Ordering::Acquire);
                match res {
                    Ok(_) => {
                        let _ = s.set_readiness(Ready::readable());                     // 定时到 入队
                        sleep_until_tick = usize::MAX as Tick;
                    },
                    Err(v) => sleep_until_tick = v as Tick,
                }
            }
        }
    }).unwrap()
}

impl<T> Evented for Timer<T> {
    #[allow(unused_variables)]
    fn register(&self, poll: &Poll, token: TokenEntry, interest: Ready,
            opts: PollOpt, job: Job) -> io::Result<()> {
        if self.inner.borrow().is_some() {
            return Err(io::Error::new(io::ErrorKind::Other,
                    "timer alreay registered"));
        }
        let (r, s) = Registration::new(poll, token, interest, opts);
        let wakeup_state = Arc::new(AtomicUsize::new(usize::MAX));
        let thread_handle = spawn_wakeup_thread(                                        // hankai0
            wakeup_state.clone(),
            s.clone(),
            self.start,
            self.tick_ms
        );
        self.inner.fill(Inner {                                                         // hankai1 分配r/s 初始化inner
            registration: r,
            set_readiness: s,
            wakeup_state,
            wakeup_thread: thread_handle,
        }).expect("timer already registed");
        if let Some(next_tick) = self.next_tick() {
            self.schedule_readiness(next_tick);
        }
        Ok(())
    }

    fn reregister(&self, poll: &Poll, token: TokenEntry, interest: Ready,
            opts: PollOpt) -> io::Result<()> {
        match self.inner.borrow() {
            Some(inner) => inner.registration.update(poll, token, interest, opts, 
                Arc::new(Mutex::new(move |_| {
                    println!("null reregister for Timer")
                }))
            ),
            None => Err(io::Error::new(io::ErrorKind::Other,
                    "receiver not register")),
        }
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        match self.inner.borrow() {
            Some(inner) => inner.registration.deregister(poll),
            None => Err(io::Error::new(io::ErrorKind::Other,
                    "receiver not register")),
        }
    }
}

