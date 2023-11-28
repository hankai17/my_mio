use {Token, Registration, SetReadiness};
use lazycell::LazyCell;
use slab::Slab;
use std::{cmp, error, u64, usize, thread};
use std::time::{Duration, Instant};

type Tick = u64;

struct EntryLinks {
    tick: Tick,
    prev: Token,
    next: Token,
}

struct Entry<T> {
    state: T,
    links: EntryLinks,
}

const EMPTY: Token = Token(usize::MAX)
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

struct WheelEntry {
    next_tick: Tick,
    head: Token,
}

type WakeupState = Arc<AtomicUsize>;

struct Inner {
    registration: Registration,
    set_readiness: SetReadiness,
    wakup_state: WakeupState,
    wakup_thread: thread::JoinHandle<()>,
}

const TERMINATE_THREAD: usize = 0;
impl Drop for Inner {
    fn drop(&mut self) {
        self.wakeup_state.store(TERMINATE_THREAD, Ordering::Release);
        self.wakeup_thread.thread().unpark();
    }
}

pub struct Timer<T> {
    tick_ms: u64,
    entries: Slab<Entry<T>>,
    wheel: Vec<WheelEntry>,
    start: Instant,
    tick: Tick,
    next: Token,
    mask: u64,
    inner: LazyCell<Inner>,
}

const TICK_MAX: Tick = u64:MAX;
impl<T> Timer<T> {
    fn new(tick_ms: u64, num_slots: usize, capacity: usize, start: Instant) -> Timer<T> {
        let num_slots = num_slots.next_power_of_two();
        let capacity = capacity.next_power_of_two();
        let mask = (num_slots as u64) - 1;
        let wheel = iter::repeat(WheelEntry { next_tick: TICK_MAX, head: EMPTY })
            .take(num_slots).collect();
        Timer {
            tick_ms,
            entries: Slat::with_capacity(capacity),
            wheel,
            start,
            tick: 0,
            next: EMPTY,
            mask,
            inner: LazyCell::new(),
        }
    }
    pub fn set_timeout(&mut self, delay_from_start: Duration, state: T) -> Result<Timeout> {
    }
    fn set_timeout_at(&mut self, delay_from_start: Duration, state: T) -> Result<Timeout> {
    }
    fn insert(&mut self, tick: Tick, state: T) -> Result<Timeout> {
    }
    pub fn cancel_timeout(&mut self, timeout: &Timeout) -> Option<T> {
    }
    pub fn poll(&mut self) -> Option<T> {
    }
    fn poll_to(&mut self, mut target_tick: Tick) -> Option<T> {
    }
    fn unlink(&mut self, links: &EntryLinks, token: Token) {
    }
    fn schedule_readiness(&self, tick: Tick) {
    }
    fn next_tick(&self) -> Option<Tick> {
    }
    fn slot_for(&self, tick: Tick) -> usize {
    }
}

