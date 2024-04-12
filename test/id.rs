
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};

#[derive(Copy, Clone)]
pub enum TokenType {
    SOCKET_EVENT,
    NOTIFY_EVENT,
    TIMERS_EVENT,
    JOBS_EVENT,
}

#[derive(Copy, Clone)]
pub struct TokenEntry {
    ttype: TokenType,
    token: usize,
}

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

fn main() {
    let mut id = IdAllocator::new();
    let mut i = id.alloc();
    println!("i: {}", i);
    i = id.alloc();
    println!("i: {}", i);
    
}

