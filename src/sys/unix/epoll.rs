#![allow(deprecated)]
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::time::Duration;
use std::{cmp, i32};

use libc::{c_int};
use libc::{EPOLLERR, EPOLLHUP, EPOLLONESHOT};
use libc::{EPOLLET, EPOLLOUT, EPOLLIN, EPOLLPRI}; // define in /usr/include/sys/epoll.h 

use {io, Ready, PollOpt, Job, JobEntry, TokenEntry, TokenType, JobState};
use event_imp::{Event};
use sys::unix::{cvt, UnixReady};
use sys::unix::io::set_cloexec;
use std::collections::HashMap;
use log::{debug, error};

static NEXT_ID: AtomicUsize = ATOMIC_USIZE_INIT;

pub struct Selector {
    id: usize,
    epfd: RawFd,
    events_map: Arc<Mutex<HashMap<i32, JobEntry>>>,                     // 重构思路: 外层用Selector时用Arc包装 这样多线程可以共享引用
}                                                                       //   events_map 用Mutex<hash>   这样多线程共享引用只有一个线程可以lock

impl Selector {
    pub fn new() -> io::Result<Selector> {
        let epfd = unsafe {
            dlsym!(fn epoll_create1(c_int) -> c_int);
            match epoll_create1.get() {
                Some(epoll_create1_fn) => {
                    cvt(epoll_create1_fn(libc::EPOLL_CLOEXEC))?         // 要么a)提取T的实例然后继续执行该函数 
                }                                                       // 要么b)退出整个函数并返Result<_, ::io>错误
                None=> {
                    let fd = cvt(libc::epoll_create(1024))?;            // 只能用在返回 Result 或 Option 的函数里
                    drop(set_cloexec(fd));
                    fd
                }
            }
        };
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(Selector {
            id: id,
            epfd: epfd,
            events_map: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn id(&self) -> usize { self.id }

    pub fn events_map(&self) -> *mut Arc<Mutex<HashMap<i32, JobEntry>>> {
        &self.events_map as *const Arc<Mutex<HashMap<i32, JobEntry>>>  as *mut Arc<Mutex<HashMap<i32, JobEntry>>>
    }

	pub fn with_events_map<F, R>(&self, f: F) -> R 
    where F: FnOnce(&mut HashMap<i32, JobEntry>) -> R, {
        let mut map = self.events_map.lock().unwrap();  // 或 expect
        f(&mut *map)
    }

    pub fn with_events_map_readonly<F, R>(&self, f: F) -> R 
    where F: FnOnce(&HashMap<i32, JobEntry>) -> R, {
        let map = self.events_map.lock().unwrap();
        f(&*map)
    }

    pub fn select(&self, evts: &mut Events, timeout: Option<Duration>) -> io::Result<bool> {    // rust的面向对象强制开发者在定义方法时就思考资源的访问方式 这里就是不可变借用
        let mut notify_idx = 0;
        let timeout_ms = timeout
                .map(|to| cmp::min(millis(to), i32::MAX as u64) as i32)
                .unwrap_or(-1);
        let mut has_notify = false;
        evts.clear();
        debug!("epoll_wait timeout: {:?}", timeout_ms);
        let cnt = unsafe {
            let ret = cvt(libc::epoll_wait(self.epfd,
                            evts.events.as_mut_ptr(),
                            evts.events.capacity() as i32,
                            timeout_ms))?;
            let cnt = ret as usize;
            evts.events.set_len(cnt);
            cnt
        };
		self.with_events_map(|map| {            // 加锁时机太长
    		for i in 0..cnt {
    		    let fd = evts.events[i].u64 as i32;
    		    match map.get_mut(&fd) {
    		        Some(job_entry) => {
    		            let token = job_entry.token_entry;
    		            if token.ttype == TokenType::NotifyEvent {
    		                notify_idx = i;
    		                has_notify = true;
    		                continue;
    		            }

    		            job_entry.ready = evts.get_ready(i).unwrap();
    		            evts.entries.push(job_entry.clone());
    		        }
    		        None => {
    		            error!("event_map get None, fd: {}", fd);
    		            // assert_eq!(0, 1);
    		            continue;
    		        }
    		    }
    		}
		});
        if has_notify {
            evts.events.remove(notify_idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn register(&self, fd: RawFd, token: TokenEntry, interests: Ready,
            opts: PollOpt, job: Job) -> io::Result<()> {
        debug!("register1 fd: {:?} len: {} token: {:?}", fd, self.events_map.lock().unwrap().len(), token.token);
        let mut info = libc::epoll_event {
            events: ioevent_to_epoll(interests, opts),
            u64: fd as u64
        };
        unsafe {
            self.with_events_map(|map| map.insert(
                fd as i32,
                JobEntry {
                    token_entry: token,
                    job,
                    ready: Ready::empty(),
                    state: Arc::new(Mutex::new(JobState::INIT))
                }
            ));
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut info))?;
            debug!("register2 fd: {:?} len: {} token: {:?}", fd, self.events_map.lock().unwrap().len(), token.token);
            Ok(())
        }
    }

    pub fn reregister(&self, fd: RawFd, token: TokenEntry, interests: Ready,
            opts: PollOpt) -> io::Result<()> {
        debug!("reregister {:?}", fd);
        let t = token.token;
        let mut info = libc::epoll_event {
            events: ioevent_to_epoll(interests, opts),
            u64: usize::from(t) as u64
        };
        unsafe {
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut info))?;
            Ok(())
        }
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        debug!("deregister1 fd: {:?} len: {} ", fd, self.events_map.lock().unwrap().len());
        let mut info = libc::epoll_event {
            events: 0,
            u64: 0,
        };
        unsafe {
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, &mut info))?;
            let v = self.with_events_map(|map| map.remove(&fd as &i32));
            debug!("deregister2 fd: {:?} len: {} token: {:?}",
                    fd, self.events_map.lock().unwrap().len(), v.unwrap().token_entry.token);
            Ok(())
        }
    }
}

fn ioevent_to_epoll(interest: Ready, opts: PollOpt) -> u32 {
    let mut kind = 0;
    if interest.is_readable() {
        kind |= EPOLLIN;
    }
    if interest.is_writable() {
        kind |= EPOLLOUT;
    }
    if UnixReady::from(interest).is_priority() {
        kind |= EPOLLPRI;
    }
    if opts.is_edge() {
        kind |= EPOLLET;
    }
    if opts.is_level() {
        kind &= !EPOLLET;
    }
    if opts.is_oneshot() {
        kind |= EPOLLONESHOT;
    }
    kind as u32
}

impl AsRawFd for Selector {
    fn as_raw_fd(&self) -> RawFd { self.epfd }
}

impl Drop for Selector {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::close(self.epfd);
        }
    }
}

pub struct Events {
    events: Vec<libc::epoll_event>, // fd
    entries: Vec<JobEntry>, // <JobEntry>
}

impl Events {
    pub fn with_capacity(u: usize) -> Events {
        Events {
            events: Vec::with_capacity(u),
            entries: Vec::with_capacity(u),
        }
    }
    pub fn len(&self) -> usize { self.entries.len() }
    pub fn capacity(&self) -> usize { self.entries.capacity() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
    pub fn get_ready(&self, idx: usize) -> Option<Ready> {
        self.events.get(idx).map(|event| {
            let epoll = event.events as c_int;
            let mut kind = Ready::empty();
            if (epoll & EPOLLIN) != 0 {
                kind = kind | Ready::readable();
            }
            if (epoll & EPOLLOUT) != 0 {
                kind = kind | Ready::writable()
            }
            if (epoll & EPOLLPRI) != 0 {
                kind = kind | Ready::readable() | UnixReady::priority();
            }
            if (epoll & EPOLLERR) != 0 {
                kind = kind | UnixReady::error()
            }
            if (epoll & EPOLLHUP) != 0 {
                kind = kind | UnixReady::hup()
            }
            kind
        })
    }
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut JobEntry> {
        match self.entries.get_mut(idx) {
            Some(v) => Some(v),
            None => None,
        }
    }
    pub fn push_event(&mut self, event: Event, job: Job) {
        self.events.push(
            libc::epoll_event {
                    events: ioevent_to_epoll(event.readiness(), PollOpt::empty()),
                    u64: usize::from(event.token().token) as u64
        });
        self.entries.push(
            JobEntry {
                token_entry: event.token(),
                ready: Ready::empty(),
                job,
                state: Arc::new(Mutex::new(JobState::INIT))
            }
        );
    }
    pub fn clear(&mut self) {
        unsafe {
            self.events.set_len(0); 
            self.entries.clear();
            //self.entries.set_len(0);
        }
    }
}

const NANOS_PER_MILLI: u32 = 1_000_000;
const MILLIS_PER_SEC: u64 = 1_000;

pub fn millis(duration: Duration) -> u64 {
    let millis = (duration.subsec_nanos() + NANOS_PER_MILLI - 1) / NANOS_PER_MILLI;
    duration.as_secs().saturating_mul(MILLIS_PER_SEC).saturating_add(millis as u64)
}

