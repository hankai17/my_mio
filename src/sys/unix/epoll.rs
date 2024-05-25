#![allow(deprecated)]
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::time::Duration;
use std::{cmp, i32};

use libc::{c_int};
use libc::{EPOLLERR, EPOLLHUP, EPOLLONESHOT};
use libc::{EPOLLET, EPOLLOUT, EPOLLIN, EPOLLPRI}; // define in /usr/include/sys/epoll.h 

//pub use poll::{TokenAllocator, TokenType};

use {io, Ready, PollOpt, Job, JobEntry, TokenEntry, TokenType};
use event_imp::{Event};
use sys::unix::{cvt, UnixReady};
use sys::unix::io::set_cloexec;
use std::collections::HashMap;
 use log::debug;

static NEXT_ID: AtomicUsize = ATOMIC_USIZE_INIT;

pub struct Selector {
    id: usize,
    epfd: RawFd,
    events_map: HashMap<i32, JobEntry>,
}

impl Selector {
    pub fn new() -> io::Result<Selector> {
        let epfd = unsafe {
            dlsym!(fn epoll_create1(c_int) -> c_int);
            match epoll_create1.get() {
                Some(epoll_create1_fn) => {
                    cvt(epoll_create1_fn(libc::EPOLL_CLOEXEC))?
                }
                None=> {
                    let fd = cvt(libc::epoll_create(1024))?;
                    drop(set_cloexec(fd));
                    fd
                }
            }
        };
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(Selector {
            id: id,
            epfd: epfd,
            events_map: HashMap::new(),
        })
    }
    pub fn id(&self) -> usize { self.id }
    pub fn select(&self, evts: &mut Events, timeout: Option<Duration>) -> io::Result<bool> {
        let mut notify_idx = 0;
        let timeout_ms = timeout
                .map(|to| cmp::min(millis(to), i32::MAX as u64) as i32)
                .unwrap_or(-1);
        let mut has_notify = false;
        let events_map = self.events_map();
        evts.clear();
        unsafe {
            let cnt = cvt(libc::epoll_wait(self.epfd,
                                            evts.events.as_mut_ptr(),
                                            evts.events.capacity() as i32,
                                            timeout_ms))?;
            let cnt = cnt as usize;
            evts.events.set_len(cnt);
            for i in 0..cnt {
                let fd = evts.events[i].u64 as usize as i32;
                //let entry: &mut JobEntry = events_map.as_mut().unwrap().get_mut(&fd).unwrap();
                let entry: &mut JobEntry = match events_map.as_mut().unwrap().get_mut(&fd) {
                    Some(entry) => entry,
                    None => {
                        debug!("fd: {}, None", fd);
                        assert_eq!(0, 1);
                        continue;
                    },
                };
                let token = entry.token_entry;
                if token.ttype == TokenType::NotifyEvent {
                    notify_idx = i;
                    has_notify = true;
                    continue;
                }
                entry.ready = evts.get_ready(i).unwrap();
                evts.entries.push(entry.clone());
            }
        }
        if has_notify {
            evts.events.remove(notify_idx);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub fn events_map(&self) -> *mut HashMap<i32, JobEntry> {
        &self.events_map as *const HashMap<i32, JobEntry>  as *mut HashMap<i32, JobEntry>
    }
    pub fn register(&self, fd: RawFd, token: TokenEntry, interests: Ready, opts: PollOpt, job: Job) -> io::Result<()> {
        debug!("register1 fd: {:?} len: {} token: {:?}", fd, self.events_map.len(), token.token);
        let mut info = libc::epoll_event {
            events: ioevent_to_epoll(interests, opts),
            u64: fd as u64
        };
        let events_map = self.events_map();
        unsafe {
            events_map.as_mut().unwrap().insert(fd as i32, JobEntry { token_entry: token, job, ready: Ready::empty()});
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut info))?;
            debug!("register2 fd: {:?} len: {} token: {:?}", fd, self.events_map.len(), token.token);
            Ok(())
        }
    }
    pub fn reregister(&self, fd: RawFd, token: TokenEntry, interests: Ready, opts: PollOpt) -> io::Result<()> {
        // TODO
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
        debug!("deregister1 fd: {:?} len: {} ", fd, self.events_map.len());
        let mut info = libc::epoll_event {
            events: 0,
            u64: 0,
        };
        let events_map = self.events_map();
        unsafe {
            cvt(libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, &mut info))?;
            let v = events_map.as_mut().unwrap().remove(&fd as &i32);
            debug!("deregister2 fd: {:?} len: {} token: {:?}", fd, self.events_map.len(), v.unwrap().token_entry.token);
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

