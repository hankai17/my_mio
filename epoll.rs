//use 
use libc::{EPOLLET, EPOLLIN, EPOLLOUT, EPOLLPRI, EPOLLRDHUP};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;
use std::{cmp, i32, io, ptr}
use std::num::NonZeroU8;
use std::{fmt, ops}

macro_rules! syscall {
    ($fn: ident ($($arg: expr),* $(,)*) ) => {
        {
            let res = unsafe { libc::$fn($($arg, )*) };
            if res == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(res)
            }
        }
    };
}

pub struct Interest(NonZeroU8);
const READABLE: u8  = 0b0001;
const WRITABLE: u8  = 0b0010;
const PRIORITY: u8  = 0b0100

impl Interest {
    pub const READABLE: Interest = Interest(unsafe { NonZeroU8::new_unchecked(READABLE) });
    pub const WRITABLE: Interest = Interest(unsafe { NonZeroU8::new_unchecked(WRITABLE) });
    pub const PRIORITY: Interest = Interest(unsafe { NonZeroU8::new_unchecked(PRIORITY) });

    pub const fn add(self, other: Interest) -> Interest {
        Interest(unsafe { NonZeroU8::new_unchecked(self.0.get() | other.0.get()) })
    }
    pub fn remove(self, other: Interest) -> Option<Interest> {
        NonZeroU8::new(self.0.get() & !other.0.get()).map(Interest)
    }
    pub const fn is_readable(self) -> bool {
        (self.0.get() & READABLE) != 0
    }
    pub const fn is_writable(self) -> bool {
        (self.0.get() & WRITABLE) != 0
    }
    pub const fn is_priority(self) -> bool {
        (self.0.get() & PRIORITY) != 0
    }
}

impl fmt::Debug for Interest {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut one = false;
        if self.is_readable() {
            if one {
                write!(fmt, " | ")?
            }
            write!(fmt, "READABLE")?;
            one = true
        }
        if self.is_writable() {
            if one {
                write!(fmt, " | ")?
            }
            write!(fmt, "WRITABLE")?;
            one = true
        }
        if self.is_priority() {
            if one {
                write!(fmt, " | ")?
            }
            write!(fmt, "PRIORITY")?;
            one = true
        }
        debug_assert!(one, "printing empty interests");
        Ok(())
    }
}

//pub struct Token(pub usize);
//impl From<Token> for usize {
//    fn from(val: Token) -> usize {
//        val.0
//    }
//}


pub mod event {
    use std::fmt;
    //use 
    pub fn token(event: &Event) -> Token {
    }

    pub fn is_readable(event: &Event) -> bool {
        (event.events as libc::c_int & libc::EPOLLIN) != 0 ||
                (event.events as libc::c_int & libc::EPOLLPRI) != 0
    }

    pub fn is_writable(event: &Event) -> bool {
        event.events as libc::c_int & libc::EPOLLOUT != 0
    }

    pub fn is_error(event: &Event) -> bool {
        event.events as libc::c_int & libc::EPOLLERR != 0
    }

    pub fn is_read_closed(event: &Event) -> bool {
        event.events as libc::c_int & libc::EPOLLHUP != 0 ||
                (event.events as libc::c_int & libc::EPOLLIN != 0 &&
                 event.events as libc::c_int & libc::EPOLLRDHUP != 0)
    }

    pub fn is_write_closed(event: &Event) -> bool {
        event.events as libc::c_int & libc::EPOLLHUP != 0 ||
                (event.events as libc::c_int & libc::EPOLLOUT != 0 &&
                 event.events as libc::c_int & libc::EPOLLERR != 0) ||
                 event.events as libc::c_int == libc::EPOLLERR
    }

    pub fn is_priority(event: &Event) -> bool {
        event.events as libc::c_int & libc::EPOLLPRI != 0
    }

    pub fn debug_details(f: &mut fmt::Formatter<'_>, event: &Event) -> fmt::Result {
        //
    }
}

////////////////////////////////////////////

////////////////////////////////////////////

pub struct Selector {
    ep: RawFd,
}

pub type Event = libc:epoll_event;
pub type Events = Vec<Event>;

fn interests_to_epoll(interests: Interest) -> u32 {
    let mut kind = EPOLLET;
    if interest.is_readable() {
        kind = kind | EPOLLIN | EPOLLRDHUP;
    }
    if interest.is_writable() {
        kind |= EPOLLOUT;
    }
    if interest.is_priority() {
        kind |= EPOLLPRI;
    }
    kind as u32
}

impl Selector {
    pub fn new() -> io::Result<Selector> {
        let res = syscall!(epoll_create1(libc::EPOLL_CLOEXEC));
        let ep = match res {
            Ok(ep) => ep as RawFd,
            Err(err) => {
                if let Some(libc::ENOSYS) = err.raw_os_error() {
                    match syscall!(epoll_create(1024)) {
                        Ok(ep) => match syscall!(fcntl(ep, libc::F_SETFD, libc::FD_CLOEXEC)) {
                            Ok(ep) => ep as RawFd,
                            Err(err) => {
                                let _ = unsafe { libc::close(ep) };
                                return Err(err);
                            }
                        },
                        Err(err) => return Err(err),
                    }
                } else {
                    return Err(err);
                }
            }
        };
        Ok(Selector {ep,})
    }

    pub fn try_clone(&self) -> io::Result<Selector> {
        syscall!(fcntl(self.ep, libc::F_DUPFD_CLOEXEC, super::LOWEST_FD)).map(|ep| Selector {
            ep,
        })
    }

    pub fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        const MAX_SAFE_TIMEOUT: u128 = libc::c_int::max_value() as u128;
        let timeout = timeout.map( |to| {
            let to_ms = to
                .checked_add(Duration::from_nanos(999_999))
                .unwrap_or(to)
                .as_millis();
        })
        .unwrap_or(-1);
        events.clear();
        syscall!(epoll_wait(self.ep, events.as_mut_ptr(), 
                events.capacity() as i32, timeout)).map( |n_events| {
            unsafe { events.set_len(n_events as usize) };
        })
    }

    pub fn register(&self, fd: RawFd, token: Token, interests: Interest) -> io::Result<()> {
        let mut event = libc:epoll_event {
            events: interests_to_epoll(interests),
            u64: usize::from(token) as u64,
        };
        syscall!(epoll_ctl(self.ep, libc::EPOLL_CTL_ADD, fd, &mut event)).map(|_| ())
    }

    pub fn reregister(&self, fd: RawFd, token: Token, interests: Interest) -> io::Result<()> {
        let mut event = libc:epoll_event {
            events: interests_to_epoll(interests),
            u64: usize::from(token) as u64,
        };
        syscall!(epoll_ctl(self.ep, libc::EPOLL_CTL_MOD, fd, &mut event)).map(|_| ())
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        syscall!(epoll_ctl(self.ep, libc::EPOLL_CTL_DEL, fd, ptr::null_mut())).map(|_| ())
    }

    pub fn id(&self) -> usize { self.id }

    pub fn as_raw_fd(&self) -> RawFd { self.ep }

    pub fn drop(&mut self) {
        if let Err(err) = syscall!(close(self.ep)) {
            error!("error closing epoll: {}", err);
        }
    }
}




