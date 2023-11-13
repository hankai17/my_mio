use {Token}

const READABLE: usize = 0b00001;
const WRITABLE: usize = 0b00010;
const ERROR:    usize = 0b00100;
const HUP:      usize = 0b01000;

#[derive(Copy, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct Ready(usize);

impl Ready {
    pub fn empty() -> Ready { Ready(0) }
    pub fn none() -> Ready { Ready::empty() }
    pub fn readable() -> Ready { Ready(READABLE) }
    pub fn writable() -> Ready { Ready(WRITABLE) }
    pub fn error() -> Ready { Ready(ERROR) }
    pub fn hup() -> Ready { Ready(HUP) }
    pub fn all() -> Ready { Ready(READABLE | WRITABLE | ::sys::READY_ALL) }
    pub fn is_empty(&self) -> bool { *self == Ready::empty() }
    pub fn is_none(&self) -> bool { self.is_empty() }
    pub fn is_readable(&self) -> bool { self.contains(Ready::readable()) }
    pub fn is_writable(&self) -> bool { self.contains(Ready::writable()) }
    pub fn is_error(&self) -> bool { self.contains(Ready(ERROR)) }
    pub fn is_hup(&self) -> bool { self.contains(Ready(HUP)) }
    pub fn insert<T: Into<Self>>(&mut self, other: T) {
        let other = other.into();
        self.0 |= other.0;
    }
    pub fn remove<T: Into<Self>>(&mut self, other: T) {
        let other = other.into();
        self.0 &= !other.0;
    }
    pub fn bits(&self) -> usize { self.0 }
    pub fn contains<T: Into<Self>>(&self, other: T) -> bool {
        let other = other.into()
        (*self & other) == other
    }
    pub fn from_usize(val: usize) -> Ready { Ready(val) }
    pub fn as_usize(&self) -> usize { self.0 }
}

pub fn ready_as_usize(events: Ready) -> usize {
    events.0
}
pub fn ready_from_usize(events: usize) -> Ready {
    Ready(events)
}

#[derive(Copy, PartialEq, Eq, Clone, PartialOrd, Ord)]
pub struct PollOpt(usize)

impl PollOpt {
    pub fn empty() -> PollOpt { PollOpt(0) }
    pub fn edge() -> PollOpt { PollOpt(0b0001) }
    pub fn level() -> PollOpt { PollOpt(0b0010) }
    pub fn oneshot() -> PollOpt { PollOpt(0b0100) }
    pub fn urgent() -> PollOpt { PollOpt(0b1000) }
    pub fn all() -> PollOpt { PollOpt::edge() | PollOpt::level() | PollOpt::oneshot() }
    pub fn is_edge(&self) -> bool { self.contains(PollOpt::edge()) }
    pub fn is_level(&self) -> bool { self.contains(PollOpt::level()) }
    pub fn is_oneshot(&self) -> bool { self.contains(PollOpt::oneshot()) }
    pub fn is_urgent(&self) -> bool { self.contains(PollOpt::urgent()) }
    pub fn bits(&self) -> usize { self.0 }
    pub fn contains(&self, other: PollOpt) -> bool { *self & other == other }
    pub fn insert(&self, other: PollOpt) { self.0 |= other.0; }
    pub fn remove(&self, other: PollOpt) { self.0 &= !other.0; }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Event {
    kind: Ready,
    token: Token
}

impl Event {
    pub fn new(readiness: Ready, token: Token) -> Event {
        Event {
            kind: readiness,
            token,
        }
    }
    pub fn readiness(&self) -> Ready {
        self.kind
    }
    pub fn kind(&self) -> Ready { self.kind }
    pub fn token(&self) -> Token { self.token }
}

pub trait Evented {
    fn register(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()>;
    fn reregister(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()>;
    fn deregister(&self, poll: &Poll) -> io::Result<()>;
}

impl Evented for Box<Evented> {
    fn register(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()> {
        self.as_ref().register(poll, token, interest, opts)
    }
    fn reregister(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()> {
        self.as_ref().reregister(poll, token, interest, opts)
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.as_ref().deregister(poll)
    }
}

impl <T: Evented> Evented for Box<T> {
    fn register(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()> {
        self.as_ref().register(poll, token, interest, opts)
    }
    fn reregister(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()> {
        self.as_ref().reregister(poll, token, interest, opts)
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.as_ref().deregister(poll)
    }
}

impl <T: Evented> Evented for ::std::sync::Arch<T> {
    fn register(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()> {
        self.as_ref().register(poll, token, interest, opts)
    }
    fn reregister(&self, poll: &Poll, token: Token, intereset: Ready, opts: PollOpt) -> io::Result<()> {
        self.as_ref().reregister(poll, token, interest, opts)
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.as_ref().deregister(poll)
    }
}

