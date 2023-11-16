mod event_imp;
pub use event_imp:: {
    PollOpt,
    Ready,
};

mod io;
pub use io::deprecated::would_block;

mod token;
pub use token::Token;

mod poll;
pub use poll::{Poll};

pub mod event {
    pub use super::poll::{Events, Iter}; 
    pub use super::event_imp::{Event, Evented};
}
pub use event::{Events, Event, Evented};

mod sys;
pub mod unix {
    pub use sys::{EventedFd,};
    pub use sys::unix::UnixReady;
}
