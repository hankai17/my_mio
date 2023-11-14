pub mod unix;

pub use self::unix::READY_ALL;
pub use self::unix::UnixSocket;
pub use self::unix::{
    Awakener,
    EventedFd,
    Events,
    Io,
    Selector,
    set_nonblock,
};

pub const READY_ALL: usize = 0;

