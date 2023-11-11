pub use self::pipe::Awakener;

mod pipe {
    use sys::unix;
    use {io, Ready, Poll, PollOpt, Token};
}
