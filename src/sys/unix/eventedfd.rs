use {io, Ready, Poll, PollOpt, Token, poll};
use event::Evented;
use std::os::unix::io::RawFd;

pub struct EventedFd<'a>(pub &'a RawFd); // RawFd的声明周期 跟struct一样长

impl <'a> Evented for EventedFd<'a> {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        poll::selector(poll).register(*self.0, token, interest, opts)
    }
    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        poll::selector(poll).reregister(*self.0, token, interest, opts)
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        poll::selector(poll).deregister(*self.0)
    }
}
