use libc::{c_int};

#[macro_use]
pub mod dlsym;

mod awakener;
pub use self::awakener::Awakener;

mod epoll;
pub use self::epoll::{Events, Selector};

mod eventedfd;
pub use self::eventedfd::EventedFd;

mod io;
pub use self::io::{Io, set_nonblock};

mod ready;
pub use self::ready::{UnixReady, READY_ALL};

use std::os::unix::io::FromRawFd;

mod tcp;
pub use self::tcp::{TcpStream, TcpListener};

//pub use iovec::IoVec;

mod uio;

trait IsMinusOne {                                  // 类型萃取: 类型得有特定的功能
    fn is_minus_one(&self) -> bool;                 // c++中类型萃取通过模板偏特化(eg: has_a)
}

impl IsMinusOne for i32 {
    fn is_minus_one(&self) -> bool { *self == -1 }
}

impl IsMinusOne for isize {
    fn is_minus_one(&self) -> bool { *self == -1 }
}

fn cvt<T: IsMinusOne>(t: T) -> ::io::Result<T> {    // 类型擦除: 最终跟类型无关 
    use std::io;
    if t.is_minus_one() {
        Err(io::Error::last_os_error())             // 首先io::Result<T, E>是个枚举 即可以接收Ok也可接收Err
    } else {                                        // ::io::Result<T> 是Rust 标准库的源代码 ::io::Result<T, std::io::Error>的语法糖 只用于接收std::io::Error
        Ok(t)                                       // 如果接收其它错误 eg: ::io::Result<T, String> 得写全
    }
}

pub fn pipe() -> ::io::Result<(Io, Io)> {
    dlsym!(fn pipe2(*mut c_int, c_int) -> c_int);
    let mut pipes = [0; 2];
    unsafe {
        match pipe2.get() {                         // pipe函数 lazy 加载
            Some(pipe2_fn) => {
                let flags = libc::O_NONBLOCK | libc::O_CLOEXEC;
                cvt(pipe2_fn(pipes.as_mut_ptr(), flags))?;
                Ok((Io::from_raw_fd(pipes[0]), Io::from_raw_fd(pipes[1])))
            }
            None => {
                cvt(libc::pipe(pipes.as_mut_ptr()))?;
                let r = Io::from_raw_fd(pipes[0]);
                let w = Io::from_raw_fd(pipes[1]);
                cvt(libc::fcntl(pipes[0], libc::F_SETFD, libc::FD_CLOEXEC))?;
                cvt(libc::fcntl(pipes[1], libc::F_SETFD, libc::FD_CLOEXEC))?;
                cvt(libc::fcntl(pipes[0], libc::F_SETFL, libc::O_NONBLOCK))?;
                cvt(libc::fcntl(pipes[1], libc::F_SETFL, libc::O_NONBLOCK))?;
                Ok((r, w))
            }
        }
    }
}

