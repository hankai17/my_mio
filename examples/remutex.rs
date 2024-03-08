extern crate my_mio;
extern crate bytes;
extern crate parking_lot;

use std::{io, mem, fmt};
use my_mio::{Events, Poll, PollOpt, Ready, Token, Job};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use my_mio::net::{TcpListener, TcpStream, EventLoop, EventLoopBuilder, Acceptor, TcpConnection};
use std::net::{self, SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Condvar};
use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{self, Acquire, Release, AcqRel, Relaxed, SeqCst};
use std::time::Duration;
use std::ops::Deref;
use std::thread;
use std::cell::{RefCell};
use std::thread_local;

use parking_lot::{Mutex, ReentrantMutex};

fn sleep_ms(ms: u64) {
    use std::thread;
    use std::time::Duration;
    thread::sleep(Duration::from_millis(ms));
}

#[derive(Debug)]
struct User {
    id: u32
}

impl User {
    fn get_id(&self) -> u32 {
        self.id
    }
    fn set_id(&mut self, id: u32) {
        self.id = id;
    }
    fn run(&mut self) {
        let user = get_current_user();
        //println!("is_poisoned: {}", user.is_poisoned());
        //user.lock().deref().borrow_mut().get_id();

        sleep_ms(1000 * 3);
    }
}

thread_local! {
    pub static CURRENT_USER: RefCell<Arc<ReentrantMutex<RefCell<User>>>> = panic!("!");
}

fn get_current_user() -> Arc<ReentrantMutex<RefCell<User>>> {
    let ptr = CURRENT_USER.with(|user| -> *mut Arc<ReentrantMutex<RefCell<User>>> {return user.as_ptr()});
    unsafe {
        let clone = (*ptr).clone();
        clone
    }
}

//let d_locked = data.lock();
//let d = d_locked.borrow();
//let mut d = d_locked.deref().borrow_mut();
//d.name = new_name;
fn test0() {
    let u1 = User {id: 4};
    let mut user = Arc::new(ReentrantMutex::new(u1));
    //CURRENT_USER.set(user.clone());

    let locked = user.lock(); 
    let mut u = locked.deref(); // fn deref(&self) -> &T // 返回共享引用
    let id = u.get_id();
    println!("id: {}", id);

    //u.set_id(123); // 不能修改
}

fn test1() {
    let s = RefCell::new(String::from("hello, world"));
    //let s1 = s.borrow();
    let s2 = s.borrow_mut();  // 可变引用 引用 二者不能共存
    //let s1 = s.borrow_mut();  // 可变引用 引用 二者不能共存
    //let s2 = s.borrow();

    //println!("{},{}", s1, s2);
}

fn test2() {
    let u1 = User {id: 4};
    let mut user = Arc::new(ReentrantMutex::new(RefCell::new(u1)));
    CURRENT_USER.set(user.clone());

    let locked = user.lock(); 
    let mut u = locked.deref(); // fn deref(&self) -> &T // 返回共享引用 &RefCell<User>
    let id = u.borrow_mut().get_id();
    println!("id: {}", id);

    let id1 = u.borrow_mut().get_id();
    let id2 = u.borrow().get_id();
    u.borrow_mut().set_id(123);
    u.borrow_mut().run();   // 可变引用一直存在 那么run里就无法拿到引用
}

fn test3() {
    let u1 = User {id: 4};
    let mut user = Arc::new(ReentrantMutex::new(RefCell::new(u1)));
    CURRENT_USER.set(user.clone());

    let locked = user.lock(); 
    let mut u = locked.deref(); // fn deref(&self) -> &T // 返回共享引用 &RefCell<User>
    let id = u.borrow_mut().get_id();
    println!("id: {}", id);

    u.borrow_mut().set_id(123);
    u.borrow_mut().run();
}


fn main() {
    //test0();
    test1();
    //test2();
}

