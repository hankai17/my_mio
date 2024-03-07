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

/*
    parking_lot::Mutex is used here, but std::sync::Mutex will work too. An extra .unwrap() will be
    required after locking, however.

    I just used parking_lot::Mutex since I did not want to mix Mutexes from different crates in this
    example
*/
use parking_lot::{Mutex, ReentrantMutex};

struct SomeData {
    name: String
}

fn reentrant_mut_fn3(data: Arc<ReentrantMutex<RefCell<SomeData>>>) {
    println!("reentrant_mut_fn3: locking the mutex now. ReentrantMutex is used so we won't deadlock.");
    reentrant_mut_change_data(data, String::from("samantha"));
}

fn reentrant_mut_fn2(data: Arc<ReentrantMutex<RefCell<SomeData>>>) {
    println!("reentrant_mut_fn2: locking the mutex now. ReentrantMutex is used so we won't deadlock.");

    /*
       Notice how the view code was refactored into its own function. That's because we perform a RefCell.borrow(),
       and depending on the type of borrow, we may need to drop the borrow before proceeding.
     */
    reentrant_mut_view_data(data.clone());
    reentrant_mut_fn3(data);
}

fn reentrant_mut_view_data(data: Arc<ReentrantMutex<RefCell<SomeData>>>) {
    let d_locked = data.lock();
    println!("Woohoo, we didn't deadlock!!!");

    println!("I'm now going to access the data in an immutable way");
    let d = d_locked.borrow();
    println!("reentrant_mut_fn2: The name in the data is now {}. It was definitely changed!!!", d.name);

    println!("The data is currently immutable so I can't change it. Let's go one level deeper");
}

fn reentrant_mut_change_data(data: Arc<ReentrantMutex<RefCell<SomeData>>>, new_name: String) {
    let d_locked = data.lock();

    let mut d = d_locked.deref().borrow_mut();
    println!("Mutex locked. Everything is ok!!!. name is: {}. Let's change it to something else.", d.name);

    d.name = new_name;
    println!("The name was changed to {}", d.name);
}

fn reentrant_mut_fn1(data: Arc<ReentrantMutex<RefCell<SomeData>>>) {
    println!("reentrant_mut_fn1: Will lock the mutex now and change the name");

    /*
        Notice how the data change code has been moved to a different function. What we're going to
        do here is perform a mutable borrow and change the data, then perform an immutable borrow
        and view the changed data (in a different function just to illustrate we don't deadlock),
        and finally perform a mutable borrow again to change the data one last time.

        We cannot do all those things at once without violating Rust's borrow checking rules. The solution
        is to perform the first borrow, change the data, then drop the borrow, After that, we perform
        the second borrow, view the data, and drop the borrow. And finally, we perform a borrow, change the data,
        and drop the borrow. The drop happens at the end of the function eg. it happens when we return
        from reentrant_mut_change_data(). So, by moving some the logic to different functions, we do
        not violate Rust's borrow rules!

        To get a runtime error, copy the contents of reentrant_mut_change_data() here instead of
        calling a different function. The code will compile, but it will panic at runtime, as per
        the RefCell documentation.
     */
    reentrant_mut_change_data(data.clone(), String::from("jane"));
    reentrant_mut_fn2(data);
}

fn reentrant_fn2(data: Arc<ReentrantMutex<SomeData>>) {
    println!("reentrant_fn2: locking the mutex now. We're using a ReentrantMutex so we won't deadlock (in this thread only!!!) even though the mutex is locked");
    let locked_data = data.lock();
    println!("Woohoo, we didn't deadlock!!!");

    println!("If i wanted to change the name in locked_data, I can't. The name is still: {}", locked_data.name);
    println!("parking_lot's ReentrantMutex implements Deref, but not DerefMut (for good reason. If it were implemented, the borrowing rules could be broken)");
}

fn reentrant_fn1(data: Arc<ReentrantMutex<SomeData>>) {
    println!("reentrant_fn1: locking the mutex now");
    let d = data.lock();
    println!("Mutex locked. Everything is ok!!!. Calling reentrant_fn2(). name is: {}", d.name);

    reentrant_fn2(data.clone());
}

fn regular_fn2(data: Arc<Mutex<SomeData>>) {
    println!("regular_fn2: locking the mutex now. Prepare for deadlock!!!");
    let _ = data.lock();
    println!("We'll never get here!!!");
}

fn regular_fn1(data: Arc<Mutex<SomeData>>) {
    println!("regular_fn1: locking the mutex now");
    let d = data.lock();
    println!("Mutex locked. Everything is ok!!!. Calling regular_fn2(). name is: {}", d.name);

    regular_fn2(data.clone());
}

fn main() {
    /*
    println!("regular_mutex_example");
    let regular_mutex = Arc::new(Mutex::new(SomeData { name: String::from("bob") }));
    let regular_handle = thread::spawn(move || regular_fn1(regular_mutex.clone()));

    //this thread deadlocks, so uncommenting this join means the app will never die
    //let _ = regular_handle.join();
    println!();
    */

    /*
    println!("Reentrant mutex example with immutable data");
    let reentrant = Arc::new(ReentrantMutex::new(SomeData { name: String::from("dave") }));
    let reentrant_handle = thread::spawn(move || reentrant_fn1(reentrant.clone()));
    let _ = reentrant_handle.join();
    println!();
    */

    println!("Reentant mutex example with mutable data");
    let reentrant_mut = Arc::new(ReentrantMutex::new(RefCell::new(SomeData { name: String::from("billy") })));
    let reentrant_mut_handle = thread::spawn(move || reentrant_mut_fn1(reentrant_mut.clone()));
    let _ = reentrant_mut_handle.join();
    println!();
}

