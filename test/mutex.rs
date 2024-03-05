#![allow(unused)]

use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::thread;
use std::time::Duration;

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
        println!("is_poisoned: {}", user.is_poisoned());
        sleep_ms(1000 * 3);
    }
}

fn test0() {
    let mutex = Arc::new(Mutex::new(0));
    let c_mutex = Arc::clone(&mutex);

    let _ = thread::spawn(move || {
        let _lock = c_mutex.lock().unwrap();
        //panic!(); // the mutex gets poisoned
        sleep_ms(5 * 1000);
    }).join();
    assert_eq!(mutex.is_poisoned(), true);
}

fn test1() {
    let u1 = User {id: 4};
    let mut mutex = Arc::new(Mutex::new(u1));
    let c_mutex = Arc::clone(&mutex);

    let _ = thread::spawn(move || {
        let _lock = c_mutex.lock().unwrap();
        //panic!(); // the mutex gets poisoned
        sleep_ms(5 * 1000);
    }).join();
    assert_eq!(mutex.is_poisoned(), true);

    /*
    u.lock().unwrap().set_id(5);
    let id = u.lock().unwrap().get_id();
    println!("id: {}", id);
    */

    //let id = u1.get_id(); // borrow of moved value: `u1`
}

use std::cell::Cell;
use std::cell::RefCell;
use std::thread_local;
thread_local! {
    pub static current_user: RefCell<Arc<Mutex<User>>> = panic!("!");
}
fn get_current_user() -> Arc<Mutex<User>> {
    let ptr = current_user.with(|user| -> *mut Arc<Mutex<User>> {return user.as_ptr()});
    unsafe {
        let clone = (*ptr).clone();
        clone
    }
}

// Arc<User> -> Arc<Mutex<User>>
fn test2() {
    /*
    let u1 = User {id: 4};
    let mut arc_user = Arc::new(u1);

    // 怎样转为Arc<Mutex<User>> 而又不影响Arc<User>的使用?
    unsafe {
        let mut mtx_user = Arc::new(Mutex::new(*(Arc::as_ptr(&arc_user)))); // cannot move out of a raw pointer
                                                                            // move occurs because value has type `User`, which does not implement the `Copy` trait
    }
    // 根本没有办法 将Arc<User> -> Arc<Mutex<User>>
    arc_user.run();
    */
}

#[derive(Debug, Clone)]
struct UserImp {
    pub inner: Arc<Mutex<User>>
}

// Arc<UserImp> -> Arc<Mutex<UserImp>>
fn test3() {
    // 封装一层inner 可以clone 可以转换
    let u1 = User {id: 4};
    let mut mtx_user = Arc::new(Mutex::new(u1));
    let mut ui = UserImp {inner: mtx_user};

    let mut mtx_ui = Arc::new(Mutex::new(ui.clone()));

    ui.inner.lock().unwrap().set_id(999);
    println!("id: use ori: {}", ui.inner.lock().unwrap().get_id());
    println!("id: use mtx: {}", mtx_ui.lock().unwrap().inner.lock().unwrap().get_id());
    // 从而mtx_ui  ui指向同一底层对象

    // 但是用的时候 仍然是死锁
    // 只要调用run函数 一定是带arc<mutex>的(因为要修改arc里的属性) 所以一旦lock了 就无法解锁
}

fn main() {
    //test0();
    //test1();
    //test2();
    test3();
}
