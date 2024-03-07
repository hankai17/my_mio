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
    fn run1(&mut self) {
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

fn test4() {
    let u1 = User {id: 4};
    let mut user = (Mutex::new(u1));
    user.get_mut().unwrap().run1();
}

fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

fn test5() {
    let u1 = User {id: 4};
    let mut user = Arc::new(Mutex::new(u1));

    //current_user.set(user.clone());   // 为什么调用完set后 在下面get_mut时 得到的是空?
    let id = user.lock().unwrap().get_id();
    println!("id: {}", id);

    print_type_of(&user);

    let mut m = Arc::get_mut(&mut user).unwrap();
    print_type_of(&m);

    let u = m.get_mut().unwrap();
    print_type_of(&u);

    //current_user.set(user.clone());
    u.run1();
}

fn test6() {
    /*
       let mut x = Arc::new(3);
     *Arc::get_mut(&mut x).unwrap() = 4;
     assert_eq!(*x, 4);

     x.clone();
    //let _y = Arc::clone(&x); // 在clone x后 为了确保安全性 就不能get_mut了
    assert!(Arc::get_mut(&mut x).is_none());
    */

    let mut x = Arc::new(3);
    let ptr = Arc::get_mut(&mut x).unwrap();
    *ptr = 5;

    let _y = Arc::clone(&x);
    //assert!(Arc::get_mut(&mut x).is_none());
    assert_eq!(*x, 5);
    assert_eq!(*_y, 5);

    //*ptr = 100;
}

fn test7() {
    let mut data = Arc::new(5);
    //let mut other_data = Arc::clone(&data);
    let mut other_data = data.clone();

    let ptr = Arc::as_ptr(&data);
    unsafe {
        let p = ptr as *const i32 as *mut i32;
        *p = 123; 
        println!("data: {}", *ptr);
        println!("other_data: {}", *other_data);
    }
}

fn test8() {
    let mut data = Arc::new(5);

    *Arc::make_mut(&mut data) += 1;         // Won't clone anything
    let mut other_data = Arc::clone(&data); // Won't clone inner data
    *Arc::make_mut(&mut data) += 1;         // Clones inner data
    *Arc::make_mut(&mut data) += 1;         // Won't clone anything
    *Arc::make_mut(&mut other_data) *= 2;   // Won't clone anything

    // Now `data` and `other_data` point to different allocations.
    assert_eq!(*data, 8);
    assert_eq!(*other_data, 12);
}

fn test9() {
    let u1 = User {id: 4};
    let mut user = Arc::new(Mutex::new(u1));

    current_user.set(user.clone());
    let id = user.lock().unwrap().get_id();
    println!("id: {}", id);

    print_type_of(&user);

    let mut user = get_current_user();
    let mut m = Arc::get_mut(&mut user).unwrap(); // 还是空的
    print_type_of(&m);

    let u = m.get_mut().unwrap();
    print_type_of(&u);

    //current_user.set(user.clone());
    u.run1();
}

fn test10() {
    let mut data = Arc::new(75);
    let weak = Arc::downgrade(&data);

    assert!(75 == *data);
    assert!(75 == *weak.upgrade().unwrap());

    *Arc::make_mut(&mut data) += 1;

    assert!(76 == *data);
    assert!(weak.upgrade().is_none()); 
    let v = weak.upgrade().unwrap();
}

/*
   fn test11() {
   let u1 = User {id: 4};
   let mut user = Arc::new(Mutex::new(u1));
   let mut weak = Arc::downgrade(&user);

   current_user.set(user.clone());
   let id = user.lock().unwrap().get_id();
   println!("id: {}", id);

   weak.upgrade()

   }
   */

fn test12() {
    let u1 = User {id: 4};
    let mut user = Arc::new(Mutex::new(u1));

    let id = user.lock().unwrap().get_id();
    println!("id: {}", id);

    print_type_of(&user);

    let ptr: *mut Mutex<User> = Arc::as_ptr(&mut user) as * mut _;

    print_type_of(&ptr);

    unsafe {
        let u = (*ptr).get_mut().unwrap();
        print_type_of(&u);

        u.run1();
    }
}

fn test13() {
    let u1 = User {id: 4};
    let mut user = Arc::new(Mutex::new(u1));

    let id = user.lock().unwrap().get_id();
    println!("id: {}", id);
    current_user.set(user.clone());   // 为什么调用完set后 在下面into_inner时 得到的是空?

    print_type_of(&user);

    let m = Arc::into_inner(user).unwrap();

    print_type_of(&m);

    let mut u = Mutex::into_inner(m).unwrap();
    print_type_of(&u);

    u.run1();
}

fn main() {
    //test0();
    //test1();
    //test2();
    //test3();
    //test4();
    //test5();
    //test6();
    //test7();
    //test9();
    //test10();
    //test11();
    //test12();
    test13();
}
