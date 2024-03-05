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

use std::cell::Cell;
use std::cell::RefCell;
use std::thread_local;

thread_local! {
    pub static CURRENT_USER: RefCell<Arc<Mutex<User>>> = panic!("!");
}

fn get_current_user() -> Arc<Mutex<User>> {
    let ptr = CURRENT_USER.with(|user| -> *mut Arc<Mutex<User>> {return user.as_ptr()});
    unsafe {
        let clone = (*ptr).clone();
        clone
    }
}

fn test2() {
    let u1 = User {id: 4};
    let mut user = Arc::new(Mutex::new(u1));
    CURRENT_USER.set(user.clone());
    user.lock().unwrap().run();
}

fn main() {
    test2();
}
