use std::sync::{Arc, Mutex};
use std::thread::spawn;

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
}

fn test0() {
    let mut u = User {id: 4};
    u.set_id(5);
    let id = u.get_id();
    println!("id: {}", id);
}

fn test1() {
    let mut u = Arc::new(User {id: 4});
    //u.set_id(5);  // cannot borrow data in an `Arc` as mutable
    let id = u.get_id();
    println!("id: {}", id);
}

fn test2() {
    let u = User {id: 4};
    let mut u = Arc::new(Mutex::new(u));
    u.lock().unwrap().set_id(5);
    let id = u.lock().unwrap().get_id();
    println!("id: {}", id);
}

fn test3() {
    let u1 = User {id: 4};
    let mut u = Arc::new(Mutex::new(u1));
    u.lock().unwrap().set_id(5);
    let id = u.lock().unwrap().get_id();
    println!("id: {}", id);

    //let id = u1.get_id(); // borrow of moved value: `u1`
}

fn test4() {
    let u1 = User {id: 4};
    let mut u = Arc::new(&u1);

    let id = u.get_id();
    println!("id: {}", id);

    let id = u1.get_id();
    println!("id: {}", id);
}

fn test5() {
    let u1 = User {id: 4};
    let u1_ref = &u1;
    let mut u = Arc::new(u1);

    //let id = u1.get_id(); // u1被移动到了arc中了
    //println!("id: {}", id);

    let id = u.get_id();
    println!("id: {}", id);

    //let id = u1_ref.get_id(); // u1被移动到了arc中了
    //println!("id: {}", id);
}

fn test6() {
    let u1 = User {id: 4};
    let u1_ref = &u1;
    let mut u = Arc::new(u1);
}

fn main() {
    //test0();
    //test1();
    //test2();
    //test3();
    //test4();
    //test5();
    test6();
}

// https://itsallaboutthebit.com/arc-mutex/
fn test2222() {
    let user = User { id: 123 };

    spawn(move || {
        println!("id first thread {}", user.id);
    }).join().unwrap();
}

// https://stackoverflow.com/questions/74917978/is-it-possible-to-cast-arcdyn-t-into-arcmutexdyn-t
fn test111() {
	let num = 5;
	let arc_num = Arc::new(num);
	unsafe {
	    let mtx_num = Arc::new(Mutex::new(*(Arc::as_ptr(&arc_num))));
	}
}
