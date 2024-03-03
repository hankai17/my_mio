//#![feature(local_key_cell_methods)]
#![allow(unused)]
use std::cell::Cell;
use std::cell::RefCell;
use std::sync::{Arc, Mutex, Condvar};

fn test1() {
	thread_local! {
		static X: RefCell<Vec<i32>> = panic!("!");
		static Y: RefCell<Arc<Mutex<i32>>> = panic!("!");
	}

	// Calling X.with() here would result in a panic.
	X.set(vec![1, 2, 3]); // But X.set() is fine, as it skips the initializer above.
	//Y.lock().unwrap().set(11);
	Y.set(Arc::new(Mutex::new(11111)));

	X.with_borrow(|v| assert_eq!(*v, vec![1, 2, 3]));

	//let v = X.get();
	//println!("v: {}", v);
}

fn test2() {
	thread_local! {
		static X: Cell<i32> = panic!("!");
	}
	// Calling X.get() here would result in a panic.
	X.set(123); // But X.set() is fine, as it skips the initializer above.
	assert_eq!(X.get(), 123);
	let v = X.get();
}

fn test3() {
	thread_local!(static FOO: RefCell<u32> = RefCell::new(1));
	//thread_local!(static Bar: RefCell<u32> = panic!("!"));
	thread_local!(static Bar: RefCell<u32> = "!");
	FOO.with(|f| {
		assert_eq!(*f.borrow(), 1);
		*f.borrow_mut() = 2;
	});
	FOO.with(|f| {
		assert_eq!(*f.borrow(), 2);
	});
	Bar.with(|f| {
		assert_eq!(*f.borrow(), 1);
		*f.borrow_mut() = 2;
	});
}

fn main() {
	test1();
	test2();
	test3();
}
