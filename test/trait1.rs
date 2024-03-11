#![allow(unused)]
use std::sync::{Arc, Mutex};

struct Sheep { naked: bool, name: &'static str }

trait Animal {
	// Associated function signature; `Self` refers to the implementor type.
	//fn new(name: &'static str) -> Self;
	fn new(name: &'static str) -> Self where Self: Sized;

	// Method signatures; these will return a string.
	fn name(&self) -> &'static str;
	fn noise(&self) -> &'static str;

	// Traits can provide default method definitions.
	fn talk(&self) {
		println!("{} says {}", self.name(), self.noise());
	}
}

impl Sheep {
	fn is_naked(&self) -> bool {
		self.naked
	}

	fn shear(&mut self) {
		if self.is_naked() {
			// Implementor methods can use the implementor's trait methods.
			println!("{} is already naked...", self.name());
		} else {
			println!("{} gets a haircut!", self.name);

			self.naked = true;
		}
	}
}

// Implement the `Animal` trait for `Sheep`.
impl Animal for Sheep {
	// `Self` is the implementor type: `Sheep`.
	fn new(name: &'static str) -> Sheep {
		Sheep { name: name, naked: false }
	}

	fn name(&self) -> &'static str {
		self.name
	}

	fn noise(&self) -> &'static str {
		if self.is_naked() {
			"baaaaah?"
		} else {
			"baaaaah!"
		}
	}

	// Default trait methods can be overridden.
	fn talk(&self) {
		// For example, we can add some quiet contemplation.
		println!("{} pauses briefly... {}", self.name, self.noise());
	}
}

fn test0() {
	// Type annotation is necessary in this case.
	let mut dolly: Sheep = Animal::new("Dolly");
	// TODO ^ Try removing the type annotations.

	dolly.talk();
	dolly.shear();
	dolly.talk();
}

/*
   fn test1() {
   let mut cb =  || -> Arc<Mutex<Animal>> {
//Arc::new(Mutex::new(Sheep {name: "Dolly", naked: false}))
Arc::new(Mutex::new(Animal::new("Dolly")))
};
let a = cb();
//a.lock().unwrap().f1();
a.lock().unwrap().talk();
}
*/

struct Test {
	id: i32
}

/*
using sessionAlloc = std::function<Session::ptr(const TcpServer::ptr &, const Socket::ptr &)>;

void start(uint16_t port, const std::string &host = "::", uint32_t backlog = 1024) {
	m_session_alloc_cb = [](const TcpServer::ptr &server, const Socket::ptr &sock) {
		auto session = std::make_shared<SessionType>(server, sock);
		return session;
	};
	start_internal(port, host, backlog);
}
*/

impl Test {
	fn new() -> Test {
		Test {
			id: 32
		}
	}
	//fn start<H: ?Sized>(&mut self) 
	fn start<H: Sized>(&mut self) 
		where H: Animal {
		let mut cb =  || -> Arc<Mutex<dyn Animal>> {
			Arc::new(Mutex::new(<H as Animal>::new("Dolly")))
		};
		let a = cb();
		a.lock().unwrap().talk();
	}
	/*
	fn start1<H: Sized>(&mut self) 
		-> Arc<Mutex<Animal>> where H: Animal {
		let mut cb =  || -> Arc<Mutex<Animal>> {
			Arc::new(Mutex::new(<H as Animal>::new("Dolly")))
		};
		return cb;
	}
	*/
}

fn test2() {
	let mut t = Test::new();
	t.start::<Sheep>();
	//let mut a1 = t.start1::<Sheep>();
	//a1.lock().unwrap().talk();
}

fn main() {
	//test0();
	//test1();
	test2();
}

