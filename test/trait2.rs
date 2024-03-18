
use std::{io, mem, fmt};
use std::collections::HashMap;
type Table = HashMap<String, Vec<String>>;

trait Animal {
	fn new(name: &'static str) -> Self where Self: Sized;
	fn name(&self) -> &'static str;
	fn noise(&self) -> &'static str;
	fn talk(&self) {
		println!("{} says {}", self.name(), self.noise());
	}
	fn test(&self, table: &mut Table);
}

struct Sheep { naked: bool, name: &'static str }
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

impl Animal for Sheep {
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
	fn talk(&self) {
		println!("{} pauses briefly... {}", self.name, self.noise());
	}
	fn test(&self, table: &mut Table) {
	}
}

fn test0() {
	let mut dolly: Sheep = Animal::new("Dolly");
	dolly.talk();
	dolly.shear();
	dolly.talk();
}

fn main() {
    test0();

}
