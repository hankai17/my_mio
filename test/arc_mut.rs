use std::sync::{Arc, Mutex};

struct ev {
    a: u32
}

// https://stackoverflow.com/questions/74917978/is-it-possible-to-cast-arcdyn-t-into-arcmutexdyn-t
fn main() {
	let num = 5;
	let arc_num = Arc::new(num);
	unsafe {
	    let mtx_num = Arc::new(Mutex::new(*(Arc::as_ptr(&arc_num))));
	}
}

