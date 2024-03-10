use std::sync::{Arc, Mutex};

trait Animal {
    fn baby_name(&mut self);
}

struct Dog;

impl Dog {
    fn f1(&mut self) {
        println!("f1");
    }
    fn baby_name1(&mut self) {
        // String::from("puppy")
        println!("dog baby name");
    }
}

impl Animal for Dog {
    fn baby_name(&mut self) {
        //String::from("cover dog baby name")
        println!("cover dog baby name");
    }
}

//pub type Handler = Box<dyn FnMut() -> Arc<Mutex<Animal>> + 'static + Send + Sync>

fn test1() {
    //println!("A baby dog is called a {}", Dog::baby_name());
    //let clone = acceptor.clone();
    //let job = Box::new(move |val: i64| { clone.lock().unwrap().handleRead(val); });
    let mut cb =  || -> Arc<Mutex<Animal>> {
        Arc::new(Mutex::new(Dog))
    };
    let a = cb();
    //a.lock().unwrap().f1();
    a.lock().unwrap().baby_name();
}

struct Test {
    id: i32
}

impl Test {
    fn new() -> Test {
        Test {
            id: 32
        }
    }
    fn start<H: ?Sized>(&mut self) 
            where H: Animal {
        let mut cb =  || -> Arc<Mutex<Animal>> {
            Arc::new(Mutex::new(Animal))
        };
    }
}

fn test2() {
    let t = Test::new();
    t.start();
}

fn main() {
    //test1();
    test2();
}
