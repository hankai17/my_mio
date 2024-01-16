struct Foo;

impl Foo {
    fn bar(&self) {}
    fn baz(&self, val: i64) {
        println!("val: {}", val);
    }
}

fn test1() {
    let foo = Foo;

    //let callback = Foo::bar;
    //callback(&foo);

    let callback = || foo.bar();
    callback();

    let cb1 = |val: i64| { foo.baz(val) };
    cb1(11);
}

pub struct MyStruct {
    x: i64
}

impl MyStruct {
    pub fn struct_function(&mut self, val: i64) {
        self.x += val;
    }
}

fn normal_function(val: i64) {
    println!( "sum -> {}", val + 1);
}

//fn do_something_with_a_function(f: fn(i64)) {
//    f(23);
//}

//fn main() {
//    do_something_with_a_function(normal_function as fn(i64));
//
//    //let instance = MyStruct{x: 0};
//    //let instance_function = |val: i64|{instance.struct_function(val)};
//    //do_something_with_a_function(instance_function as fn(i64));
//}

fn do_something_with_a_function<F: FnMut(i64)>(mut f: F) {
    f(23);
}

fn main() {
    test1();
    do_something_with_a_function(normal_function);

    let mut instance = MyStruct{x: 0};
    let mut instance_function = |val: i64|{instance.struct_function(val)};
    do_something_with_a_function(&mut instance_function);
    do_something_with_a_function(&mut instance_function);
}
