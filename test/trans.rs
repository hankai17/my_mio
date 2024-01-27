/*
if you have a &T, then there is no &mut T to the same instance,
if you have a &mut T, then there is no &T or &mut T to the same instance.
*/
fn test() {
    let mut i = 32;
    let mut_ref = &mut i;   // &mut i32 指向同一实例
    let x: &i32 = mut_ref; // &i32  指向同一实例
    //*mut_ref = 2;           // &mut T 与 &T是可以指向同一实例的 // &mut T会被降级为&T
    println!("{}", x);
}
// 更多案例参考test/rust/day01/type.rs 

fn main() {
    let x = 5;
    let raw = &x as *const i32 as *mut _;
    unsafe { *raw = 2; }
    
    let mut y = 10;
    let raw_mut = &mut y as *mut i32;

    //println!("raw points at {}", *raw_mut);
}

//#[repr(C)]
#[derive(Debug)]
pub struct TraitObject {
    pub ptr: *mut (),
    pub extra: usize,
}

fn main2() {
    let a: String = "foo".to_string();
    let ref_a: &str = a.as_str();
    
    println!("{:?}", a);
    println!("{:?}", ref_a);
    let b: TraitObject = unsafe { std::mem::transmute(ref_a) };
    println!("{:?}", b);
}

#[derive(Debug)]
pub struct Test {
    pub t: usize,
}

fn main3() {
    let a : Test = Test { t: 99 };
    //println!("{:?}, addr {}", a, &a);

    let ptr1: *mut() = unsafe { std::mem::transmute(&a) };
    println!("{:?}", ptr1);

    let ptr2: *mut() = &a as *const _ as * mut();
    println!("{:?}", ptr2);

}
