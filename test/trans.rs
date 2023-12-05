
fn main1() {
    let x = 5;
    let raw = &x as *const i32;
    
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

fn main() {
    let a : Test = Test { t: 99 };
    //println!("{:?}, addr {}", a, &a);

    let ptr1: *mut() = unsafe { std::mem::transmute(&a) };
    println!("{:?}", ptr1);

    let ptr2: *mut() = &a as *const _ as * mut();
    println!("{:?}", ptr2);

}
