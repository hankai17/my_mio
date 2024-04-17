
#[derive(Clone)]
pub struct FdEntry {
    pub token: i64,
}

impl Drop for FdEntry {
    fn drop(&mut self) {
        println!("------------dropping FdEntry job");
    }
}

fn main() {
    let mut vec : Vec<FdEntry> = Vec::with_capacity(16);

    vec.push(FdEntry { token: 1});
    vec.push(FdEntry { token: 2});
    vec.push(FdEntry { token: 3});

    vec.clear();
    println!("after clear");

}
