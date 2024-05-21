use std::thread;

fn print_hello() {
    println!("hello");
}

//fn run_fn(f: &dyn Fn()) {
fn run_fn<F: Fn() + Send + 'static>(f: F) { // 参考TcpClient 对传参ClientHandler的修饰 
    let hand = thread::spawn(move || {
        f();
    });
}

fn main() {
    run_fn(&print_hello);
}

