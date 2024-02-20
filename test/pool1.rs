use std::thread::{self, JoinHandle};
use std::sync::{Arc, mpsc, Mutex};


type Job = Box<dyn FnMut(i64) + 'static + Send + Sync>;

enum Message {
    ByeBye,
    NewJob(Job),
}

struct Worker where
{
    _id: usize,
    t: Option<JoinHandle<()>>,
}

impl Worker
{
    fn new(id: usize, receiver: Arc::<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let t = thread::spawn( move || {
            loop {
                let message = receiver.lock().unwrap().recv().unwrap();
                match message {
                    Message::NewJob(mut job) => {
                        println!("do job from worker[{}]", id);
                        job(123);
                    },
                    Message::ByeBye => {
                        println!("ByeBye from worker[{}]", id);
                        break
                    },
                }  
            }
        });

        Worker {
            _id: id,
            t: Some(t),
        }
    }
}

pub struct Pool {
    workers: Vec<Worker>,
    max_workers: usize,
    sender: mpsc::Sender<Message>
}

impl Pool where {
    pub fn new(max_workers: usize) -> Pool {
        if max_workers == 0 {
            panic!("max_workers must be greater than zero!")
        }
        let (tx, rx) = mpsc::channel(); // 多发送 单接受

        let mut workers = Vec::with_capacity(max_workers);
        let receiver = Arc::new(Mutex::new(rx));
        for i in 0..max_workers {
            workers.push(Worker::new(i, Arc::clone(&receiver)));    // 接收端互斥
        }

        Pool { workers: workers, max_workers: max_workers, sender: tx }
    }
    
    pub fn execute<F>(&self, f:F) where F: FnMut(i64) + 'static + Send + Sync
    {

        let job = Message::NewJob(Box::new(f));
        self.sender.send(job).unwrap();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for _ in 0..self.max_workers {
            self.sender.send(Message::ByeBye).unwrap();
        }
        for w in self.workers.iter_mut() {
            if let Some(t) = w.t.take() {
                t.join().unwrap();
            }
        }
    }
}

pub struct MyStruct {
    x: i64
}

impl MyStruct {
    pub fn struct_function(&mut self, val: i64) {
        self.x += val;
        println!("self.x: {}", self.x)
    }
}

fn test1() {
    use std::collections::HashMap;
    let mut events_map: HashMap<i32, Job> = HashMap::new();
    let mut instance = MyStruct{x: 2000};
    let job = Box::new(move |val: i64| {instance.struct_function(val)});
    events_map.insert(123, job);

    let cb = events_map.remove(&123);
    let mut c = cb.unwrap();
    c(123);
}

fn test2() {
    let p = Pool::new(4);
    //p.execute(|| println!("do new job1"));
    //p.execute(|| println!("do new job2"));
    //p.execute(|| println!("do new job3"));
    p.execute(|val: i64| println!("do new job4"));

    let mut instance = MyStruct{x: 1000};
    //p.execute(|val: i64| {instance.struct_function(val)});
    p.execute(move |val: i64| {instance.struct_function(val)});
}

fn main() {
    test1();
    //test2();
}
