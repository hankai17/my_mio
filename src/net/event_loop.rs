use {channel, Poll, Events, Token, TokenAllocator, TokenType, TokenEntry};
use event::Evented;
use event_imp::{Ready, PollOpt, Job, ready_as_usize, TimerJob};
use timer::{self, Timer, Timeout};
use std::{io, usize};
use std::default::Default;
use std::time::Duration;
use std::{fmt, error, any};
use std::sync::{Arc, Mutex};
use std::thread_local;
use poll::CURRENT_TOKEN_ALLOCATOR;

pub enum NotifyError<T> {
    Io(io::Error),
    Full(T),
    Closed(Option<T>),
}

impl<M: any::Any> error::Error for NotifyError<M> {
    fn description(&self) -> &str {
        match *self {
            NotifyError::Io(ref err) => err.description(),
            NotifyError::Closed(..) => "The receiving end has hung up",
            NotifyError::Full(..) => "Queue is full"
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            NotifyError::Io(ref err) => Some(err),
            _ => None
        }
    }
}

impl<M> From<channel::TrySendError<M>> for NotifyError<M> {
    fn from(src: channel::TrySendError<M>) -> NotifyError<M> {
        match src {
            channel::TrySendError::Io(e) => NotifyError::Io(e),
            channel::TrySendError::Full(v) => NotifyError::Full(v),
            channel::TrySendError::Disconnected(v) => NotifyError::Closed(Some(v)),
        }
    }
}

impl<M> fmt::Debug for NotifyError<M> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NotifyError::Io(ref e) => {
                write!(fmt, "NotifyError::IO({:?})", e)
            }
            NotifyError::Full(..) => {
                write!(fmt, "NotifyError::Full(..)")
            }
            NotifyError::Closed(..) => {
                write!(fmt, "NotifyError::Closed(..)")
            }
        }
    }
}

impl<M> fmt::Display for NotifyError<M> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NotifyError::Io(ref e) => {
                write!(fmt, "IO error: {}", e)
            }
            NotifyError::Full(..) => write!(fmt, "Full"),
            NotifyError::Closed(..) => write!(fmt, "Closed"),
        }
    }
}

struct Config {
    notify_capacity: usize,
    messages_per_tick: usize,
    timer_tick: Duration,
    timer_wheel_size: usize,
    timer_capacity: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            notify_capacity: 4_096,
            messages_per_tick: 256,
            timer_tick: Duration::from_millis(100),
            timer_wheel_size: 1_024,
            timer_capacity: 65_536,
        }
    }
}

pub struct Sender<M> {
    tx: channel::SyncSender<M>
}

impl<M> Clone for Sender<M> {
    fn clone(&self) -> Sender<M> {
        Sender { tx: self.tx.clone() }
    }
}

impl<M> Sender<M> {
    fn new(tx: channel::SyncSender<M>) -> Sender<M> {
        Sender { tx }
    }
    pub fn send(&self, msg: M) -> Result<(), NotifyError<M>> {
        self.tx.try_send(msg)?;
        Ok(())
    }
}

unsafe impl Send for EventLoop {}
unsafe impl Sync for EventLoop {}

pub struct EventLoop {
    run: bool,
    pub poll: Poll,
    events: Events,
    timer: Timer<TimerJob>,
    notify_tx: channel::SyncSender<i32>,
    notify_rx: channel::Receiver<i32>,
    config: Config,
}

impl EventLoop {
    fn configured(config: Config) -> io::Result<EventLoop> {
        let token_allocator = Arc::new(Mutex::new(TokenAllocator::new()));
        CURRENT_TOKEN_ALLOCATOR.set(token_allocator);
        let poll = Poll::new()?;                                        // 分配一个poll // 监听无锁队列里pipe的读端
        let timer = timer::Builder::default()
            .tick_duration(config.timer_tick)
            .num_slots(config.timer_wheel_size)
            .capacity(config.timer_capacity)
            .build();
        let (tx, rx) = channel::sync_channel(config.notify_capacity);   // 初始化pipe
        poll.register(&rx,
                    TokenEntry {
                        ttype: TokenType::NotifyEvent,
                        token: Token(0)
                    },
                    Ready::readable(),
                    PollOpt::edge() | PollOpt::oneshot(),
                    Arc::new(Mutex::new(move |_| {}))
        )?;
        poll.register(&timer,
                    TokenEntry {
                        ttype: TokenType::TimersEvent,
                        token: Token(0)
                    },
                    Ready::readable(),
                    PollOpt::edge(),
                    Arc::new(Mutex::new(move |_| {}))
        )?;
        Ok(EventLoop {
            run: true,
            poll,
            events: Events::with_capacity(1024),
            timer,
            notify_tx: tx,
            notify_rx: rx,
            config,
        })
    }

    pub fn new() -> io::Result<EventLoop> {
        EventLoop::configured(Config::default())
    }

    pub fn channel(&self) -> Sender<i32> {
        Sender::new(self.notify_tx.clone())
    }

    pub fn timeout(&mut self, delay: Duration, token: TimerJob)
            -> timer::Result<Timeout> {
        self.timer.set_timeout(delay, token)
    }

    pub fn clear_timeout(&mut self, timeout: &Timeout) -> bool {
        self.timer.cancel_timeout(&timeout).is_some()
    }

    pub fn shutdown(&mut self) { 
        self.run = false;
    }

    pub fn is_running(&self) -> bool { self.run }

    pub fn register<E>(&self, io: &E, token: TokenEntry, interest: Ready,
            opt: PollOpt, job: Job) -> io::Result<()>
            where E: Evented {
        self.poll.register(io, token, interest, opt, job)
    }

    pub fn reregister<E: ?Sized>(&self, io: &E, token: TokenEntry,
            interest: Ready, opt: PollOpt) -> io::Result<()>
            where E: Evented {
        self.poll.reregister(io, token, interest, opt)
    }

    pub fn deregister<E: ?Sized>(&self, io: &E) -> io::Result<()>
        where E: Evented {
        self.poll.deregister(io)
    }

    fn io_poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.poll.poll(&mut self.events, timeout)
    }

    fn io_process(&mut self, cnt: usize) {
        let mut i = 0;
        log::trace!("io_process(..); cnt: {}; len: {}", cnt, self.events.len());
        while i < cnt {
            match self.events.get_mut(i) {
                Some(entry) => {
                    let token = entry.token_entry;
                    println!("token: {:?}", token);
                    match token.ttype {
                        TokenType::TimersEvent => {
                            while let Some(mut t) = self.timer.poll() {
                                t();
                            }
                        },
                        _ => {
                            let c = &mut entry.job;
                            let ready = entry.ready;
                            c
                            .lock()
                            .unwrap()(ready_as_usize(ready) as i64);
                        }
                    }
                },
                None => {
                    println!("get_mut none");
                }
            }
            i += 1;
        }
        self.events.clear();
    }

    pub fn run_once(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        let cnt = match self.io_poll(timeout) {
            Ok(e) => e,
            Err(err) => {
                if err.kind() == io::ErrorKind::Interrupted {
                    //handler.interrupted(self);
                    0
                } else {
                    return Err(err);
                }
            }
        };
        self.io_process(cnt);
        Ok(())
    }

    pub fn run(&mut self) -> io::Result<()> {
        self.run = true;
        while self.run {
            self.run_once(None)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct EventLoopBuilder {
    config: Config,
}

use std::cell::RefCell;
thread_local! {
    pub static CURRENT_LOOP: RefCell<Arc<Mutex<EventLoop>>> = panic!("!");
}

impl EventLoopBuilder {
    pub fn new() -> EventLoopBuilder {
        EventLoopBuilder::default()
    }

    pub fn notify_capacity(&mut self, capacity: usize) -> &mut Self {
        self.config.notify_capacity = capacity;
        self
    }

    pub fn messages_per_tick(&mut self, messages: usize) -> &mut Self {
        self.config.messages_per_tick = messages;
        self
    }

    pub fn timer_tick(&mut self, val: Duration) -> &mut Self {
        self.config.timer_tick = val;
        self
    }

    pub fn timer_wheel_size(&mut self, size: usize) -> &mut Self {
        self.config.timer_wheel_size = size;
        self
    }

    pub fn timer_capacity(&mut self, cap: usize) -> &mut Self {
        self.config.timer_capacity = cap;
        self
    }

	pub fn build(self) -> io::Result<EventLoop> {
        EventLoop::configured(self.config)
    }

    pub fn get_build(self) -> io::Result<Arc<Mutex<EventLoop>>> {
        let event_loop = Arc::new(Mutex::new(self.build().unwrap()));
        let clone = event_loop.clone();
        CURRENT_LOOP.set(clone);

        Ok(event_loop)
    }

    pub fn get_current_loop() -> Arc<Mutex<EventLoop>> {
        let ptr = CURRENT_LOOP.with(|poll| -> *mut Arc<Mutex<EventLoop>> {return poll.as_ptr()});
        unsafe {
            let clone = (*ptr).clone();
            clone
        }
    }
}

