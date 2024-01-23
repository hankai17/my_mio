use {channel, Poll, Events, Token};
use event::Evented;
use deprecated::{Handler, NotifyError};
use event_imp::{Event, Ready, PollOpt};
use timer::{self, Timer, Timeout};
use std::{io, usize};
use std::default::Default;
use std::time::Duration;

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

pub struct EventLoop {
    run: bool,
    poll: Poll,
    events: Events,
    timer: Timer<i32>,
    notify_tx: channel::SyncSender<i32>,
    notify_rx: channel::Receiver<i32>,
    config: Config
}

const NOTIFY: Token = Token(usize::MAX - 1);
const TIMER: Token = Token(usize::MAX - 2);

impl EventLoop {
    fn configured(config: Config) -> io::Result<EventLoop> {
        let poll = Poll::new()?;                // 分配一个poll // 监听无锁队列里pipe的读端
        let timer = timer::Builder::default()
            .tick_duration(config.timer_tick)
            .num_slots(config.timer_wheel_size)
            .capacity(config.timer_capacity)
            .build();
        let (tx, rx) = channel::sync_channel(config.notify_capacity);   // 初始化pipe
        poll.register(&rx, NOTIFY, Ready::readable(), PollOpt::edge() | PollOpt::oneshot())?;   // 初始化receiver中的node
        poll.register(&timer, TIMER, Ready::readable(), PollOpt::edge())?;  // 初始化timer
        Ok(EventLoop {
            run: true,
            poll,
            timer,
            notify_tx: tx,
            notify_rx: rx,
            config,
            events: Events::with_capacity(1024),
        })
    }
    pub fn new() -> io::Result<EventLoop> {
        EventLoop::configured(Config::default())
    }
    pub fn channel(&self) -> Sender<i32> {
        Sender::new(self.notify_tx.clone())
    }
    pub fn timeout(&mut self, token: i32, delay: Duration) -> timer::Result<Timeout> {
        self.timer.set_timeout(delay, token)
    }
    pub fn clear_timeout(&mut self, timeout: &Timeout) -> bool {
        self.timer.cancel_timeout(&timeout).is_some()
    }
    pub fn shutdown(&mut self) { self.run = false; }
    pub fn is_running(&self) -> bool { self.run }
    pub fn register<E: ?Sized>(&mut self, io: &E, token: Token, interest: Ready, opt: PollOpt) -> io::Result<()>
    //pub fn register<E>(&mut self, io: &E, token: Token, interest: Ready, opt: PollOpt) -> io::Result<()> // 也可以
        where E: Evented {
        self.poll.register(io, token, interest, opt)
    }
    pub fn reregister<E: ?Sized>(&mut self, io: &E, token: Token, interest: Ready, opt: PollOpt) -> io::Result<()>
        where E: Evented {
        self.poll.reregister(io, token, interest, opt)
    }
    pub fn deregister<E: ?Sized>(&mut self, io: &E) -> io::Result<()>
        where E: Evented {
        self.poll.deregister(io)
    }
    fn io_poll(&mut self, timeout: Option<Duration>) -> io::Result<usize> {
        self.poll.poll(&mut self.events, timeout)
    }
    fn io_event<H>(&mut self, handler: &mut H, evt: Event) 
        where H: Handler {
        handler.ready(self, evt.token(), evt.readiness());
    }
    fn notify<H>(&mut self, handler: &mut H) 
        where H: Handler {
        for _ in 0..self.config.messages_per_tick {     // 每个周期尝试从pipe 最多读取256次
            match self.notify_rx.try_recv() {
                Ok(msg) => handler.notify(self, msg),
                _ => break,
            }
        }
        let _ = self.poll.reregister(&self.notify_rx, NOTIFY, Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
    }
    fn timer_process<H>(&mut self, handler: &mut H) 
        where H: Handler {
        while let Some(t) = self.timer.poll() {
            handler.timeout(self, t);
        }
    }
    // https://stackoverflow.com/questions/45116984/the-trait-cannot-be-made-into-an-object
    fn io_process<H>(&mut self, handler: &mut H, cnt: usize) 
        where H: Handler {
        let mut i = 0;
        log::trace!("io_process(..); cnt={}; len={}", cnt, self.events.len());
        while i < cnt {
            let evt = self.events.get(i).unwrap();  // epoll_event 转为 Ready
            log::trace!("event={:?}; idx={:?}", evt, i);
            match evt.token() {
                NOTIFY => self.notify(handler),
                TIMER => self.timer_process(handler),
                _ => self.io_event(handler, evt)
            }
            i += 1;
        }
    }
    pub fn run_once<H>(&mut self, handler: &mut H, timeout: Option<Duration>) -> io::Result<()> 
        where H: Handler {
        log::trace!("event loop tick");
        let events = match self.io_poll(timeout) {
            Ok(e) => e,
            Err(err) => {
                if err.kind() == io::ErrorKind::Interrupted {
                    handler.interrupted(self);
                    0
                } else {
                    return Err(err);
                }
            }
        };
        self.io_process(handler, events);
        handler.tick(self);     // 没有实现也能调?
        Ok(())
    }
    pub fn run<H>(&mut self, handler: &mut H) -> io::Result<()> 
        where H: Handler {
        self.run = true;
        while self.run {
            self.run_once(handler, None)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct EventLoopBuilder {
    config: Config,
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
}

