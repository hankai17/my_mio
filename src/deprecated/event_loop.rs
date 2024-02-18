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

pub struct EventLoop { // 改造成ReadinessQueueInner 并提供get()->*mut
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
    pub fn test(&self) -> io::Result<()> {
        Ok(())
    }
    pub fn new() -> io::Result<EventLoop> {
        EventLoop::configured(Config::default())
    }
    pub fn channel(&self) -> Sender<i32> {
        Sender::new(self.notify_tx.clone())
    }
    fn timer(&self) -> *mut Timer<i32> {
        &self.timer as * const Timer<i32> as *mut Timer<i32>
    }
    pub fn timeout(&self, token: i32, delay: Duration) -> timer::Result<Timeout> {
        let timer = self.timer();
        unsafe {
            timer.as_mut().unwrap().set_timeout(delay, token)
        }
    }
    pub fn clear_timeout(&self, timeout: &Timeout) -> bool {
        let timer = self.timer();
        unsafe {
            timer.as_mut().unwrap().cancel_timeout(&timeout).is_some()
        }
    }
    fn running(&self) -> *mut bool {
        &self.run as * const bool as *mut bool
    }
    pub fn shutdown(&mut self) { 
        let run = self.running();
        unsafe {
            *run.as_mut().unwrap() = false;
        }
    }
    pub fn is_running(&self) -> bool { self.run }
    //pub fn register<E: ?Sized>(&mut self, io: &E, token: Token, interest: Ready, opt: PollOpt) -> io::Result<()>
    pub fn register<E>(&self, io: &E, token: Token, interest: Ready, opt: PollOpt) -> io::Result<()> // 也可以
        where E: Evented {
        self.poll.register(io, token, interest, opt)
    }
    pub fn reregister<E: ?Sized>(&self, io: &E, token: Token, interest: Ready, opt: PollOpt) -> io::Result<()>
        where E: Evented {
        self.poll.reregister(io, token, interest, opt)
    }
    pub fn deregister<E: ?Sized>(&self, io: &E) -> io::Result<()>
        where E: Evented {
        self.poll.deregister(io)
    }
    fn poller(&self) -> *mut Poll {
        &self.poll as * const Poll as *mut Poll
    }
    fn events(&self) -> *mut Events {
        &self.events as * const Events as *mut Events
    }

    fn io_poll(&self, timeout: Option<Duration>) -> io::Result<usize> {
        let poll = self.poller();
        let events = self.events();
        unsafe {
            poll.as_mut().unwrap().poll(events.as_mut().unwrap(), timeout)
        }
    }
    fn io_event<H>(&self, handler: &mut H, evt: Event) 
        where H: Handler {
        handler.ready(self, evt.token(), evt.readiness());
    }
    fn notify<H>(&self, handler: &mut H) 
        where H: Handler {
        for _ in 0..self.config.messages_per_tick {     // 每个周期尝试从pipe 最多读取256次
            match self.notify_rx.try_recv() {
                Ok(msg) => handler.notify(self, msg),
                _ => break,
            }
        }
        let _ = self.poll.reregister(&self.notify_rx, NOTIFY, Ready::readable(), PollOpt::edge() | PollOpt::oneshot());
    }
    fn timer_process<H>(&self, handler: &mut H) 
        where H: Handler {
        /*
        while let Some(t) = self.timer.poll() {
            handler.timeout(self, t);
        }
        */
    }
    fn io_process1(&self, cnt: usize) {
        let mut i = 0;
        log::trace!("io_process(..); cnt={}; len={}", cnt, self.events.len());
        while i < cnt {
            let evt = self.events.get(i).unwrap();
            log::trace!("event={:?}; idx={:?}", evt, i);
            match evt.token() {
                //NOTIFY => self.notify(handler),
                //TIMER => self.timer_process(handler),
                _ => self.io_event1(evt)
            }
            i += 1;
        }
    }
    pub fn run_once1(&self, timeout: Option<Duration>) -> io::Result<()> {
        log::trace!("event loop tick1");
        let cnt = match self.io_poll(timeout) {
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
        self.io_process1(cnt);
        handler.tick(self);
        Ok(())
    }
    // https://stackoverflow.com/questions/45116984/the-trait-cannot-be-made-into-an-object
    fn io_process<H>(&self, handler: &mut H, cnt: usize) 
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
    pub fn run_once<H>(&self, handler: &mut H, timeout: Option<Duration>) -> io::Result<()> 
        where H: Handler {
        log::trace!("event loop tick");
        let cnt = match self.io_poll(timeout) {
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
        self.io_process(handler, cnt);
        handler.tick(self);     // 没有实现也能调?
        Ok(())
    }
    pub fn run<H>(&self, handler: &mut H) -> io::Result<()>  // 为什么这里的handler没有那种 基类指针指向子类对象那种多态
                                                            // 这里没有做到 所谓的"acceptor调用自己的handler connection调用自己的handler" // 这里的handler是写死的
        where H: Handler {
        let run = self.running();
        unsafe {
            *run.as_mut().unwrap() = true;
        }
        while self.run {
            self.run_once(handler, None)?; // 改成cb_obj 并让epoll的ptr指向之 只有这样才能抽象任何对象
                                            // 要么就是 server1那种 一个大handler里面用token区分acceptor或者conn
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

