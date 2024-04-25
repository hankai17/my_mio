use {io, Ready, Poll, PollOpt, Registration, SetReadiness, TokenEntry, Job};
use event::Evented;
use std::fmt;
use std::sync::{mpsc, Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use lazycell::{LazyCell, AtomicLazyCell};

pub enum SendError<T> {
    Io(io::Error),
    Disconnected(T),
}

fn format_send_error<T>(e: &SendError<T>, f: &mut fmt::Formatter) -> fmt::Result {
    match *e {
        SendError::Io(ref io_err) => write!(f, "{}", io_err),
        SendError::Disconnected(..) => write!(f, "Disconnected"),
    }
}

impl<T> From<mpsc::SendError<T>> for SendError<T> {
    fn from(src: mpsc::SendError<T>) -> SendError<T> {
        SendError::Disconnected(src.0)
    }
}

impl<T> From<io::Error> for SendError<T> {
    fn from(src: io::Error) -> SendError<T> {
        SendError::Io(src)
    }
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        format_send_error(self, f)
    }
}

pub enum TrySendError<T> {
    Io(io::Error),
    Full(T),
    Disconnected(T),
}

impl<T> From<mpsc::TrySendError<T>> for TrySendError<T> {
    fn from(src: mpsc::TrySendError<T>) -> TrySendError<T> {
        match src {
            mpsc::TrySendError::Full(v) => TrySendError::Full(v),
            mpsc::TrySendError::Disconnected(v) => TrySendError::Disconnected(v),
        }
    }
}

impl<T> From<mpsc::SendError<T>> for TrySendError<T> {
    fn from(src: mpsc::SendError<T>) -> TrySendError<T> {
        TrySendError::Disconnected(src.0)
    }
}

impl<T> From<io::Error> for TrySendError<T> {
    fn from(src: io::Error) -> TrySendError<T> {
        TrySendError::Io(src)
    }
}

struct Inner {
    pending: AtomicUsize,   // 发送成功的次数
    senders: AtomicUsize,
    set_readiness: AtomicLazyCell<SetReadiness>,
}

pub struct SenderCtl {
    inner: Arc<Inner>,
}

impl SenderCtl {
    pub fn inc(&self) -> io::Result<()> {   // 每send成功一次 调用该函数
        let cnt = self.inner.pending.fetch_add(1, Ordering::Acquire);
        if 0 == cnt {
            if let Some(set_readiness) = self.inner.set_readiness.borrow() {    // Option类型 需用Some来接
                set_readiness.set_readiness(Ready::readable())?;
            }
        }
        Ok(())
    }
}

impl Clone for SenderCtl {
    fn clone(&self) -> SenderCtl {
        self.inner.senders.fetch_add(1, Ordering::Relaxed); // 多此一举? 不能读取use_count?
        SenderCtl { inner: self.inner.clone() }
    }
}

impl Drop for SenderCtl {
    fn drop(&mut self) {
        if self.inner.senders.fetch_sub(1, Ordering::Release) == 1 {
            let _ = self.inc();
        }
    }
}

pub struct ReceiverCtl {
    registration: LazyCell<Registration>,
    inner: Arc<Inner>,
}

impl ReceiverCtl {
    pub fn dec(&self) -> io::Result<()> {
        let first = self.inner.pending.load(Ordering::Acquire);
        if first == 1 {
            if let Some(set_readiness) = self.inner.set_readiness.borrow() {
                set_readiness.set_readiness(Ready::empty())?;
            }
        }
        let second = self.inner.pending.fetch_sub(1, Ordering::AcqRel);
        if first == 1 && second > 1 {
            if let Some(set_readiness) = self.inner.set_readiness.borrow() {
                set_readiness.set_readiness(Ready::readable())?;
            }
        }
        Ok(())
    }
}

impl Evented for ReceiverCtl {
    fn register(&self, poll: &Poll, token: TokenEntry, interest: Ready, opts: PollOpt, job: Job) -> io::Result<()> {   // 接收端注册: 分配一个node // 如果有pending则立即入队
        if self.registration.borrow().is_some() {
            return Err(io::Error::new(io::ErrorKind::Other, "receiver already registered"));
        }
        let (registration, set_readiness) = Registration::new(poll, token, interest, opts);
        if self.inner.pending.load(Ordering::Relaxed) > 0 {
            let _ = set_readiness.set_readiness(Ready::readable());
        }
        self.registration.fill(registration).expect("unexpected state encountered");        // 接收端 保存node
        self.inner.set_readiness.fill(set_readiness).expect("unexpected state encountered");    // hankai1初始化inner中的set_readiness
        Ok(())
    }
    fn reregister(&self, poll: &Poll, token: TokenEntry, interest: Ready, opts: PollOpt) -> io::Result<()> {
        let job = Arc::new(Mutex::new(move |val: i64| { println!("null reregister for ReceiverCtl") }));
        match self.registration.borrow() {
            Some(registration) => registration.update(poll, token, interest, opts, job),
            None => Err(io::Error::new(io::ErrorKind::Other, "receiver not registered")),
        }
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        match self.registration.borrow() {
            Some(registration) => registration.deregister(poll),
            None => Err(io::Error::new(io::ErrorKind::Other, "receiver not registered")),
        }
    }
}

pub struct Sender<T> {
    tx: mpsc::Sender<T>,
    ctl: SenderCtl,
}

impl<T> Sender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.tx.send(t)
            .map_err(SendError::from)
            .and_then(|_|{
                self.ctl.inc()?;
                Ok(())
            })
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        Sender {
            tx: self.tx.clone(),
            ctl: self.ctl.clone(),
        }
    }
}

pub struct SyncSender<T> {
    tx: mpsc::SyncSender<T>,
    ctl: SenderCtl,
}

impl<T> SyncSender<T> {
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.tx.send(t)
            .map_err(From::from)
            .and_then(|_| {
                self.ctl.inc()?;
                Ok(())
            })
    }
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(t)
            .map_err(From::from)
            .and_then(|_| {
                self.ctl.inc()?;
                Ok(())
            })
    }
}

impl<T> Clone for SyncSender<T> {
    fn clone(&self) -> SyncSender<T> {
        SyncSender {
            tx: self.tx.clone(),
            ctl: self.ctl.clone(),
        }
    }
}

pub struct Receiver<T> {
    rx: mpsc::Receiver<T>,
    ctl: ReceiverCtl,
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, mpsc::TryRecvError> {
        self.rx.try_recv().and_then(|res| {
            let _ = self.ctl.dec();
            Ok(res)
        })
    }
}

impl<T> Evented for Receiver<T> {
    fn register(&self, poll: &Poll, token: TokenEntry, interest: Ready, opts: PollOpt, job: Job) -> io::Result<()> {
        self.ctl.register(poll, token, interest, opts, job)
    }
    fn reregister(&self, poll: &Poll, token: TokenEntry, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.ctl.reregister(poll, token, interest, opts)
    }
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.ctl.deregister(poll)
    }
}

pub fn ctl_pair() -> (SenderCtl, ReceiverCtl) {
    let inner = Arc::new(Inner {
        pending: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        set_readiness: AtomicLazyCell::new(),
    });
    let tx = SenderCtl {
        inner: inner.clone(),
    };
    let rx = ReceiverCtl {
        registration: LazyCell::new(),
        inner,  // 为何不clone?
    };
    (tx, rx)
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx_ctl, rx_ctl) = ctl_pair();
    let (tx, rx) = mpsc::channel();
    let tx = Sender {
        tx,
        ctl: tx_ctl,
    };
    let rx = Receiver {
        rx,
        ctl: rx_ctl,
    };
    (tx, rx)
}

pub fn sync_channel<T>(bound: usize) -> (SyncSender<T>, Receiver<T>) {
    let (tx_ctl, rx_ctl) = ctl_pair();
    let (tx, rx) = mpsc::sync_channel(bound);
    let tx = SyncSender {
        tx,
        ctl: tx_ctl,
    };
    let rx = Receiver {
        rx,
        ctl: rx_ctl,
    };
    (tx, rx)
}

