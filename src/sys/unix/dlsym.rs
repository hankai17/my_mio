#![allow(unused)]
use std::marker;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

macro_rules! dlsym {
    (fn $name:ident($($t:ty),*) -> $ret:ty) => (
        #[allow(bad_style)]
        static $name: ::sys::unix::dlsym::DlSym<unsafe extern fn($($t),*) -> $ret> =
            ::sys::unix::dlsym::DlSym {
                name: concat!(stringify!($name), "\0"),
                addr: ::std::sync::atomic::AtomicUsize::new(0),
                _marker: ::std::marker::PhantomData,
            };
    )
}

pub struct DlSym<F> {
    pub name: &'static str,
    pub addr: AtomicUsize,
    pub _marker: marker::PhantomData<F>,
}

impl<F> DlSym<F> {
    pub fn get(&self) -> Option<&F> {
        assert_eq!(mem::size_of::<F>(), mem::size_of::<usize>());
        unsafe {
            if self.addr.load(Ordering::SeqCst) == 0 {                  // 原子操作是原子的没有问题 但它只能保证单条语句(一行)的原子性
                self.addr.store(fetch(self.name), Ordering::SeqCst);    // 保证不了多条语句被编译器乱序 比如这条语句(可以拆成两行)如果是Order::Relaxed 那么就有可能乱序(先store后fetch)
            }                                                           // 虽然SeqCst保证了内存序 但它保证不了多线程对他同时store(但也没啥问题 建议配合CAS操作)
            if self.addr.load(Ordering::SeqCst) == 1 {
                None
            } else {
                mem::transmute::<&AtomicUsize, Option<&F>>(&self.addr)
            }
        }
    }
}

unsafe fn fetch(name: &str) -> usize {
    assert_eq!(name.as_bytes()[name.len() - 1], 0);
    match libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr() as *const _) as usize {
        0 => 1,
        n => n,
    }
}
