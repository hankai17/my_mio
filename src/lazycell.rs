use std::cell::UnsafeCell;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};


pub struct LazyCell<T> {                // 封装的是 一个编译期大小不能确定的枚举值
                                        // 目的是为了判断 在编译期这个值有无初始化?
    inner: UnsafeCell<Option<T>>,       // Option包装的是枚举(Some None) 
                                        // UnsafeCell作用是编译期不能决定大小
}

impl<T> LazyCell<T> {
    pub fn new() -> LazyCell<T> {
        LazyCell { inner: UnsafeCell::new(None) }
    }

    pub fn fill(&self, value: T) -> Result<(), T> {
        let slot = unsafe { &mut *self.inner.get() };   // 解引用会 消除mut
        if slot.is_some() {
            return Err(value);
        }
        *slot = Some(value);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn replace(&mut self, value: T) -> Option<T> {
        mem::replace(
            //unsafe { &mut *self.inner.get() },
            self.inner.get_mut(),
            Some(value)
        )
    }

    pub fn borrow(&self) -> Option<&T> {
        unsafe { &*self.inner.get() }.as_ref()          //  converts from &Option<T> to Option<&T>
    }

    #[allow(dead_code)]
    pub fn filled(&self) -> bool {
        self.borrow().is_some()
    }

    #[allow(dead_code)]
    pub fn borrow_mut(&mut self) -> Option<&mut T> {
        unsafe { &mut *self.inner.get() }.as_mut()      // converts from &mut Option<T> to Option<&mut T>
    }

    #[allow(dead_code)]
    pub fn borrow_with<F: FnOnce() -> T>(&self, f: F) -> &T {
        if let Some(value) = self.borrow() {
            return value;
        }
        let value = f();
        if self.fill(value).is_err() {
            panic!("borrow_with: cell was filled by closure")
        }
        self.borrow().unwrap()
    }

    #[allow(dead_code)]
    pub fn borrow_mut_with<F: FnOnce() -> T>(&mut self, f: F) -> &mut T {
        if !self.filled() {
            let value = f();
            if self.fill(value).is_err() {
                panic!("borrow_mut_with: cell was filled by closure")
            }
        }
        self.borrow_mut().unwrap()
    }

    #[allow(dead_code)]
    pub fn try_borrow_with<E, F>(&self, f: F) -> Result<&T, E>
        where F: FnOnce() -> Result<T, E>
    {
        if let Some(value) = self.borrow() {
            return Ok(value);
        }
        let value = f()?;
        if self.fill(value).is_err() {
            panic!("try_borrow_with: cell was filled by closure")
        }
        Ok(self.borrow().unwrap())
    }

    #[allow(dead_code)]
    pub fn try_borrow_mut_with<E, F>(&mut self, f: F) -> Result<&mut T, E>
        where F: FnOnce() -> Result<T, E>
    {
        if self.filled() {
            return Ok(self.borrow_mut().unwrap());
        }
        let value = f()?;
        if self.fill(value).is_err() {
            panic!("try_borrow_mut_with: cell was filled by closure")
        }
        Ok(self.borrow_mut().unwrap())
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Option<T> {
        self.inner.into_inner()
    }
}

impl<T: Copy> LazyCell<T> {
    #[allow(dead_code)]
    pub fn get(&self) -> Option<T> {                    // 打破了引用的两大定律?
        unsafe { *self.inner.get() }
    }
}

const NONE: usize = 0;
const LOCK: usize = 1;
const SOME: usize = 2;

pub struct AtomicLazyCell<T> {
    inner: UnsafeCell<Option<T>>,
    state: AtomicUsize,
}

impl<T> AtomicLazyCell<T> {
    pub fn new() -> AtomicLazyCell<T> {
        Self {
            inner: UnsafeCell::new(None),
            state: AtomicUsize::new(NONE),
        }
    }

    #[allow(dead_code)]
    pub fn fill(&self, t: T) -> Result<(), T> {
        if NONE != self.state.compare_and_swap(NONE, LOCK, Ordering::Acquire) {
            return Err(t);
        }
        unsafe { *self.inner.get() = Some(t) };
        if LOCK != self.state.compare_and_swap(LOCK, SOME, Ordering::Release) {
            panic!("unable to release lock");
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn replace(&mut self, value: T) -> Option<T> {
        match mem::replace(self.state.get_mut(), SOME) {
            NONE | SOME => {}
            _ => panic!("cell in inconsistent state"),
        }
        mem::replace(unsafe { &mut *self.inner.get() }, Some(value))
    }

    #[allow(dead_code)]
    pub fn filled(&self) -> bool {
        self.state.load(Ordering::Acquire) == SOME
    }

    pub fn borrow(&self) -> Option<&T> {
        match self.state.load(Ordering::Acquire) {
            SOME => unsafe { &*self.inner.get() }.as_ref(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Option<T> {
        self.inner.into_inner()
    }
}

impl<T: Copy> AtomicLazyCell<T> {
    #[allow(dead_code)]
    pub fn get(&self) -> Option<T> {
        match self.state.load(Ordering::Acquire) {
            SOME => unsafe { *self.inner.get() },
            _ => None,
        }
    }
}

unsafe impl<T: Sync + Send> Sync for AtomicLazyCell<T> {}
unsafe impl<T: Send> Send for AtomicLazyCell<T> {}

