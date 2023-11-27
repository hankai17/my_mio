use std::cell::UnsafeCell;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct LazyCell<T> {
    inner: UnsafeCell<Option<T>>,
}

impl<T> LazyCell<T> {
    pub fn new() -> LazyCell<T> {
        LazyCell { inner: UnsafeCell::new(None) }
    }

    pub fn fill(&self, value: T) -> Result<(), T> {
        let slot = unsafe { &mut *self.inner.get() };
        if slot.is_some() {
            return Err(value);
        }
        *slot = Some(value);

        Ok(())
    }

    pub fn replace(&mut self, value: T) -> Option<T> {
        mem::replace(unsafe { &mut *self.inner.get() }, Some(value))
    }

    pub fn filled(&self) -> bool {
        self.borrow().is_some()
    }

    pub fn borrow(&self) -> Option<&T> {
        unsafe { &*self.inner.get() }.as_ref()
    }

    pub fn borrow_mut(&mut self) -> Option<&mut T> {
        unsafe { &mut *self.inner.get() }.as_mut()
    }

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

    pub fn borrow_mut_with<F: FnOnce() -> T>(&mut self, f: F) -> &mut T {
        if !self.filled() {
            let value = f();
            if self.fill(value).is_err() {
                panic!("borrow_mut_with: cell was filled by closure")
            }
        }

        self.borrow_mut().unwrap()
    }

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

    pub fn into_inner(self) -> Option<T> {
        unsafe { self.inner.into_inner() }
    }
}

impl<T: Copy> LazyCell<T> {
    pub fn get(&self) -> Option<T> {
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

    pub fn replace(&mut self, value: T) -> Option<T> {
        match mem::replace(self.state.get_mut(), SOME) {
            NONE | SOME => {}
            _ => panic!("cell in inconsistent state"),
        }
        mem::replace(unsafe { &mut *self.inner.get() }, Some(value))
    }

    pub fn filled(&self) -> bool {
        self.state.load(Ordering::Acquire) == SOME
    }

    pub fn borrow(&self) -> Option<&T> {
        match self.state.load(Ordering::Acquire) {
            SOME => unsafe { &*self.inner.get() }.as_ref(),
            _ => None,
        }
    }

    pub fn into_inner(self) -> Option<T> {
        unsafe { self.inner.into_inner() }
    }
}

impl<T: Copy> AtomicLazyCell<T> {
    pub fn get(&self) -> Option<T> {
        match self.state.load(Ordering::Acquire) {
            SOME => unsafe { *self.inner.get() },
            _ => None,
        }
    }
}

unsafe impl<T: Sync + Send> Sync for AtomicLazyCell<T> {}
unsafe impl<T: Send> Send for AtomicLazyCell<T> {}

