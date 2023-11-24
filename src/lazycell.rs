use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::UnsafeCell;

pub struct LazyCell<T> {
    inner: UnsafeCell<Option<T>>,
}
