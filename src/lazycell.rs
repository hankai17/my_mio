use std::cell::UnsafeCell;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};


pub struct LazyCell<T> {                // 封装的是 一个编译期大小不能确定的枚举值
                                        // 目的是为了判断 在编译期这个值有无初始化?
    inner: UnsafeCell<Option<T>>,       // Option包装的是枚举(Some None)    // 它告诉编译器：“这个字段虽然通过 &self（共享引用）访问，但允许被修改”。 而已
                                        // UnsafeCell作用是编译期不能决定大小
}

impl<T> LazyCell<T> {
    pub fn new() -> LazyCell<T> {
        LazyCell { inner: UnsafeCell::new(None) }
    }

    pub fn fill(&self, value: T) -> Result<(), T> {
        let slot = unsafe { &mut *self.inner.get() };   // get 返回的是*mut类型  rust中要求必须对裸指针添加unsafe保护
        if slot.is_some() {                             //  self.inner.get() 返回 *mut Option<T>（可变裸指针）                              // 如果没有值 则初始化 如果有值则返回 这就是懒的含义 // 把昂贵的( 配置加载、数据库连接、大对象构建、正则表达式编译、缓存等初始化成本高但不一定会被用到 )初始化操作推迟到真正需要的那一刻，并且只做一次
            return Err(value);                          //  *self.inner.get() 对裸指针解引用，产生一个 Option<T> 的位置（place）注意这里的可变性没有了，但还没有产生引用。 
        }                                               //      具体产生什么引用看是怎么定义的 eg: 这里的&mut 就是可变引用
        *slot = Some(value);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn replace(&mut self, value: T) -> Option<T> {
        mem::replace(
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
        let value = f()?;                               // 失败返回 Result<T, E> 
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
    pub fn into_inner(self) -> Option<T> {              // 消费self
        self.inner.into_inner()
    }
}

impl<T: Copy> LazyCell<T> {                             // 类型擦除: 确保T已实现Copy 即T可拷贝
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn fill(&self, t: T) -> Result<(), T> {
        let res = self.state.compare_exchange(NONE, LOCK,
                Ordering::Acquire, Ordering::Acquire);
        match res {
            Ok(_) => {},
            Err(_) => return Err(t),
        }
        unsafe { *self.inner.get() = Some(t) };
        let res = self.state.compare_exchange(LOCK, SOME,
                Ordering::Release, Ordering::Relaxed);
        match res {
            Ok(_) => {},
            Err(_) => panic!("unable to release lock"),
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

