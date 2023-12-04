use {Ready, Token};
use deprecated::{EventLoop};

pub trait Handler: Sized {  // trait的Size默认是未知的 即?Sized 因为不知道实现这个trait的结构是什么
                            // 把unsized的类型放到指针或者Box里面，就变成了sized了
    type Timeout;   // 关联类型 意思是在实现的时候才知道他是什么类型
    type Message;

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: Ready) {}
    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {} // 值传递Self 所以trait必须是Sized
    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Self::Timeout) {}
    fn interrupted(&mut self, event_loop: &mut EventLoop<Self>) {}
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {} // 定时器
}

// https://laplacedemon.gitbooks.io/-rust/content/sized4e0e3f-sized.html
