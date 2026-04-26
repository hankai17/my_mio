
#[warn(dead_code)]
pub use std::io::{Result, Error, ErrorKind};

pub trait MapNonBlock<T> {
    fn map_non_block(self) -> Result<Option<T>>;
}

impl<T> MapNonBlock<T> for Result<T> {              // 给 Result<T> 实现 MapNonBlock trait 
    fn map_non_block(self) -> Result<Option<T>> {   // self 就是那个 Result<T> 类型的值
        use std::io::ErrorKind::WouldBlock;         // 只对wb感兴趣
        match self {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if let WouldBlock = err.kind() {    // 如果io错误类型是wb
                    Ok(None)
                } else {
                    Err(err)
                }
            }
        }
    }
}

pub mod deprecated {
    pub fn would_block() -> ::std::io::Error {
        ::std::io::ErrorKind::WouldBlock.into()
    }
}
