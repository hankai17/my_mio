
#[warn(dead_code)]
pub use std::io::{Result, Error, ErrorKind};
pub trait MapNonBlock<T> {
    fn map_non_block(self) -> Result<Option<T>>;
}
impl<T> MapNonBlock<T> for Result<T> {
    fn map_non_block(self) -> Result<Option<T>> {
        use std::io::ErrorKind::WouldBlock;
        match self {
            Ok(value) => Ok(Some(value)),
            Err(err) => {
                if let WouldBlock = err.kind() {
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
