use event_impl::{Ready, ready_as_usize, ready_from_usize};
use std::ops;
use std::fmt;

pub struct UnixReady(Ready);
