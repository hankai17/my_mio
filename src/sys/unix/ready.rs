use event_imp::{Ready, ready_as_usize, ready_from_usize};
use std::ops;
use std::fmt;

pub struct UnixReady(Ready);

const ERROR: usize  = 0b00_0100;
const HUP: usize    = 0b00_1000;
const AIO: usize    = 0b00_0000;
const LIO: usize    = 0b10_1000;
const PRI: usize    = 0b100_1000;

pub const READY_ALL: usize = ERROR | HUP | AIO | LIO | PRI;

