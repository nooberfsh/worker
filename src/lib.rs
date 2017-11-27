extern crate futures;
extern crate tokio_core;

pub mod future;
pub mod general;

#[derive(Debug, Eq, PartialEq)]
pub struct Stopped;
