extern crate futures;
extern crate tokio_core;

mod future;
mod general;

pub use self::future::{Runner as FutureRunner, Worker as FutureWorker,
                       Scheduler as FutureScheduler};

pub use self::general::{Runner as GeneralRunner, Worker as GeneralWorker,
                        Scheduler as GeneralScheduler};

#[derive(Debug, Eq, PartialEq)]
pub struct Stopped;
