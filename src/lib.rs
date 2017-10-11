extern crate futures;
extern crate tokio_core;

use std::thread::{self, JoinHandle};

use futures::Stream;
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded};
use tokio_core::reactor::{Core, Handle};

pub trait Runner<T> {
    fn run(&mut self, t: T, handle: &Handle);
    fn shutdown(&mut self) {}
}

pub struct FutureWorker<T> {
    sender: UnboundedSender<Option<T>>,
    thread_handle: Option<JoinHandle<()>>,
}

fn poll<T, R: Runner<T>>(mut runner: R, rx: UnboundedReceiver<Option<T>>) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    {
        let f = rx.take_while(|t| Ok(t.is_some())).for_each(|t| {
            runner.run(t.unwrap(), &handle);
            Ok(())
        });
        core.run(f).unwrap();
    }

    runner.shutdown();
}

impl<T: Send + 'static> FutureWorker<T> {
    pub fn new<N, R>(name: N, runner: R) -> Self
    where
        N: Into<String>,
        R: Runner<T> + Send + 'static,
    {
        let (tx, rx) = unbounded();

        let handle = thread::Builder::new()
            .name(name.into())
            .spawn(move || poll(runner, rx))
            .unwrap(); // TODO error handle;

        FutureWorker {
            sender: tx,
            thread_handle: Some(handle),
        }
    }

    pub fn schedule(&self, task: T) {
        self.sender.unbounded_send(Some(task)).unwrap();
    }
}

impl<T> Drop for FutureWorker<T> {
    fn drop(&mut self) {
        self.sender.unbounded_send(None).unwrap();
        let thread_handle = self.thread_handle.take().unwrap();
        thread_handle.join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::mpsc::{self, Sender};
    use std::time::{Duration, Instant};

    use tokio_core::reactor::{Timeout, Handle};
    use futures::Future;

    struct StepRunner {
        ch: Sender<u64>,
    }

    impl Runner<u64> for StepRunner {
        fn run(&mut self, step: u64, handle: &Handle) {
            let timeout = Timeout::new(Duration::from_millis(step), handle).unwrap();
            let sender = self.ch.clone();
            let f = timeout.then(move |_| {
                sender.send(step).unwrap();
                Ok(())
            });
            handle.spawn(f);
        }

        fn shutdown(&mut self) {
            self.ch.send(0).unwrap();
        }
    }

    #[test]
    fn test_future_worker() {
        let (tx, rx) = mpsc::channel();
        {
            let step_runner = StepRunner { ch: tx };
            let worker = FutureWorker::new("test-async-worker", step_runner);

            let start = Instant::now();
            worker.schedule(50);
            worker.schedule(100);
            worker.schedule(150);
            assert_eq!(rx.recv().unwrap(), 50);
            assert_eq!(rx.recv().unwrap(), 100);
            assert_eq!(rx.recv().unwrap(), 150);

            assert!(start.elapsed() < Duration::from_millis(200));
            assert!(start.elapsed() > Duration::from_millis(150));
        }

        assert_eq!(rx.recv().unwrap(), 0);
    }
}
