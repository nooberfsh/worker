use std::thread::{self, JoinHandle};

use futures::Stream;
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded};
use tokio_core::reactor::{Core, Handle};

use super::Stopped;

pub trait Runner<T> {
    fn run(&mut self, task: T, handle: &Handle);
    fn shutdown(&mut self) {}
}

pub struct Worker<T> {
    sender: UnboundedSender<Option<T>>,
    thread_handle: Option<JoinHandle<()>>,
}

pub struct Scheduler<T> {
    sender: UnboundedSender<Option<T>>,
}

impl<T> Scheduler<T> {
    fn new(sender: UnboundedSender<Option<T>>) -> Self {
        Scheduler { sender: sender }
    }

    pub fn schedule(&self, task: T) -> Result<(), Stopped> {
        self.sender.unbounded_send(Some(task)).map_err(|_| Stopped)
    }
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

impl<T: Send + 'static> Worker<T> {
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

        Worker {
            sender: tx,
            thread_handle: Some(handle),
        }
    }

    pub fn schedule(&self, task: T) {
        self.sender.unbounded_send(Some(task)).unwrap();
    }

    pub fn get_scheduler(&self) -> Scheduler<T> {
        Scheduler::new(self.sender.clone())
    }
}

impl<T> Drop for Worker<T> {
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
        let step_runner = StepRunner { ch: tx };
        let worker = Worker::new("test_future_worker", step_runner);

        let start = Instant::now();
        worker.schedule(500);
        let scheduler = worker.get_scheduler();
        scheduler.schedule(1000).unwrap();
        worker.schedule(1500);
        assert_eq!(rx.recv().unwrap(), 500);
        assert_eq!(rx.recv().unwrap(), 1000);
        assert_eq!(rx.recv().unwrap(), 1500);

        assert!(start.elapsed() > Duration::from_millis(1500));
        assert!(start.elapsed() < Duration::from_millis(2000));

        drop(worker);
        let res = scheduler.schedule(100);
        assert_eq!(res, Err(Stopped));
        assert_eq!(rx.recv().unwrap(), 0);
    }
}
