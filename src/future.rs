use std::thread::{self, JoinHandle};

use futures::{Stream, Future};
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded};
use tokio_core::reactor::{Core, Handle};

use super::Stopped;

pub type BoxFuture = Box<Future<Item = (), Error = ()> + 'static>;

pub trait Runner<T> {
    fn init(&mut self, _: Handle) {}
    fn future(&self, task: T, handle: &Handle) -> BoxFuture;
    fn spawn(&self, task: T, handle: &Handle) {
        let f = self.future(task, handle);
        handle.spawn(f);
    }
}

pub struct Worker<T> {
    sender: UnboundedSender<Option<T>>,
    thread_handle: Option<JoinHandle<()>>,
}

#[derive(Clone)]
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
    runner.init(handle.clone());

    let f = rx.take_while(|t| Ok(t.is_some())).for_each(|t| {
        runner.spawn(t.unwrap(), &handle);
        Ok(())
    });
    core.run(f).unwrap();
}

fn poll_buffered<T, R: Runner<T>>(mut runner: R, rx: UnboundedReceiver<Option<T>>, amt: usize) {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    runner.init(handle.clone());

    let f = rx.take_while(|t| Ok(t.is_some()))
        .map(move |t| runner.future(t.unwrap(), &handle))
        .buffer_unordered(amt)
        .for_each(|_| Ok(()));
    core.run(f).unwrap();
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

    pub fn new_buffered<N, R>(name: N, runner: R, amt: usize) -> Self
    where
        N: Into<String>,
        R: Runner<T> + Send + 'static,
    {
        let (tx, rx) = unbounded();

        let handle = thread::Builder::new()
            .name(name.into())
            .spawn(move || poll_buffered(runner, rx, amt))
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

    use std::sync::mpsc::{self, Sender, TryRecvError};
    use std::time::{Duration, Instant};

    use tokio_core::reactor::{Timeout, Handle};
    use futures::Future;
    use futures::sync::oneshot;

    #[test]
    fn test_future_worker() {
        let (tx, rx) = mpsc::channel();
        let runner = SenderRunner { tx: tx };
        let worker = Worker::new("test_future_worker", runner);

        let start = Instant::now();
        worker.schedule(500);
        worker.schedule(1000);
        worker.schedule(1500);
        assert_eq!(rx.recv().unwrap(), 500);
        assert_eq!(rx.recv().unwrap(), 1000);
        assert_eq!(rx.recv().unwrap(), 1500);

        assert!(start.elapsed() > Duration::from_millis(1500));
        assert!(start.elapsed() < Duration::from_millis(2000));
    }

    #[test]
    fn test_buffered_future_worker() {
        let (tx, rx) = mpsc::channel();
        let runner = SenderRunner { tx: tx };
        let worker = Worker::new_buffered("test_future_worker", runner, 2);

        let start = Instant::now();
        worker.schedule(500);
        worker.schedule(1000);
        worker.schedule(1500);
        assert_eq!(rx.recv().unwrap(), 500);
        assert_eq!(rx.recv().unwrap(), 1000);
        assert_eq!(rx.recv().unwrap(), 1500);

        assert!(start.elapsed() > Duration::from_millis(2000));
    }

    #[test]
    fn test_buffered_future_worker2() {
        let (tx, rx) = mpsc::channel();
        let runner = SenderRunner { tx: tx };
        let worker = Worker::new_buffered("test_future_worker2", runner, 2);

        let (tx1, rx1) = oneshot::channel();
        worker.schedule(rx1);
        let (tx2, rx2) = oneshot::channel();
        worker.schedule(rx2);
        let (tx3, rx3) = oneshot::channel();
        worker.schedule(rx3);
        let (tx4, rx4) = oneshot::channel();
        worker.schedule(rx4);

        tx1.send(1).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
        tx4.send(4).unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tx2.send(2).unwrap();
        assert_eq!(rx.recv().unwrap(), 2);
        assert_eq!(rx.recv().unwrap(), 4);
        tx3.send(3).unwrap();
        assert_eq!(rx.recv().unwrap(), 3);
    }

    #[test]
    fn test_scheduler() {
        let (tx, rx) = mpsc::channel();
        let runner = SenderRunner { tx: tx };
        let worker = Worker::new("test_future_scheduler", runner);
        let scheduler = worker.get_scheduler();

        scheduler.schedule(1).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);

        drop(worker);
        let res = scheduler.schedule(100);
        assert_eq!(res, Err(Stopped));
        assert_eq!(rx.recv().unwrap(), 0);
    }

    struct SenderRunner {
        tx: Sender<u64>,
    }

    impl Runner<u64> for SenderRunner {
        fn future(&self, du: u64, handle: &Handle) -> BoxFuture {
            let sender = self.tx.clone();
            let timeout = Timeout::new(Duration::from_millis(du), handle).unwrap();
            let f = timeout.then(move |_| Ok(sender.send(du).unwrap()));
            Box::new(f)
        }
    }

    impl Runner<oneshot::Receiver<u64>> for SenderRunner {
        fn future(&self, tx: oneshot::Receiver<u64>, _handle: &Handle) -> BoxFuture {
            let sender = self.tx.clone();
            let f = tx.map_err(|_| ()).map(move |t| sender.send(t).unwrap());
            Box::new(f)
        }
    }

    impl Drop for SenderRunner {
        fn drop(&mut self) {
            self.tx.send(0).unwrap();
        }
    }
}
