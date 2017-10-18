use std::thread::{self, JoinHandle};
use std::sync::mpsc::{self, Sender, Receiver};

use super::Stopped;

pub trait Runner<T> {
    fn run(&mut self, task: T);
    fn shutdown(&mut self) {}
}

pub struct Worker<T> {
    sender: Sender<Option<T>>,
    thread_handle: Option<JoinHandle<()>>,
}

pub struct Scheduler<T> {
    sender: Sender<Option<T>>,
}


impl<T> Scheduler<T> {
    fn new(sender: Sender<Option<T>>) -> Self {
        Scheduler { sender: sender }
    }

    pub fn schedule(&self, task: T) -> Result<(), Stopped> {
        self.sender.send(Some(task)).map_err(|_| Stopped)
    }
}

fn poll<T, R: Runner<T>>(mut runner: R, rx: &Receiver<Option<T>>) {
    while let Some(task) = rx.recv().unwrap() {
        runner.run(task)
    }
    runner.shutdown();
}

impl<T: Send + 'static> Worker<T> {
    pub fn new<N, R>(name: N, runner: R) -> Self
    where
        N: Into<String>,
        R: Runner<T> + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        let handle = thread::Builder::new()
            .name(name.into())
            .spawn(move || poll(runner, &rx))
            .unwrap(); // TODO error handle;

        Worker {
            sender: tx,
            thread_handle: Some(handle),
        }
    }

    pub fn schedule(&self, task: T) {
        self.sender.send(Some(task)).unwrap();
    }

    pub fn get_scheduler(&self) -> Scheduler<T> {
        Scheduler::new(self.sender.clone())
    }
}

impl<T> Drop for Worker<T> {
    fn drop(&mut self) {
        self.sender.send(None).unwrap();
        let thread_handle = self.thread_handle.take().unwrap();
        thread_handle.join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;
    use std::sync::mpsc::{self, Sender};
    use std::time::{Duration, Instant};

    struct StepRunner {
        ch: Sender<u64>,
    }

    impl Runner<u64> for StepRunner {
        fn run(&mut self, step: u64) {
            thread::sleep(Duration::from_millis(step));
            self.ch.send(step).unwrap();
        }

        fn shutdown(&mut self) {
            self.ch.send(0).unwrap();
        }
    }

    #[test]
    fn test_general_worker() {
        let (tx, rx) = mpsc::channel();
        let step_runner = StepRunner { ch: tx };
        let worker = Worker::new("test_general_worker", step_runner);

        let start = Instant::now();
        worker.schedule(500);
        let scheduler = worker.get_scheduler();
        scheduler.schedule(1000).unwrap();
        worker.schedule(1500);
        assert_eq!(rx.recv().unwrap(), 500);
        assert_eq!(rx.recv().unwrap(), 1000);
        assert_eq!(rx.recv().unwrap(), 1500);

        assert!(start.elapsed() > Duration::from_millis(3000));
        assert!(start.elapsed() < Duration::from_millis(3100));

        drop(worker);
        let res = scheduler.schedule(100);
        assert_eq!(res, Err(Stopped));
        assert_eq!(rx.recv().unwrap(), 0);
    }
}
