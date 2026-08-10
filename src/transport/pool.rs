//! A fixed pool of worker threads.
//!
//! The HTTP transport accepts connections on one thread and answers them on
//! these, so a request that blocks - a `tasks/result` waiting for its task -
//! costs one worker rather than the accept loop and with it every other client.
//!
//! `std::thread` and a channel. No runtime, and nothing here is HTTP-specific
//! beyond the shape it was built for.

use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A fixed set of threads, each running the same handler over items taken from
/// a bounded queue.
///
/// Bounded in both directions on purpose. The thread count is what keeps one
/// slow request from costing more than a slot. The queue is what keeps a peer
/// from making the server hold an unbounded number of accepted-but-unanswered
/// requests - the same reasoning every other ceiling in this crate is there
/// for. When both are full, [`dispatch`](Self::dispatch) hands the item *back*
/// instead of blocking or dropping it: the accept loop stays responsive, and an
/// overloaded server gets to say so rather than going quiet with the socket
/// still open.
pub(crate) struct WorkerPool<T: Send + 'static> {
    /// Where dispatched items go. `Some` for the pool's whole life; taken in
    /// `Drop`, because dropping the sender is what ends the workers' `recv`.
    queue: Option<SyncSender<T>>,
    workers: Vec<JoinHandle<()>>,
}

impl<T: Send + 'static> WorkerPool<T> {
    /// Start `threads` workers, each ready to run `handle`, with room for
    /// `backlog` items to wait for one.
    ///
    /// `threads` is clamped to at least 1: a pool with no workers accepts items
    /// and never answers them, which is worse than any size the caller could
    /// have meant. A `backlog` of 0 makes the queue a rendezvous - an item is
    /// only accepted when a worker is already waiting for it.
    pub(crate) fn new<H>(threads: usize, backlog: usize, handle: H) -> Self
    where
        H: Fn(T) + Send + Sync + 'static,
    {
        let (queue, receiver) = sync_channel(backlog);
        let receiver = Arc::new(Mutex::new(receiver));
        let handle = Arc::new(handle);

        let workers = (0..threads.max(1))
            .map(|_| {
                let receiver = Arc::clone(&receiver);
                let handle = Arc::clone(&handle);
                std::thread::spawn(move || work(&receiver, handle.as_ref()))
            })
            .collect();

        Self {
            queue: Some(queue),
            workers,
        }
    }

    /// Hand an item to a worker, or hand it back.
    ///
    /// `Err(item)` means every worker is busy *and* the queue behind them is
    /// full. The caller still owns the item and can answer it however an
    /// overloaded server should; nothing is dropped on the floor here.
    pub(crate) fn dispatch(&self, item: T) -> Result<(), T> {
        let Some(queue) = self.queue.as_ref() else {
            return Err(item);
        };

        match queue.try_send(item) {
            Ok(()) => Ok(()),
            // Disconnected cannot happen while we hold the sender, but it is
            // the same answer either way: nobody is going to run this.
            Err(TrySendError::Full(item) | TrySendError::Disconnected(item)) => Err(item),
        }
    }

    /// How many workers are running.
    #[cfg(test)]
    pub(crate) fn size(&self) -> usize {
        self.workers.len()
    }
}

impl<T: Send + 'static> Drop for WorkerPool<T> {
    /// Stop taking work, then wait for what is already in flight.
    ///
    /// Dropping the sender ends the workers' `recv` - after the queue drains,
    /// so a shutdown does not strand items that were already accepted. Joining
    /// is what keeps a half-written answer from being cut off by the process
    /// exiting out from under it.
    fn drop(&mut self) {
        drop(self.queue.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// One worker: take items until the pool goes away, and survive a handler that
/// panics.
fn work<T: Send + 'static, H: Fn(T)>(receiver: &Mutex<Receiver<T>>, handle: &H) {
    loop {
        // The lock is held across `recv` and nothing else. A worker that is
        // *running* an item does not hold it, which is the whole point: that is
        // what lets the other workers keep taking items while this one blocks.
        //
        // Poisoning is tolerated for the same reason it is everywhere else in
        // this crate - a channel receiver has no invariant an interrupted read
        // could break, and refusing to look at it would turn one panic into a
        // pool that never runs anything again.
        let taken = {
            let queue = receiver.lock().unwrap_or_else(|e| e.into_inner());
            queue.recv()
        };

        // `Err` is the pool dropping its sender, with the queue already
        // drained.
        let Ok(item) = taken else { return };

        // A panicking handler costs its own item, not the worker. A pool that
        // shrinks by one every time a tool explodes eventually has no threads
        // left, and a server with no threads left accepts requests forever and
        // answers none of them.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handle(item)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Condvar};
    use std::time::{Duration, Instant};

    /// A channel for a test to collect what the handlers produced.
    ///
    /// `sync_channel` rather than `channel` because the pool shares its handler
    /// across threads, so the sender has to be `Sync`. Nothing here ever fills
    /// the buffer.
    fn results<T>() -> (SyncSender<T>, Receiver<T>) {
        sync_channel(64)
    }

    /// A latch the test opens when it wants blocked workers to proceed.
    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl Gate {
        fn wait(&self) {
            let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
            while !*open {
                open = self.changed.wait(open).unwrap_or_else(|e| e.into_inner());
            }
        }

        fn open(&self) {
            *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.changed.notify_all();
        }
    }

    /// Spin until `check` holds, or give up - so a broken pool fails the test
    /// instead of hanging the suite.
    fn eventually(what: &str, check: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if check() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("timed out waiting for {what}");
    }

    #[test]
    fn an_item_runs_on_a_worker() {
        let (tx, rx) = results();
        let pool = WorkerPool::new(2, 2, move |item: u32| {
            tx.send(item * 2).unwrap();
        });

        pool.dispatch(21).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 42);
    }

    #[test]
    fn items_run_at_the_same_time_rather_than_one_after_another() {
        // The reason the pool exists. Four items that each wait for the other
        // three can only all finish if all four are running at once - a serial
        // executor deadlocks here and the barrier wait never returns.
        const THREADS: usize = 4;

        let barrier = Arc::new(Barrier::new(THREADS));
        let done = Arc::new(AtomicUsize::new(0));

        let (their_barrier, their_done) = (Arc::clone(&barrier), Arc::clone(&done));
        let pool = WorkerPool::new(THREADS, THREADS, move |_: ()| {
            their_barrier.wait();
            their_done.fetch_add(1, Ordering::SeqCst);
        });

        for _ in 0..THREADS {
            pool.dispatch(()).unwrap();
        }

        eventually("every item to finish", || {
            done.load(Ordering::SeqCst) == THREADS
        });
    }

    #[test]
    fn a_blocked_worker_does_not_hold_up_a_free_one() {
        // One worker parked in a handler that never returns on its own; the
        // other must still take work. This is the pool-level statement of the
        // shipping blocker the HTTP transport had.
        let gate = Arc::new(Gate::default());
        let served = Arc::new(AtomicUsize::new(0));

        let (their_gate, their_served) = (Arc::clone(&gate), Arc::clone(&served));
        let pool = WorkerPool::new(2, 2, move |block: bool| {
            if block {
                their_gate.wait();
            }
            their_served.fetch_add(1, Ordering::SeqCst);
        });

        pool.dispatch(true).unwrap();
        pool.dispatch(false).unwrap();

        eventually("the unblocked item to be served", || {
            served.load(Ordering::SeqCst) == 1
        });

        gate.open();
        eventually("the blocked item to be served", || {
            served.load(Ordering::SeqCst) == 2
        });
    }

    #[test]
    fn a_saturated_pool_hands_the_item_back_intact() {
        // One worker, room for one more: the third item has nowhere to go, and
        // the caller gets it back rather than the pool blocking or eating it.
        let gate = Arc::new(Gate::default());
        let their_gate = Arc::clone(&gate);
        let arrived = Arc::new(AtomicUsize::new(0));
        let their_arrived = Arc::clone(&arrived);

        let pool = WorkerPool::new(1, 1, move |_: String| {
            their_arrived.fetch_add(1, Ordering::SeqCst);
            their_gate.wait();
        });

        pool.dispatch("running".to_string()).unwrap();
        eventually("the first item to occupy the worker", || {
            arrived.load(Ordering::SeqCst) == 1
        });
        pool.dispatch("queued".to_string()).unwrap();

        let refused = pool.dispatch("refused".to_string()).unwrap_err();
        assert_eq!(refused, "refused", "the item comes back untouched");

        gate.open();
    }

    #[test]
    fn room_frees_up_again_once_a_worker_is_free() {
        // Saturation is a moment, not a state: the pool must take work again
        // as soon as one exists to take it.
        let gate = Arc::new(Gate::default());
        let their_gate = Arc::clone(&gate);
        let served = Arc::new(AtomicUsize::new(0));
        let their_served = Arc::clone(&served);

        let pool = WorkerPool::new(1, 0, move |_: ()| {
            their_gate.wait();
            their_served.fetch_add(1, Ordering::SeqCst);
        });

        eventually("the worker to be waiting for work", || {
            pool.dispatch(()).is_ok()
        });
        assert!(
            pool.dispatch(()).is_err(),
            "a rendezvous queue holds nothing"
        );

        gate.open();
        eventually("the pool to take work again", || pool.dispatch(()).is_ok());
        eventually("both items to be served", || {
            served.load(Ordering::SeqCst) == 2
        });
    }

    #[test]
    fn a_panicking_handler_does_not_shrink_the_pool() {
        // A pool that loses a thread per panic eventually has none, and a
        // server with no threads accepts everything and answers nothing.
        let (tx, rx) = results();
        let pool = WorkerPool::new(1, 4, move |item: u32| {
            if item == 0 {
                panic!("handler exploded on purpose");
            }
            tx.send(item).unwrap();
        });

        for _ in 0..3 {
            pool.dispatch(0).unwrap();
        }
        pool.dispatch(7).unwrap();

        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            7,
            "the only worker survived three panics"
        );
        assert_eq!(pool.size(), 1);
    }

    #[test]
    fn dropping_the_pool_finishes_what_it_accepted() {
        // Queued work is not stranded by shutdown: the sender is dropped, the
        // queue drains, and only then do the workers see the end.
        let served = Arc::new(AtomicUsize::new(0));
        let their_served = Arc::clone(&served);

        let pool = WorkerPool::new(2, 16, move |_: ()| {
            std::thread::sleep(Duration::from_millis(20));
            their_served.fetch_add(1, Ordering::SeqCst);
        });
        for _ in 0..8 {
            pool.dispatch(()).unwrap();
        }

        drop(pool);
        assert_eq!(
            served.load(Ordering::SeqCst),
            8,
            "drop returned before the work did"
        );
    }

    #[test]
    fn a_pool_asked_for_no_threads_gets_one() {
        let (tx, rx) = results();
        let pool = WorkerPool::new(0, 1, move |item: u32| tx.send(item).unwrap());

        assert_eq!(pool.size(), 1);
        pool.dispatch(1).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    }
}
