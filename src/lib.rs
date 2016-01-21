#![deny(warnings)]
#![deny(missing_docs)]
//! # SPMC
use std::cell::UnsafeCell;
use std::mem;
use std::ops::Deref;
use std::ptr;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicPtr, AtomicBool, Ordering};

pub use std::sync::mpsc::{SendError, RecvError, TryRecvError};

/// Create a new SPMC channel.
pub fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
    let a = Arc::new(Inner::new());
    (Sender::new(a.clone()), Receiver::new(a))
}

/// The Sending side of a SPMC channel.
pub struct Sender<T: Send> {
    inner: Arc<Inner<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}

impl <T: Send> Sender<T> {
    fn new(inner: Arc<Inner<T>>) -> Sender<T> {
        Sender {
            inner: inner
        }
    }

    /// Send a message to the receivers.
    ///
    /// Returns a SendError if there are no more receivers listening.
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if self.inner.is_disconnected.load(Ordering::Acquire) {
            Err(SendError(t))
        } else {
            self.inner.queue.push(t);
            if self.inner.is_sleeping.load(Ordering::Acquire) {
                *self.inner.sleeping_guard.lock().unwrap() = true;
                self.inner.sleeping_condvar.notify_all();
            }
            Ok(())
        }
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.is_disconnected.store(true, Ordering::Release);
        if self.inner.is_sleeping.load(Ordering::Relaxed) {
            *self.inner.sleeping_guard.lock().unwrap() = true;
            self.inner.sleeping_condvar.notify_all();
        }
    }
}

/// The receiving side of a SPMC channel.
///
/// There may be many of these, and the Receiver itself is Sync, so it can be
/// placed in an Arc, or cloned itself.
pub struct Receiver<T: Send> {
    inner: Arc<RecvInner<T>>,
}

unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

impl<T: Send> Clone for Receiver<T> {
    fn clone(&self) -> Receiver<T> {
        Receiver {
            inner: self.inner.clone()
        }
    }
}

impl<T: Send> Receiver<T> {
    fn new(inner: Arc<Inner<T>>) -> Receiver<T> {
        Receiver {
            inner: Arc::new(RecvInner {
                inner: inner
            })
        }
    }

    /// Try to receive a message, without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.inner.queue.pop() {
            Some(t) => Ok(t),
            None => if self.inner.is_disconnected.load(Ordering::Acquire) {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        }
    }

    /// Receive a message from the channel.
    ///
    /// If no message is available, this will block the current thread until a
    /// message is sent.
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.try_recv() {
            Ok(t) => Ok(t),
            Err(TryRecvError::Disconnected) => Err(RecvError),
            Err(TryRecvError::Empty) => {
                self.inner.is_sleeping.store(true, Ordering::Release);
                let guard = self.inner.sleeping_guard.lock().unwrap();
                let mut guard = self.inner.sleeping_condvar.wait(guard).unwrap();
                if *guard {
                    *guard = false;
                    self.inner.is_sleeping.store(false, Ordering::Release);
                }
                self.recv()
            }
        }
    }
}

struct Inner<T: Send> {
    queue: Queue<T>,

    is_disconnected: AtomicBool,

    // ohai there. this is all just to allow the blocking functionality
    // of recv(). The existance of this mutex is only because the condvar
    // needs one. A lock is not used elsewhere, its still a lock-free queue.
    sleeping_guard: Mutex<bool>,
    sleeping_condvar: Condvar,
    is_sleeping: AtomicBool,
}

impl<T: Send> Inner<T> {
    fn new() -> Inner<T> {
        Inner {
            queue: Queue::new(),
            is_disconnected: AtomicBool::new(false),

            sleeping_guard: Mutex::new(false),
            sleeping_condvar: Condvar::new(),
            is_sleeping: AtomicBool::new(false),
        }
    }
}

struct RecvInner<T: Send> {
    inner: Arc<Inner<T>>,
}

impl<T: Send> Deref for RecvInner<T> {
    type Target = Arc<Inner<T>>;
    fn deref(&self) -> &Arc<Inner<T>> {
        &self.inner
    }
}

impl<T: Send> Drop for RecvInner<T> {
    fn drop(&mut self) {
        self.inner.is_disconnected.store(true, Ordering::Release);
    }
}

struct Queue<T: Send> {
    head: AtomicPtr<Node<T>>,
    tail: UnsafeCell<*mut Node<T>>,
}

impl<T: Send> Queue<T> {
    fn new() -> Queue<T> {
        let stub = Node::new(None);
        Queue {
            head: AtomicPtr::new(stub),
            tail: UnsafeCell::new(stub),
        }
    }

    fn push(&self, t: T) {
        unsafe {
            let end = Node::new(None);
            let tail = *self.tail.get();
            (*tail).next.store(end, Ordering::Release);
            (*tail).value = Some(t);
            *self.tail.get() = end;

        }
    }

    fn pop(&self) -> Option<T> {
        unsafe {
            loop {
                let node = self.head.load(Ordering::Acquire);
                let next = (*node).next.load(Ordering::Acquire);
                if !next.is_null() {
                    if node == self.head.compare_and_swap(node, next, Ordering::SeqCst) {
                        return (*node).value.take();
                    }
                } else {
                    return None
                }
            }
        }
    }
}

struct Node<T> {
    value: Option<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    fn new(v: Option<T>) -> *mut Node<T> {
        let mut b = Box::new(Node {
            value: v,
            next: AtomicPtr::new(ptr::null_mut())
        });
        let n = &mut *b as *mut _;
        mem::forget(b);
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity() {
        let (tx, rx) = channel();
        tx.send(5).unwrap();
        tx.send(12).unwrap();

        assert_eq!(rx.try_recv(), Ok(5));
        assert_eq!(rx.try_recv(), Ok(12));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn test_send_on_dropped_chan() {
        let (tx, rx) = channel();
        drop(rx);
        assert_eq!(tx.send(5), Err(SendError(5)));
    }

    #[test]
    fn test_try_recv_on_dropped_chan() {
        let (tx, rx) = channel();
        tx.send(2).unwrap();
        drop(tx);

        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(rx.recv(), Err(RecvError));
    }

    #[test]
    fn test_recv_blocks() {
        use std::thread;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let (tx, rx) = channel();
        let toggle = Arc::new(AtomicBool::new(false));
        let toggle_clone = toggle.clone();
        thread::spawn(move || {
            toggle_clone.store(true, Ordering::Relaxed);
            tx.send(11).unwrap();
        });

        assert_eq!(rx.recv(), Ok(11));
        assert!(toggle.load(Ordering::Relaxed))
    }

    #[test]
    fn test_recv_unblocks_on_dropped_chan() {
        use std::thread;

        let (tx, rx) = channel::<i32>();
        thread::spawn(move || {
            let _tx = tx;
        });

        assert_eq!(rx.recv(), Err(RecvError));
    }
}
