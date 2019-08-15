use std::ops::Deref;
use std::ptr;

use std::sync::mpsc::{SendError, RecvError, TryRecvError};
use std::sync::atomic::Ordering;

use loom::sync::{Arc, Mutex, CausalCell, Condvar};
use loom::sync::atomic::{AtomicPtr, AtomicBool, AtomicUsize};
use loom::thread;

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

impl<T: Send> Sender<T> {
    fn new(inner: Arc<Inner<T>>) -> Sender<T> {
        Sender { inner: inner }
    }

    /// Send a message to the receivers.
    ///
    /// Returns a SendError if there are no more receivers listening.
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if self.inner.is_disconnected.load(Ordering::Acquire) {
            Err(SendError(t))
        } else {
            self.inner.queue.push(t);
            if self.inner.num_sleeping.load(Ordering::Acquire) > 0 {
                *self.inner.sleeping_guard.lock().unwrap() = true;
                self.inner.sleeping_condvar.notify_one();
            }
            Ok(())
        }
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.is_disconnected.store(true, Ordering::Release);
        if self.inner.num_sleeping.load(Ordering::SeqCst) > 0 {
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
        Receiver { inner: self.inner.clone() }
    }
}

impl<T: Send> Receiver<T> {
    fn new(inner: Arc<Inner<T>>) -> Receiver<T> {
        Receiver { inner: Arc::new(RecvInner { inner: inner }) }
    }

    /// Try to receive a message, without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.inner.queue.pop() {
            Some(t) => Ok(t),
            None => {
                if self.inner.is_disconnected.load(Ordering::Acquire) {
                    Err(TryRecvError::Disconnected)
                } else {
                    Err(TryRecvError::Empty)
                }
            }
        }
    }

    /// Receive a message from the channel.
    ///
    /// If no message is available, this will block the current thread until a
    /// message is sent.
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.try_recv() {
            Ok(t) => return Ok(t),
            Err(TryRecvError::Disconnected) => return Err(RecvError),
            Err(TryRecvError::Empty) => {},
        }

        let ret;
        let mut guard = self.inner.sleeping_guard.lock().unwrap();
        self.inner.num_sleeping.fetch_add(1, Ordering::SeqCst);

        loop {
            match self.try_recv() {
                Ok(t) => {
                    ret = Ok(t);
                    break;
                },
                Err(TryRecvError::Disconnected) => {
                    ret = Err(RecvError);
                    break;
                },
                Err(TryRecvError::Empty) => {}
            }
            guard = self.inner.sleeping_condvar.wait(guard).unwrap();
        }

        self.inner.num_sleeping.fetch_sub(1, Ordering::SeqCst);
        ret
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
    num_sleeping: AtomicUsize,
}

impl<T: Send> Inner<T> {
    fn new() -> Inner<T> {
        Inner {
            queue: Queue::new(),
            is_disconnected: AtomicBool::new(false),

            sleeping_guard: Mutex::new(false),
            sleeping_condvar: Condvar::new(),
            num_sleeping: AtomicUsize::new(0),
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
    tail: CausalCell<*mut Node<T>>,
}

impl<T: Send> Queue<T> {
    fn new() -> Queue<T> {
        let stub = Node::new(None);
        Queue {
            head: AtomicPtr::new(stub),
            tail: CausalCell::new(stub),
        }
    }

    fn push(&self, t: T) {
        unsafe {
            let end = Node::new(None);
            self.tail.with_mut(move |tail| {
                (**tail).next.store(end, Ordering::Release);
                (**tail).value = Some(t);
                *tail = end;
            });
        }
    }

    fn pop(&self) -> Option<T> {
        unsafe {
            let mut head = ptr::null_mut();
            loop {
                head = self.head.swap(head, Ordering::SeqCst);
                if head == ptr::null_mut() {
                    thread::yield_now();
                    continue;
                } else {
                    break;
                }
            }
            let mut node = Box::from_raw(head);
            let next = node.next.load(Ordering::Acquire);
            if !next.is_null() {
                self.head.store(next, Ordering::SeqCst);
                return node.value.take();
            } else {
                self.head.store(Box::into_raw(node), Ordering::Release);
                return None;
            }
        }
    }
}

impl<T: Send> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let head = self.head.swap(ptr::null_mut(), Ordering::SeqCst);
            if head != ptr::null_mut() {
                let mut node = Box::from_raw(head);
                loop {
                    let next = node.next.load(Ordering::Acquire);
                    if !next.is_null() {
                        node = Box::from_raw(next);
                    } else {
                        break;
                    }
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
        let b = Box::new(Node {
            value: v,
            next: AtomicPtr::new(ptr::null_mut()),
        });
        Box::into_raw(b)
    }
}
