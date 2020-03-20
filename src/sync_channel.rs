use std::ops::Deref;
use std::ptr;

use std::sync::mpsc::{SendError, RecvError, TryRecvError};
use std::sync::atomic::Ordering;

use loom::sync::{Arc, Mutex, CausalCell, Condvar};
use loom::sync::atomic::{AtomicPtr, AtomicBool, AtomicUsize};
use loom::thread;

/// Create a new SPMC channel.
pub fn sync_channel<T: Send>(bound: usize) -> (Sender<T>, Receiver<T>) {
    let a = Arc::new(Inner::new(bound));
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
    /// Returns Some() if the bound is reached.
    pub fn try_send(&mut self, t: T) -> Result<Option<T>, SendError<T>> {
        if self.inner.is_disconnected.load(Ordering::SeqCst) {
            Err(SendError(t))
        } else {
            // Only safe from a single thread...
            //
            // But we have `&mut self`, so we're good!
            match unsafe { self.inner.queue.push(t) } {
                Ok(_) => Ok(None),
                Err(t) => Ok(Some(t)),
            }
        }
    }

    /// Send a message to the receivers.
    ///
    /// Returns a SendError if there are no more receivers listening.
    pub fn send(&mut self, t: T) -> Result<(), SendError<T>> {
        if self.inner.is_disconnected.load(Ordering::SeqCst) {
            Err(SendError(t))
        } else {
            if let Err(t) = unsafe {
                // Only safe from a single thread...
                //
                // But we have `&mut self`, so we're good!
                self.inner.queue.push(t)
            } {
                let mut guard = self.inner.throttling_guard.lock().expect("failed acquire throttling mutex");
                let mut tx = t;
                while let Err(ty) = unsafe { self.inner.queue.push(tx) } {
                    tx = ty;
                    let tmp = self.inner.throttling_condvar.wait(guard);
                    if self.inner.is_disconnected.load(Ordering::SeqCst) {
                        return Err(SendError(tx));
                    }
                    guard = match tmp {
                        Ok(guard) => guard,
                        Err(_) => return Err(SendError(tx)),
                    }
                }
            }

            if self.inner.num_sleeping.load(Ordering::SeqCst) > 0 {
                let guard = self.inner.sleeping_guard.lock().unwrap();
                self.inner.sleeping_condvar.notify_one();
                std::mem::drop(guard);
            }

            Ok(())
        }
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.is_disconnected.store(true, Ordering::SeqCst);
        if self.inner.num_sleeping.load(Ordering::SeqCst) > 0 {
            let guard = self.inner.sleeping_guard.lock().unwrap();
            self.inner.sleeping_condvar.notify_all();
            std::mem::drop(guard);
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
        let handlepop = |len: usize| {
                if true || len == self.inner.queue.bound {
                    match self.inner.throttling_guard.lock() {
                        Ok(guard) => self.inner.throttling_condvar.notify_one(),
                        Err(_) => panic!("failed to acquire throttling mutex"),
                    }
                }
        };
        match self.inner.queue.pop() {
            Some((len, t)) => {
                handlepop(len);
                Ok(t)
            },
            None => {
                if self.inner.is_disconnected.load(Ordering::SeqCst) {
                    // Check that it didn't fill in a message inbetween
                    // trying to pop and us checking is_disconnected
                    match self.inner.queue.pop() {
                        Some((len, t)) => {
                            handlepop(len);
                            Ok(t)
                        },
                        None => Err(TryRecvError::Disconnected),
                    }
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
    sleeping_guard: Mutex<()>,
    sleeping_condvar: Condvar,
    throttling_guard: Mutex<()>,
    throttling_condvar: Condvar,
    num_sleeping: AtomicUsize,
}

impl<T: Send> Inner<T> {
    fn new(bound: usize) -> Inner<T> {
        Inner {
            queue: Queue::new(bound),
            is_disconnected: AtomicBool::new(false),

            sleeping_guard: Mutex::new(()),
            sleeping_condvar: Condvar::new(),
            throttling_guard: Mutex::new(()),
            throttling_condvar: Condvar::new(),
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
        self.inner.is_disconnected.store(true, Ordering::SeqCst);

        match self.inner.throttling_guard.lock() {
            Ok(guard) => {
                self.inner.throttling_condvar.notify_one();
                std::mem::drop(guard);
            },
            Err(_) => panic!("failed to acquire throttling mutex"),
        }
    }
}

pub(super) struct Queue<T: Send> {
    bound: usize,
    len: AtomicUsize,
    head: CausalCell<*mut Node<T>>,
    tail: AtomicPtr<Node<T>>,
}

impl<T: Send> Queue<T> {
    pub(super) fn new(bound: usize) -> Queue<T> {
        let stub = Node::new(None);
        Queue {
            bound: bound,
            len: AtomicUsize::new(0),
            head: CausalCell::new(stub),
            tail: AtomicPtr::new(stub),
        }
    }

    // Not safe to call from multiple threads.
    pub(super) unsafe fn push(&self, t: T) -> Result<(), T> {
        let len = self.len.fetch_add(1, Ordering::SeqCst);
        if len >= self.bound {
            self.len.fetch_sub(1, Ordering::SeqCst);
            return Err(t);
        }

        let end = Node::new(None);

        let node = self.head.with_mut(|p| {
            ::std::mem::replace(&mut *p, end)
        });

        (*node).value = Some(t);
        (*node).next.store(end, Ordering::SeqCst);

        Ok(()) 
    }

    pub(super) fn pop(&self) -> Option<(usize, T)> {
        unsafe {
            let mut tail = ptr::null_mut();
            loop {
                tail = self.tail.swap(tail, Ordering::SeqCst);
                if tail.is_null() {
                    thread::yield_now();
                    continue;
                } else {
                    break;
                }
            }

            let mut node = Box::from_raw(tail);

            let next = node.next.load(Ordering::SeqCst);
            if !next.is_null() {
                let len = self.len.fetch_sub(1, Ordering::SeqCst);
                self.tail.store(next, Ordering::SeqCst);
                return node.value.take().map(|v| (len, v));
            } else {
                self.tail.store(Box::into_raw(node), Ordering::SeqCst);
                return None;
            }
        }
    }
}

impl<T: Send> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let head = self.tail.swap(ptr::null_mut(), Ordering::SeqCst);
            if head != ptr::null_mut() {
                let mut node = Box::from_raw(head);
                loop {
                    let next = node.next.load(Ordering::SeqCst);
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
