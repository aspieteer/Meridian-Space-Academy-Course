use std::{
    collections::BinaryHeap,
    fmt,
    sync::{Arc, Condvar, Mutex},
};

use super::command::{Command, CommandKind};
use super::error::QueueShutdown;
use super::metrics::Metrics;

// ===== CommandQueue =====

pub struct CommandQueue {
    inner: Mutex<QueueInner>,
    metrics: Arc<Metrics>,
    not_empty: Condvar,
    not_full: Condvar,
}

#[derive(Debug)]
struct QueueInner {
    heap: BinaryHeap<Command>,
    capacity: usize,
    shutdown: bool,
}

impl fmt::Debug for CommandQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandQueue")
            .field("queue", &self.inner)
            .finish()
    }
}

impl CommandQueue {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self::with_capacity(capacity))
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(QueueInner {
                heap: BinaryHeap::with_capacity(capacity),
                capacity,
                shutdown: false,
            }),
            metrics: Metrics::new(),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        }
    }

    pub fn metrics_clone(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn push(&self, cmd: Command) -> Result<(), QueueShutdown> {
        let mut queue = self.inner.lock().unwrap();

        loop {
            if queue.is_shutsown() {
                return Err(QueueShutdown::new());
            }

            if queue.is_not_full() {
                let is_safe_mode = matches!(cmd.kind_and_priority().0, CommandKind::SafeMode);
                // Push command into queue
                queue.push(cmd);
                // Statistical methods
                self.metrics.increment_push();
                if is_safe_mode {
                    self.metrics.increment_safe_mode_count();
                }
                self.not_empty.notify_one();
                return Ok(());
            }
            queue = self.not_full.wait(queue).unwrap();
        }
    }

    pub fn pop(&self) -> Result<Command, QueueShutdown> {
        let mut queue = self.inner.lock().unwrap();

        loop {
            if let Some(cmd) = queue.pop() {
                self.metrics.increment_dispatch();
                self.not_full.notify_one();
                return Ok(cmd);
            } else if queue.is_shutsown() {
                return Err(QueueShutdown::new());
            }

            queue = self.not_empty.wait(queue).unwrap();
        }
    }

    pub fn shutdown(&self) {
        let mut queue = self.inner.lock().unwrap();
        queue.shutdown = true;
        // Wake all blocked producers and the consumer.
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    pub fn is_shutsown(&self) -> bool {
        self.inner.lock().unwrap().shutdown
    }
}

// ===== impl QueueInner =====

impl QueueInner {
    fn is_not_full(&self) -> bool {
        self.heap.len() < self.capacity
    }

    fn is_shutsown(&self) -> bool {
        self.shutdown
    }

    fn push(&mut self, cmd: Command) {
        self.heap.push(cmd);
    }

    fn pop(&mut self) -> Option<Command> {
        self.heap.pop()
    }
}
