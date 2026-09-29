use std::{
    collections::BinaryHeap,
    fmt,
    sync::{Arc, Condvar, Mutex},
};

#[derive(Debug, Eq, PartialEq)]
pub struct Command {
    priority: u8,
    payload: String,
}

impl Ord for Command {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority = higher value in max-heap.
        self.priority.cmp(&other.priority)
    }
}
impl PartialOrd for Command {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Command {
    pub fn new(priority: u8, payload: String) -> Self {
        Self { priority, payload }
    }

    pub fn priority(&self) -> u8 {
        self.priority
    }

    pub fn payload(&self) -> &str {
        &self.payload
    }
}

#[derive(Default)]
pub struct CommandQueue {
    // Mutex + Condvar is the standard pattern for blocking producers/consumers.
    inner: Mutex<BinaryHeap<Command>>,
    available: Condvar,
}

impl fmt::Debug for CommandQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandQueue")
            .field("inner", &self.inner)
            .finish()
    }
}

impl CommandQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BinaryHeap::new()),
            available: Condvar::new(),
        })
    }

    pub fn push(&self, cmd: Command) {
        self.inner.lock().unwrap().push(cmd);
        // Notify one waiting consumer that data is available.
        self.available.notify_one();
    }

    pub fn pop_blocking(&self) -> Command {
        let mut queue = self.inner.lock().unwrap();
        // Condvar::wait releases the mutex and blocks until notified,
        // then reacquires the mutex before returning.
        loop {
            if let Some(cmd) = queue.pop() {
                return cmd;
            }
            queue = self.available.wait(queue).unwrap();
        }
    }
}

#[test]
fn test_command_queue_order() {
    let queue = CommandQueue::default();

    let c1 = Command::new(1, "Hi".to_string());
    let c2 = Command::new(10, "Hello".to_string());
    let c3 = Command::new(6, "foo".to_string());
    let c4 = Command::new(7, "bar".to_string());

    queue.push(c1);
    queue.push(c2);
    queue.push(c3);
    queue.push(c4);

    println!("Queue: {:?}", queue);
}
