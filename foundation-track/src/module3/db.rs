use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbDropGuard` struct
    /// is dropped.
    db: Db,
}

#[derive(Debug, Clone)]
pub struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
    shutdown: AtomicBool,
}

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}

// ===== impl DbDropGuard =====

impl DbDropGuard {
    pub fn new() -> Self {
        Self { db: Db::new() }
    }

    pub fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
    }
}

// ===== impl Db =====

impl Db {
    pub(crate) fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
            }),
            background_task: Notify::new(),
            shutdown: AtomicBool::new(false),
        });

        tokio::spawn(purge_expired_tasks(shared.clone()));

        Self { shared }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|e| e.data.clone())
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // this value is to check whether the new entry to set is gonna
        // expire before all entries existed.
        let mut notify = false;

        let expires_at = expire.map(|dur| {
            let when = Instant::now() + dur;

            // All entries yet have expirations later than the incoming entry?
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        if let Some(prev) = prev
            && let Some(when) = prev.expires_at
        {
            state.expirations.remove(&(when, key.clone()));
        }

        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);

        if notify {
            // Use this notify to rearrange the next expiring task.
            self.shared.background_task.notify_one();
        }
    }

    pub fn shutdown_purge_task(&self) {
        self.shared.shutdown.store(true, Ordering::Release);

        self.shared.background_task.notify_one();
    }
}

// ===== impl Shared =====

impl Shared {
    fn purge_expired_keys(&self) -> Option<Instant> {
        if self.shutdown.load(Ordering::Relaxed) {
            return None;
        }

        let mut state = self.state.lock().unwrap();
        let state = &mut *state;

        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // Return the next key we're gonna purge.
                return Some(when);
            }

            // For the keys already expired, remove 'em now.
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

// ===== impl State =====

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

// ==== background task =====

async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            tokio::select! {
                _ = tokio::time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }

    tracing::debug!("Purge background task shut down");
}
