use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// MAX_CLIENTS and MAX_CONNECTIONS are both under 256 in this project,
/// so it's sufficient to claim u8 as HashMap index.
#[derive(Debug)]
pub struct IdPool {
    pool: Mutex<HashMap<u8, u8>>,
}

#[derive(Debug)]
pub(crate) struct PoolId {
    id_pool: Arc<IdPool>,
    id: u8,
}

impl IdPool {
    pub fn new(total: usize) -> Arc<Self> {
        debug_assert!(total <= 256, "u8 id space holds at most 256 ids");
        let v1: Vec<u8> = (0..total).map(|i| i as u8).collect();
        let v2: Vec<u8> = (0..total).map(|i| i as u8).collect();
        let map = v1.into_iter().zip(v2).collect::<HashMap<_, _>>();

        Arc::new(Self {
            pool: Mutex::new(map),
        })
    }

    pub(crate) fn select_id(self: &Arc<Self>) -> PoolId {
        let mut pool = self.pool.lock().unwrap();

        // SAFETY: We configure the max connection number at server side,
        // so it's not necessary to check at the client side again.
        let (&idx, _) = fastrand::choice(&*pool).unwrap();
        // SAFETY: the idx is randomly chosen from the original pool,
        // so the removed value could not be None.
        let id = pool.remove(&idx).unwrap();

        PoolId {
            id_pool: Arc::clone(self),
            id,
        }
    }
}

impl PoolId {
    pub(crate) fn get(&self) -> u8 {
        self.id
    }
}

impl Drop for PoolId {
    fn drop(&mut self) {
        self.id_pool.pool.lock().unwrap().insert(self.id, self.id);
        tracing::info!("Pool id guard dropped, reclaim ID: {}", self.id);
    }
}
