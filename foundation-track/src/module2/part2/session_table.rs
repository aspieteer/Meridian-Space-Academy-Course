use std::{collections::HashMap, sync::RwLock};

#[derive(Default)]
pub struct SessionTable {
    table: RwLock<HashMap<u32, String>>,
}

impl SessionTable {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
        }
    }

    pub fn table(&self) -> &RwLock<HashMap<u32, String>> {
        &self.table
    }

    pub fn register(&self, id: u32, station: String) -> Option<String> {
        // Write lock — exclusive.
        // If the key present, update to the new_value and return Some(old_value), otherwise None
        self.table.write().unwrap().insert(id, station)
    }

    pub fn query_session(&self, id: u32) -> Option<String> {
        // Read lock — concurrent with other readers.
        // If the k-v pair present, return Some(value), otherwise None
        self.table.read().unwrap().get(&id).cloned()
    }
}
