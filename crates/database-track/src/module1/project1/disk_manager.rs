use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use super::{PageId, PageNo};
use crate::module1::{
    PAGE_SIZE,
    config::PAGES_PER_FILE,
    project1::{PageStore, relation_file::RelationFile},
};

pub struct DiskManager {
    relations_dir: PathBuf,
    // HashMap((relation_id, segment_idx) -> RelationFile)
    open_relations: RwLock<HashMap<(u32, u32), Arc<RelationFile>>>,
}

impl DiskManager {
    pub fn new<P: AsRef<Path>>(db_dir: P) -> io::Result<Self> {
        let relations_dir = db_dir.as_ref().join("relations");
        std::fs::create_dir_all(&relations_dir)?;

        Ok(Self {
            relations_dir,
            open_relations: RwLock::new(HashMap::new()),
        })
    }

    fn relation_path(&self, relation_id: u32, segment_idx: u32) -> PathBuf {
        self.relations_dir
            .join(format!("{relation_id}.{segment_idx}.rel"))
    }

    pub fn create_relation(&self, page_id: &PageId) -> io::Result<Arc<RelationFile>> {
        let segment_idx = get_segment_idx(page_id.page_no);
        let path = self.relation_path(page_id.relation_id, segment_idx);

        let relation_file = Arc::new(RelationFile::open(page_id.relation_id, path, PAGE_SIZE)?);

        self.open_relations.write().unwrap().insert(
            (page_id.relation_id, segment_idx),
            Arc::clone(&relation_file),
        );

        Ok(relation_file)
    }

    pub fn get_relation(&self, page_id: &PageId) -> io::Result<Arc<RelationFile>> {
        let segment_idx = get_segment_idx(page_id.page_no);
        if let Some(file) = self
            .open_relations
            .read()
            .unwrap()
            .get(&(page_id.relation_id, segment_idx))
            .cloned()
        {
            return Ok(file);
        }

        let mut relations_wguard = self.open_relations.write().unwrap();

        // Check again the Hashmap to prevent other threads that opened the file just now.
        if let Some(file) = relations_wguard
            .get(&(page_id.relation_id, segment_idx))
            .cloned()
        {
            return Ok(file);
        }

        let path = self.relation_path(page_id.relation_id, segment_idx);

        let relation_file = Arc::new(RelationFile::open(page_id.relation_id, path, PAGE_SIZE)?);
        relations_wguard.insert(
            (page_id.relation_id, segment_idx),
            Arc::clone(&relation_file),
        );

        Ok(relation_file)
    }
}

impl PageStore for DiskManager {
    fn read_page(&self, page_id: &PageId, dest: &mut [u8]) -> io::Result<()> {
        let relation_file = self.get_relation(page_id)?;

        relation_file.read_and_verify_page(page_id.page_no, dest)
    }

    fn write_page(&self, page_id: &PageId, src: &mut [u8]) -> io::Result<()> {
        let relation_file = self.get_relation(page_id)?;

        relation_file.write_page_with_checksum(page_id.page_no, src)
    }
}

// ===== utils =====

fn get_segment_idx(page_no: PageNo) -> u32 {
    page_no / PAGES_PER_FILE
}
