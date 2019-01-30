use indexmap::IndexMap;
use parking_lot::{Mutex, RWLock};
use std::sync::{Arc, AtomicIsize};

pub struct ChunkData {
    data: Arc<Vec<u8>>,
    ttl: AtomicIsize,
}

#[derive(Debug, Clone)]
pub struct Bucket(Arc<RWLock<IndexMap<&'d str, ChunkData>);
