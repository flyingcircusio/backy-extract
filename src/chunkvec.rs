use crate::backend::Backend;
use crate::{pos2chunk, Chunk, Data, ExtractError, Result, CHUNKSZ};

use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};
use smallstr::SmallString;
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap};
use std::iter::IntoIterator;

pub type ChunkId = SmallString<[u8; 32]>;
pub type Seq = SmallString<[u8; 7]>;

// Format of the revision file as deserialized from JSON
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct RevisionMap {
    pub mapping: HashMap<Seq, ChunkId>,
    pub size: u64,
}

impl IntoIterator for RevisionMap {
    type Item = (u32, Option<ChunkId>);
    type IntoIter = RevisionMapIterator;

    fn into_iter(self) -> RevisionMapIterator {
        RevisionMapIterator::new(self)
    }
}

pub struct RevisionMapIterator {
    map: HashMap<u32, ChunkId>,
    i: u32,
    max: u32,
}

impl RevisionMapIterator {
    fn new(map: RevisionMap) -> Self {
        let max = pos2chunk(map.size);
        Self {
            map: map
                .mapping
                .into_iter()
                .map(|(k, v)| (k.parse().expect("numeric key"), v))
                .collect(),
            i: 0,
            max,
        }
    }
}

impl Iterator for RevisionMapIterator {
    type Item = (u32, Option<ChunkId>);

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        self.i += 1;
        if i < self.max {
            let id: Option<ChunkId> = self.map.remove(&i);
            Some((i, id))
        } else {
            None
        }
    }
}

/// Mapping chunk_id (relpath) to list of seq_ids which reference it.
/// This can be thought of a reverse mapping of what is in the revfile.
type ChunkMap = BTreeMap<ChunkId, SmallVec<[u32; 4]>>;

/// All chunks of a revision, grouped by chunk ID.
#[derive(Debug, Clone)]
pub struct ChunkVec {
    /// Total image size in bytes
    pub size: u64,
    /// Map chunk_id -> seqs
    chunks: ChunkMap,
    /// Empty seqs not found in `chunks`
    zero_seqs: Vec<u32>,
}

impl ChunkVec {
    /// Parses backup spec JSON and constructs chunk map.
    pub fn decode(input: &str) -> Result<Self> {
        let rev: RevisionMap =
            serde_json::from_str(input).map_err(|e| ExtractError::DecodeMap(input.into(), e))?;
        let size = rev.size;
        if size % CHUNKSZ as u64 != 0 {
            return Err(ExtractError::UnalignedSize(rev.size));
        }
        let mut chunks = BTreeMap::new();
        let mut zero_seqs = Vec::new();
        for (seq, id) in rev {
            if let Some(id) = id {
                chunks.entry(id).or_insert_with(SmallVec::new).push(seq);
            } else {
                zero_seqs.push(seq);
            }
        }
        Ok(Self {
            size,
            chunks,
            zero_seqs,
        })
    }

    /// Number of chunks to restore
    pub fn len(&self) -> usize {
        pos2chunk(self.size) as usize
    }

    /// Reads chunks from disk and decompresses them. The iterator `idx` controls which chunks are
    /// to be read. Parallel instances of `read` can be fed with disjunct sequences.
    pub fn send_decompressed(
        &self,
        threadid: u8,
        nthreads: u8,
        backend: &Backend,
        tx: Sender<Chunk>,
    ) -> Result<()> {
        assert!(nthreads > 0 && threadid < nthreads);
        let mut ids: Vec<(&ChunkId, &SmallVec<[u32; 4]>)> = self
            .chunks
            .iter()
            .skip(threadid as usize)
            .step_by(nthreads as usize)
            .collect();
        // lowest seq_ids first
        ids.sort_unstable_by_key(|e| e.1[0]);
        for (id, seqs) in ids {
            let decompressed = backend.load(id).map_err(|e| ExtractError::InvalidChunk {
                seq: seqs[0],
                id: id.to_string(),
                source: e,
            })?;
            tx.send(Chunk {
                data: Data::Some(decompressed),
                seqs: seqs.clone(),
            })?;
        }
        Ok(())
    }

    pub fn send_zero(&self, tx: Sender<Chunk>) -> Result<()> {
        if !self.zero_seqs.is_empty() {
            tx.send(Chunk {
                data: Data::Zero,
                seqs: SmallVec::from_slice(&self.zero_seqs),
            })?;
        }
        Ok(())
    }
}

// XXX unit tests
