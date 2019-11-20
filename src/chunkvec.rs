use crate::backend::Backend;
use crate::{pos2chunk, Chunk, Data, ExtractError, CHUNKSZ_LOG};

use crossbeam::channel::Sender;
use failure::{Fail, Fallible, ResultExt};
use serde_derive::Deserialize;
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap};

// Format of the revision file as deserialized from JSON
#[derive(Debug, Deserialize)]
struct Revision<'d> {
    #[serde(borrow)]
    mapping: HashMap<&'d str, &'d str>,
    size: u64,
}

impl<'d> Revision<'d> {
    fn into_vec(mut self) -> Fallible<Vec<Option<&'d str>>> {
        let max_chunks = pos2chunk(self.size);
        let mut vec = vec![None; max_chunks];
        for (chunknum, relpath) in self.mapping.drain() {
            let n = chunknum.parse::<usize>()?;
            if n >= max_chunks {
                return Err(ExtractError::OutOfBounds(n, max_chunks).into());
            }
            vec[n] = Some(relpath);
        }
        Ok(vec)
    }
}

/// Mapping chunk_id (relpath) to list of seq_ids which reference it.
/// This can be thought of a reverse mapping of what is in the revfile.
type ChunkMap<'d> = BTreeMap<&'d str, SmallVec<[usize; 4]>>;

/// All chunks of a revision, grouped by chunk ID.
#[derive(Debug, Clone)]
pub struct ChunkVec<'d> {
    /// Total image size in bytes
    pub size: u64,
    /// Map chunk_id -> seqs
    chunks: ChunkMap<'d>,
    /// Empty seqs not found in `chunks`
    zero_seqs: Vec<usize>,
}

impl<'d> ChunkVec<'d> {
    /// Parses backup spec JSON and constructs chunk map.
    pub fn decode(input: &'d str) -> Fallible<Self> {
        let rev: Revision<'d> =
            serde_json::from_str(input).with_context(|_| ExtractError::LoadSpec(input.into()))?;
        let size = rev.size;
        if size % (1 << CHUNKSZ_LOG) != 0 {
            return Err(ExtractError::UnalignedSize(rev.size).into());
        }
        let mut chunks = BTreeMap::new();
        let mut zero_seqs = Vec::new();
        for (seq, id) in rev.into_vec()?.into_iter().enumerate() {
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
        pos2chunk(self.size)
    }

    /// Reads chunks from disk and decompresses them. The iterator `idx` controls which chunks are
    /// to be read. Parallel instances of `read` can be fed with disjunct sequences.
    pub fn send_decompressed(
        &self,
        threadid: u8,
        nthreads: u8,
        backend: &Backend,
        tx: Sender<Chunk>,
    ) -> Fallible<()> {
        assert!(nthreads > 0 && threadid < nthreads);
        let mut ids: Vec<(&&str, &SmallVec<[usize; 4]>)> = self
            .chunks
            .iter()
            .skip(threadid as usize)
            .step_by(nthreads as usize)
            .collect();
        // lowest seq_ids first
        ids.sort_unstable_by_key(|e| e.1[0]);
        for (id, seqs) in ids {
            let decompressed = backend
                .load(id)
                .with_context(|_| chunk_error(seqs[0], id))?;
            tx.send(Chunk {
                data: Data::Some(decompressed),
                seqs: seqs.clone(),
            })?;
        }
        Ok(())
    }

    pub fn send_zero(&self, tx: Sender<Chunk>) -> Fallible<()> {
        if !self.zero_seqs.is_empty() {
            tx.send(Chunk {
                data: Data::Zero,
                seqs: SmallVec::from_slice(&self.zero_seqs),
            })?;
        }
        Ok(())
    }
}

#[derive(Fail, Debug, PartialEq, Eq)]
#[fail(display = "Error while loading chunk #{} ({})", seq, id)]
struct ChunkError {
    seq: usize,
    id: String,
}

fn chunk_error(seq: usize, id: &str) -> ChunkError {
    ChunkError {
        seq,
        id: id.to_owned(),
    }
}
