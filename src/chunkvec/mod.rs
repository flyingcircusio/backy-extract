mod backend;
mod cache;

use self::cache::{Cache, Entry};
use super::{ExtractError, RawChunk, CHUNKSIZE};

use crossbeam::channel::Sender;
use failure::{format_err, Fail, Fallible, ResultExt};
use num_cpus;
use parking_lot::Mutex;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Format of the revision file as deserialized from JSON
#[derive(Debug, Deserialize)]
struct Revision<'d> {
    #[serde(borrow)]
    mapping: HashMap<&'d str, &'d str>,
    size: u64,
}

impl<'d> Revision<'d> {
    fn into_vec(mut self) -> Fallible<Vec<Option<&'d str>>> {
        let max_chunks = (self.size / u64::from(CHUNKSIZE)) as usize;
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

/// Linearized version of a revision chunk map
#[derive(Debug, Clone)]
pub struct ChunkVec<'d> {
    /// Backup directory (without `chunks`)
    pub dir: PathBuf,
    /// Total image size in bytes
    pub size: u64,
    /// Chunk file id indexed by chunk number, may contain holes
    ids: Vec<Option<&'d str>>,
    /// Caches decompressed output of multiply-referenced chunks
    cache: Arc<Mutex<Cache<'d>>>,
    /// Precomputed list of IDs which should be looked up in the cache (to reduce lock contention)
    cached_ids: Vec<&'d str>,
}

impl<'d> ChunkVec<'d> {
    /// Parses backup spec JSON and constructs chunk map.
    pub fn decode(input: &'d str, dir: &Path) -> Fallible<Self> {
        let rev: Revision<'d> =
            serde_json::from_str(input).with_context(|_| ExtractError::LoadSpec(input.into()))?;
        let size = rev.size;
        if size % u64::from(CHUNKSIZE) != 0 {
            return Err(ExtractError::UnalignedSize(rev.size).into());
        }
        let cache = Cache::new(&rev.mapping);
        let cached_ids = cache.interesting();
        let ids = rev.into_vec()?;
        Ok(Self {
            dir: dir.into(),
            size,
            ids,
            cache: Arc::new(Mutex::new(cache)),
            cached_ids,
        })
    }

    /// Number of chunks to restore
    #[inline]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Reads chunks from disk and decompresses them. The iterator `idx` controls which chunks are
    /// to be read. Parallel instances of `read` can be fed with disjunct sequences.
    #[allow(clippy::needless_pass_by_value)]
    pub fn read(
        &self,
        idx: Box<dyn Iterator<Item = usize>>,
        uncomp_tx: Sender<RawChunk>,
    ) -> Fallible<()> {
        backend::check(&self.dir).context("Invalid `store' version tag")?;
        for seq in idx {
            let chunk = self.ids[seq];
            uncomp_tx
                .send(RawChunk {
                    seq,
                    data: match chunk {
                        Some(id) => self.cached(seq, id)?,
                        None => None,
                    },
                })
                .context("Failed to send chunk to writer")?;
        }
        Ok(())
    }

    fn cached(&self, seq: usize, id: &'d str) -> Fallible<Option<Vec<u8>>> {
        if !self.cached_ids.iter().any(|i| *i == id) {
            return Ok(Some(self.load_decomp(seq, id)?));
        }
        let mut cache_lck = self.cache.lock();
        Ok(match cache_lck.query(id) {
            Entry::Unknown => {
                let data = self.load_decomp(seq, id)?;
                cache_lck.memorize(id, &data);
                Some(data)
            }
            Entry::Known(data) => Some(data),
            Entry::KnownZero => None,
            Entry::Ignored => Some(self.load_decomp(seq, id)?),
        })
    }

    fn load_decomp(&self, seq: usize, id: &'d str) -> Fallible<Vec<u8>> {
        backend::load(&self.dir, id).and_then(|d| self.decompress(seq, &d))
    }

    fn decompress(&self, seq: usize, compressed: &[u8]) -> Fallible<Vec<u8>> {
        let uncomp = backend::decompress(compressed)
            .with_context(|_| format_err!("Failed to decompress {}", self.fmt_chunk(seq)))?;
        if uncomp.len() != CHUNKSIZE as usize {
            return Err(ChunkError::Missized(seq, uncomp.len()).into());
        }
        Ok(uncomp)
    }

    fn fmt_chunk(&self, seq: usize) -> String {
        format!("chunk #{} ({})", seq, self.ids[seq].unwrap_or("n/a"))
    }
}

#[derive(Fail, Debug, PartialEq, Eq)]
pub enum ChunkError {
    #[fail(display = "Chunk #{} has wrong size: {} B", _0, _1)]
    Missized(usize, usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn check_backend_store() {
        let (raw_tx, _) = unbounded();
        let tmp = TempDir::new("check_backend_store").expect("create tempdir");
        let cv = ChunkVec::decode(r#"{"mapping": {}, "size": 0}"#, &tmp.path())
            .expect("ChunkVec::decode");

        // (1) no `store' file at all
        fs::create_dir(tmp.path().join("chunks")).unwrap();
        assert!(cv.read(Box::new(0..0), raw_tx.clone()).is_err());

        // (2) wrong contents
        fs::write(tmp.path().join("chunks/store"), b"v1").unwrap();
        assert!(cv.read(Box::new(0..0), raw_tx.clone()).is_err());

        // (3) acceptable contents
        fs::write(tmp.path().join("chunks/store"), b"v2").unwrap();
        assert!(cv.read(Box::new(0..0), raw_tx.clone()).is_ok())
    }
}
