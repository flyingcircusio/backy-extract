mod backend;

use super::{CompressedChunk, ExtractError, RawChunk, CHUNKSIZE};
use crossbeam::channel::{Receiver, Sender};
use failure::{format_err, Fallible, ResultExt};
use num_cpus;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

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
    pub id: Vec<Option<&'d str>>,
}

impl<'d> ChunkVec<'d> {
    pub fn decode(input: &'d str, dir: &Path) -> Fallible<Self> {
        let rev: Revision<'_> =
            serde_json::from_str(input).context(ExtractError::LoadSpec(input.into()))?;
        let size = rev.size;
        if size % u64::from(CHUNKSIZE) != 0 {
            return Err(ExtractError::UnalignedSize(rev.size).into());
        }
        Ok(Self {
            dir: dir.into(),
            size,
            id: rev.into_vec()?,
        })
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn read(&self, comp: Sender<CompressedChunk>, uncomp: Sender<RawChunk>) -> Fallible<()> {
        backend::check(&self.dir).context("Invalid `store' version tag")?;
        for (seq, f) in self.id.iter().enumerate() {
            match f {
                Some(id) => comp.send(CompressedChunk {
                    seq,
                    data: backend::load(&self.dir, id)?,
                })?,
                None => uncomp.send(RawChunk { seq, data: None })?,
            };
        }
        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn decompress(&self, rx: Receiver<CompressedChunk>, out: Sender<RawChunk>) -> Fallible<()> {
        for c in rx {
            let uncomp = backend::decompress(&c.data)
                .with_context(|_| format_err!("Failed to decompress {}", self.fmt_chunk(c.seq)))?;
            if uncomp.len() != CHUNKSIZE as usize {
                return Err(ExtractError::BackupFormat(format!(
                    "uncompressed {} has wrong length",
                    self.fmt_chunk(c.seq)
                ))
                .into());
            }
            out.send(RawChunk {
                seq: c.seq,
                data: Some(uncomp),
            })
            .context("Failed to send decompressed chunk")?;
        }
        Ok(())
    }

    fn fmt_chunk(&self, seq: usize) -> String {
        format!("chunk #{} ({})", seq, self.id[seq].unwrap_or("n/a"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn check_backend_store() {
        let (comp_tx, _) = unbounded();
        let (raw_tx, _) = unbounded();
        let tmp = TempDir::new("check_backend_store").expect("create tempdir");
        let cv = ChunkVec::decode(r#"{"mapping": {}, "size": 0}"#, &tmp.path())
            .expect("ChunkVec::decode");

        // (1) no `store' file at all
        fs::create_dir(tmp.path().join("chunks")).unwrap();
        assert!(cv.read(comp_tx.clone(), raw_tx.clone()).is_err());

        // (2) wrong contents
        fs::write(tmp.path().join("chunks/store"), b"v1").unwrap();
        assert!(cv.read(comp_tx.clone(), raw_tx.clone()).is_err());

        // (3) acceptable contents
        fs::write(tmp.path().join("chunks/store"), b"v2").unwrap();
        assert!(cv.read(comp_tx.clone(), raw_tx.clone()).is_ok())
    }
}
