//! Chunked backend access.
//!
//! Currently, we support only backy's chunked v2 data store. Other store
//! formats may follow in the future.

mod fadvise;

use self::fadvise::{fadvise, POSIX_FADV_DONTNEED};
use crate::CHUNKSZ_LOG;
use byteorder::{BigEndian, WriteBytesExt};
use lazy_static::lazy_static;
use smallvec::{smallvec, SmallVec};
use std::fs::{read_to_string, File};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Backend metadata not found")]
    NotFound,
    #[error("Unexpected version tag in chunk store: {0}")]
    VersionTag(String),
    #[error("Decompressed chunk has wrong size: {0}B")]
    Missized(usize),
    #[error("Compressed chunk does not start with magic number")]
    Magic,
    #[error("Lzo compression format error")]
    Lzo(#[from] minilzo::Error),
    #[error("I/O error")]
    Io(#[from] io::Error),
}

type Result<T, E = Error> = std::result::Result<T, E>;

lazy_static! {
    pub static ref MAGIC: SmallVec<[u8; 5]> = {
        let mut m = smallvec![0xF0];
        m.write_u32::<BigEndian>(1 << CHUNKSZ_LOG).unwrap();
        m
    };
}

fn decompress(f: &mut File) -> Result<Vec<u8>> {
    let mut compressed = Vec::with_capacity(1 << (CHUNKSZ_LOG - 1));
    f.read_to_end(&mut compressed)?;
    fadvise(&f, POSIX_FADV_DONTNEED);
    // first 5 bytes contain header
    if compressed[0..5] != MAGIC[..] {
        Err(Error::Magic)
    } else {
        Ok(minilzo::decompress(&compressed[5..], 1 << CHUNKSZ_LOG).map_err(Error::Lzo)?)
    }
}

#[derive(Debug, Clone)]
pub struct Backend {
    dir: PathBuf,
}

impl Backend {
    /// Opens chunked backend store and checks store format.
    ///
    /// # Errors
    ///
    /// Fails with Error::NotFound or Error::VersionTag if no
    /// valid version tag is present in the store directory.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        let s = read_to_string(dir.join("chunks/store")).map_err(|_| Error::NotFound)?;
        let version_tag = s.trim();
        if version_tag != "v2" {
            Err(Error::VersionTag(version_tag.to_owned()))
        } else {
            Ok(Self {
                dir: dir.to_owned(),
            })
        }
    }

    /// Computes file name for chunk with ID (relative to backup base
    /// directory).
    pub fn filename(&self, id: &str) -> PathBuf {
        self.dir
            .join(format!("chunks/{}/{}.chunk.lzo", &id[0..2], id))
    }

    /// Loads compressed chunk identified by `id`. The chunk is decompressed
    /// on the fly and returned as raw data.
    pub fn load(&self, id: &str) -> Result<Vec<u8>> {
        let data = decompress(&mut File::open(self.filename(id))?)?;
        if data.len() != 1 << CHUNKSZ_LOG {
            Err(Error::Missized(data.len()))
        } else {
            Ok(data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::*;

    use std::collections::hash_map::DefaultHasher;
    use std::fs::{create_dir, metadata, set_permissions, write, OpenOptions};
    use std::hash::Hasher;
    use tempdir::TempDir;

    #[test]
    fn check_backend_store() -> Result<()> {
        let tmp = TempDir::new("check_backend_store")?;

        // (1) no `store' file at all
        create_dir(tmp.path().join("chunks"))?;
        assert!(Backend::open(tmp.path()).is_err());

        // (2) wrong contents
        write(tmp.path().join("chunks/store"), b"v1")?;
        assert!(Backend::open(tmp.path()).is_err());

        // (3) acceptable contents
        write(tmp.path().join("chunks/store"), b"v2")?;
        assert!(Backend::open(tmp.path()).is_ok());
        Ok(())
    }

    #[test]
    fn decode_chunk() -> Result<()> {
        let s = store_tar();
        let be = Backend::open(s.path())?;
        let mut h = DefaultHasher::new();
        h.write(&be.load("4db6e194fd398e8edb76e11054d73eb0")?);
        Ok(assert_eq!(h.finish(), 4783617329521481478))
    }

    #[test]
    fn corrupted_chunk() -> Result<()> {
        let s = store_tar();
        let file = s
            .path()
            .join("chunks/4d/4db6e194fd398e8edb76e11054d73eb0.chunk.lzo");
        let mut p = metadata(&file)?.permissions();
        p.set_readonly(false);
        set_permissions(&file, p)?;
        OpenOptions::new().write(true).open(&file)?.set_len(1000)?;
        let be = Backend::open(s.path())?;
        match be.load("4db6e194fd398e8edb76e11054d73eb0") {
            Err(Error::Lzo(_)) => Ok(()),
            other => panic!("unexpected: {:?}", other),
        }
    }
}
// TODO:
// Test short chunk
