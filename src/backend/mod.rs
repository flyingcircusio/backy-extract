//! Chunked backend access.
//!
//! Currently, we support only backy's chunked v2 data store. Other store
//! formats may follow in the future.

#[cfg(feature = "fuse_driver")]
mod rev;
#[cfg(feature = "fuse_driver")]
pub use rev::{Error as RevError, Rev, RevId};

mod fadvise;

use crate::CHUNKSZ;

use byteorder::{BigEndian, WriteBytesExt};
use lazy_static::lazy_static;
use log::debug;
use smallvec::{smallvec, SmallVec};
use std::convert::TryFrom;
use std::fs::{self, File};
use std::io::{self, Read, Write};
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
        m.write_u32::<BigEndian>(u32::try_from(CHUNKSZ).unwrap())
            .unwrap();
        m
    };
}

fn decompress(f: &mut File) -> Result<Vec<u8>> {
    debug!("read lzo from {:?}", f);
    let mut buf = Vec::with_capacity(f.metadata()?.len() as usize);
    f.read_to_end(&mut buf)?;
    // first 5 bytes contain header
    if buf[0..5] != MAGIC[..] {
        Err(Error::Magic)
    } else {
        Ok(minilzo::decompress(&buf[5..], CHUNKSZ)?)
    }
}

#[derive(Debug, Clone)]
pub struct Backend {
    pub dir: PathBuf,
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
        let s = fs::read_to_string(dir.join("chunks/store")).map_err(|_| Error::NotFound)?;
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
    ///
    /// # Errors
    ///
    /// Fails with Error::Missized if decompressed data does not fix exactly into a chunk.
    pub fn load(&self, id: &str) -> Result<Vec<u8>> {
        let mut f = File::open(self.filename(id))?;
        let data = decompress(&mut f)?;
        fadvise::dontneed(f);

        if data.len() != CHUNKSZ {
            Err(Error::Missized(data.len()))
        } else {
            Ok(data)
        }
    }

    #[allow(unused)]
    pub fn save(&self, id: &str, buf: &[u8]) -> Result<()> {
        if buf.len() != CHUNKSZ {
            return Err(Error::Missized(buf.len()));
        }
        let dir = self.dir.join(format!("chunks/{}", &id[0..2]));
        if fs::metadata(&dir).is_err() {
            fs::create_dir(dir)?;
        }
        let mut f = File::create(self.filename(id))?;
        debug!("write lzo to {:?}", f);
        f.write_all(&MAGIC)?;
        f.write_all(&minilzo::compress(buf)?)?;
        fadvise::dontneed(f);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helper::*;

    use std::collections::hash_map::DefaultHasher;
    use std::fs::{create_dir, metadata, read, set_permissions, write, OpenOptions};
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
    fn encode_chunk() {
        let s = store_tar();
        let be = Backend::open(s.path()).unwrap();
        let buf = be.load("4db6e194fd398e8edb76e11054d73eb0").unwrap();
        be.save("00000000000000000000000000000000", &buf).unwrap();
        assert_eq!(
            read(&format!(
                "{}/chunks/{}/{}.chunk.lzo",
                s.path().display(),
                "4d",
                "4db6e194fd398e8edb76e11054d73eb0"
            ))
            .unwrap(),
            read(&format!(
                "{}/chunks/{}/{}.chunk.lzo",
                s.path().display(),
                "00",
                "00000000000000000000000000000000"
            ))
            .unwrap()
        )
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
