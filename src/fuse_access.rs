//! Fuse-driven access to revisions with in-memory COW

use crate::backend::{self, Backend};
use crate::chunkvec::{ChunkID, RevisionMap};
use crate::{pos2chunk, RevID, CHUNKSZ_LOG, ZERO_CHUNK};
use chrono::{DateTime, TimeZone, Utc};
use fnv::FnvHashMap as HashMap;
use log::{debug, info};
use serde::{Deserialize, Deserializer};
use std::cmp::min;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

static ID_SEQ: AtomicU64 = AtomicU64::new(4);

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IO(#[from] io::Error),
    #[error("Invalid data in .rev file '{}'", path.display())]
    ParseRev {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("Invalid data in chunk map file '{}'", path.display())]
    ParseMap {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("Failed to open data store")]
    Backend(#[from] backend::Error),
    #[error("Failed to load data chunk {chunk_id:?}")]
    BackendLoad {
        chunk_id: ChunkID,
        source: backend::Error,
    },
    #[error("Unknown backend_type '{}' found in {}", betype, path.display())]
    WrongType { betype: String, path: PathBuf },
    #[error("Invalid revision file name '{}'", .0.display())]
    InvalidName(PathBuf),
    #[error("'{}' contains no revision or no chunks - is this really a backy directory?",
            .0.display())]
    NoRevisions(PathBuf),
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Default)]
struct Page(Box<[u8]>);

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page[...]")
    }
}

impl Deref for Page {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Default)]
struct Cache {
    seq: usize,
    data: Page,
}

impl Cache {
    fn update(&mut self, seq: usize, data: Page) {
        self.seq = seq;
        self.data = data;
    }
}

type Chunks = Vec<Option<ChunkID>>;

fn revid_de<'de, D>(deserializer: D) -> Result<RevID, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(RevID::from_string(String::deserialize(deserializer)?))
}

static TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f%z";
fn timestamp_de<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    Utc.datetime_from_str(&String::deserialize(deserializer)?, TIMESTAMP_FORMAT)
        .map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
pub struct RevStats {
    pub bytes_written: u64,
    pub duration: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rev {
    pub backend_type: String,
    #[serde(deserialize_with = "revid_de")]
    pub parent: RevID,
    pub stats: RevStats,
    #[serde(deserialize_with = "timestamp_de")]
    pub timestamp: DateTime<Utc>,
    pub trust: String,
    #[serde(deserialize_with = "revid_de")]
    pub uuid: RevID,
}

impl Rev {
    fn load<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let map = dir.as_ref().join(id.as_ref());
        let rev = map.with_extension("rev");
        let r: Self = serde_yaml::from_reader(fs::File::open(&rev)?).map_err(|source| {
            Error::ParseRev {
                path: rev.to_path_buf(),
                source,
            }
        })?;
        if r.backend_type != "chunked" {
            return Err(Error::WrongType {
                betype: r.backend_type.to_owned(),
                path: rev.to_owned(),
            });
        }
        Ok(r)
    }
}

#[derive(Debug, Clone)]
pub struct FuseAccess {
    pub path: PathBuf,
    pub rev: Rev,
    pub size: u64,
    map: Chunks,
    backend: Backend,
    cache: Cache,
    cow: HashMap<usize, Page>,
}

const OFFSET_MASK: u64 = (1 << CHUNKSZ_LOG) - 1;

impl FuseAccess {
    fn new<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let dir = dir.as_ref();
        let path = dir.join(id.as_ref());
        let rev = Rev::load(dir, id)?;
        let backend = Backend::open(dir)?;
        Ok(Self {
            path,
            rev,
            size: 0,     // initialized by load()
            map: vec![], // initialized by load()
            backend,
            cache: Default::default(),
            cow: HashMap::default(),
        })
    }

    #[cfg(test)]
    fn load<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let mut res = Self::new(dir, id)?;
        res.load_map()?;
        Ok(res)
    }

    #[allow(unused)]
    /// Returns the local path name of the map/revision file (relative to basedir)
    pub fn name(&self) -> &OsStr {
        OsStr::new(
            self.path
                .file_name()
                .expect("Internal error: revision without path"),
        )
    }

    #[allow(unused)]
    /// Loads chunk map from basedir
    ///
    /// Returns number of chunks found in the map. This function is idempotent: If the map is
    /// already initialized, nothing happens.
    pub fn load_if_empty(&mut self) -> Result<()> {
        if self.map.is_empty() {
            let n = self.load_map()?;
            debug!("Loaded {} chunks from chunk map {:?}", n, self.name());
        }
        Ok(())
    }

    fn load_map(&mut self) -> Result<usize> {
        let revmap_s = fs::read_to_string(&self.path)?;
        let revmap: RevisionMap =
            serde_json::from_str(&revmap_s).map_err(|source| Error::ParseMap {
                path: self.path.to_owned(),
                source,
            })?;
        self.size = revmap.size;
        self.map = revmap.into_iter().map(|(_seq, id)| id).collect();
        if let Some(initial_cache) = self.load_page(0) {
            self.cache.update(0, initial_cache?);
        }
        Ok(self.map.len())
    }

    fn load_page(&self, seq: usize) -> Option<Result<Page>> {
        self.map[seq].as_ref().map(|chunk_id| {
            let data =
                self.backend
                    .load(chunk_id.as_str())
                    .map_err(|e| Error::BackendLoad {
                        chunk_id: chunk_id.clone(),
                        source: e,
                    })?;
            Ok(Page(data.into_boxed_slice()))
        })
    }

    #[allow(unused)]
    pub fn read_at(&mut self, offset: u64, size: usize) -> Result<&[u8]> {
        if offset > self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read beyond end of image",
            )
            .into());
        } else if offset == self.size {
            return Ok(&[]);
        }
        let seq = pos2chunk(offset);
        let off = (offset & OFFSET_MASK) as usize;
        let end = min(off + size, 1 << CHUNKSZ_LOG);
        if let Some(page) = self.cow.get(&seq) {
            return Ok(&page[off..end]);
        }
        if self.map[seq].is_some() {
            if seq == self.cache.seq {
                return Ok(&self.cache.data[off..end]);
            }
            info!("{:?}: load seq {}", self.name(), seq);
            self.cache.update(seq, self.load_page(seq).unwrap()?);
            Ok(&self.cache.data[off..end])
        } else {
            Ok(&ZERO_CHUNK[off..end])
        }
    }

    #[allow(unused)]
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        if offset >= self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "write beyond end of image",
            )
            .into());
        }
        let seq = pos2chunk(offset);
        let off = (offset & OFFSET_MASK) as usize;
        // split large writes that go over a page boundary
        if pos2chunk(offset + buf.len() as u64 - 1) != seq {
            let in_first_chunk = (1 << CHUNKSZ_LOG) - off;
            return self
                .write_at(offset, &buf[..in_first_chunk])
                .and_then(|written| {
                    Ok(written
                        + self.write_at(
                            offset + in_first_chunk as u64,
                            &&buf[in_first_chunk..],
                        )?)
                });
        }
        assert!(off + buf.len() <= 1 << CHUNKSZ_LOG);
        if let Some(page) = self.cow.get_mut(&seq) {
            page.0[off..off + buf.len()].clone_from_slice(buf);
        } else {
            let mut p = Vec::from(self.read_at(offset & !OFFSET_MASK, 1 << CHUNKSZ_LOG)?);
            info!("{:?}: clone into cow", self.name());
            p[off..off + buf.len()].clone_from_slice(buf);
            self.cow.insert(seq, Page(p.into_boxed_slice()));
        }
        Ok(buf.len())
    }
}

#[derive(Debug, Clone, Default)]
pub struct FuseDirectory {
    pub basedir: PathBuf,
    revs: HashMap<u64, FuseAccess>,
}

impl FuseDirectory {
    #[allow(unused)]
    pub fn init<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        let mut d = Self {
            basedir: dir.to_owned(),
            revs: HashMap::default(),
        };
        for entry in fs::read_dir(&dir)? {
            let e = entry?;
            let p = PathBuf::from(e.file_name());
            if p.extension().unwrap_or_default() == "rev" {
                let ino = ID_SEQ.fetch_add(1, Ordering::SeqCst);
                let rid = p
                    .file_stem()
                    .ok_or_else(|| Error::InvalidName(p.to_owned()))?
                    .to_str()
                    .ok_or_else(|| Error::InvalidName(p.to_owned()))?;
                let f = FuseAccess::new(&dir, rid)?;
                d.revs.insert(ino, f);
            }
        }
        if !d.is_empty() && dir.join("chunks").exists() {
            Ok(d)
        } else {
            Err(Error::NoRevisions(PathBuf::from(dir)))
        }
    }

    #[allow(unused)]
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &FuseAccess)> {
        self.revs.iter()
    }

    #[allow(unused)]
    pub fn get(&self, ino: u64) -> Option<&FuseAccess> {
        self.revs.get(&ino)
    }

    #[allow(unused)]
    pub fn get_mut(&mut self, ino: u64) -> Option<&mut FuseAccess> {
        self.revs.get_mut(&ino)
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.revs.len()
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.revs.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::{cid, rid, store};
    use maplit::hashmap;

    #[test]
    fn initialize_rev() -> Result<()> {
        let s = store(hashmap! {rid("pqEKi7Jfq4bps3NVNEU49K") => vec![]});
        let rev = Rev::load(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        assert_eq!(
            rev.timestamp,
            Utc.ymd(2019, 11, 14).and_hms_milli(14, 21, 18, 289)
        );
        assert_eq!(rev.uuid, rid("pqEKi7Jfq4bps3NVNEU49K"));
        Ok(())
    }

    #[test]
    fn init_map() -> Result<()> {
        let s = store(hashmap! {
            rid("pqEKi7Jfq4bps3NVNEU49K") => vec![
                Some((cid("4355a46b19d348dc2f57c046f8ef63d4"), vec![1u8; 1<<CHUNKSZ_LOG])),
                None
            ]
        });
        let ra = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        assert_eq!(
            ra.map,
            &[Some(cid("4355a46b19d348dc2f57c046f8ef63d4")), None]
        );
        Ok(())
    }

    #[test]
    fn read_data() -> Result<()> {
        let s = store(hashmap! {
            rid("pqEKi7Jfq4bps3NVNEU49K") => vec![
                Some((cid("4355a46b19d348dc2f57c046f8ef63d4"), vec![1u8; 1<<CHUNKSZ_LOG])),
                None,
                Some((cid("1121cfccd5913f0a63fec40a6ffd44ea"), vec![3u8; 1<<CHUNKSZ_LOG])),
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        let read_size = 1 << (CHUNKSZ_LOG - 3);
        assert_eq!(
            *fuse.read_at(2 << CHUNKSZ_LOG, read_size)?,
            *vec![3u8; read_size]
        );
        assert_eq!(fuse.cache.seq, 2);
        assert_eq!(*fuse.cache.data, *vec![3u8; 1 << CHUNKSZ_LOG]);
        // empty chunk -> zeroes
        assert_eq!(
            *fuse.read_at(1 << CHUNKSZ_LOG, read_size)?,
            *vec![0; read_size]
        );
        assert_eq!(fuse.cache.seq, 2);
        // another chunk
        assert!(fuse.read_at(0, read_size).is_ok());
        assert_eq!(fuse.cache.seq, 0);
        // read over the end -> short read
        assert_eq!(
            fuse.read_at((3 << CHUNKSZ_LOG) - 32, read_size)?,
            &[3u8; 32]
        );
        // read at end -> []
        assert!(fuse.read_at(3 << CHUNKSZ_LOG, read_size)?.is_empty());
        // offset > len
        assert!(fuse.read_at(3 << CHUNKSZ_LOG + 1, read_size).is_err());
        // nothing should have ended up in the COW map
        assert!(fuse.cow.is_empty());
        Ok(())
    }

    #[test]
    fn all_zero_chunks() -> Result<()> {
        let s = store(hashmap! {
            rid("pqEKi7Jfq4bps3NVNEU400") => vec![
                None,
                None,
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU400")?;
        assert_eq!(
            *fuse.read_at(0, 1 << CHUNKSZ_LOG)?,
            *vec![0; 1 << CHUNKSZ_LOG]
        );
        assert_eq!(
            *fuse.read_at(1 << CHUNKSZ_LOG, 1 << CHUNKSZ_LOG)?,
            *vec![0; 1 << CHUNKSZ_LOG]
        );
        Ok(())
    }

    #[test]
    fn read_offset() -> Result<()> {
        let mut data = vec![11u8; 1 << CHUNKSZ_LOG];
        data[..10].clone_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s = store(hashmap! {
            rid("MmE1MThjMDZmMWQ5Y2JkMG") => vec![
                Some((cid("25d4f2a86deb5e2574bb3210b67bb24f"), vec![1u8; 1<<CHUNKSZ_LOG])),
                Some((cid("a1fb50e6c86fae1679ef3351296fd671"), data)),
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "MmE1MThjMDZmMWQ5Y2JkMG")?;
        assert_eq!(
            fuse.read_at((1 << CHUNKSZ_LOG) + 4, 8)?,
            &[4, 5, 6, 7, 8, 9, 11, 11]
        );
        Ok(())
    }

    #[test]
    fn write_cow() -> Result<()> {
        let mut data = vec![11u8; 1 << CHUNKSZ_LOG];
        data[..10].clone_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s = store(hashmap! {
            rid("XmE1MThjMDZmMWQ5Y2JkMG") => vec![
                Some((cid("00d4f2a86deb5e2574bb3210b67bb24f"), vec![10u8; 1<<CHUNKSZ_LOG])),
                Some((cid("01fb50e6c86fae1679ef3351296fd671"), data))
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "XmE1MThjMDZmMWQ5Y2JkMG")?;
        assert!(fuse.cow.is_empty());
        // write at beginning boundary
        assert_eq!(fuse.write_at(0, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(0, 5)?, &[0, 1, 2, 3, 10]);
        // write at page boundary
        assert_eq!(fuse.write_at((1 << CHUNKSZ_LOG) - 4, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at((1 << CHUNKSZ_LOG) - 5, 5)?, &[10, 0, 1, 2, 3]);
        // -> should not have loaded page 1 into the cache!
        assert_eq!(fuse.cache.seq, 0);
        // write over page boundary
        assert_eq!(fuse.write_at((1 << CHUNKSZ_LOG) - 2, &[20, 21, 22, 23])?, 4);
        assert_eq!(
            fuse.read_at((1 << CHUNKSZ_LOG) - 4, 4)?,
            &[
                0, 1, // from last write
                20, 21 // newly written to page 0
            ]
        );
        assert_eq!(
            fuse.read_at(1 << CHUNKSZ_LOG, 5)?,
            &[
                22, 23, // newly written to page 1
                2, 3, 4 // original contents of page 1
            ]
        );
        // write over EOF XXX
        assert!(fuse
            .write_at((2 << CHUNKSZ_LOG) - 2, &[4, 5, 6, 7])
            .is_err());
        Ok(())
    }

    #[test]
    fn write_cow_empty_page() -> Result<()> {
        let s = store(hashmap! {rid("YmE1MThjMDZmMWQ5Y2JkMG") => vec![None]});
        let mut fuse = FuseAccess::load(s.path(), "YmE1MThjMDZmMWQ5Y2JkMG")?;
        assert_eq!(fuse.write_at(2, &[1])?, 1);
        assert_eq!(fuse.read_at(0, 5)?, &[0, 0, 1, 0, 0]);
        Ok(())
    }

    // XXX Test: missing files
    // XXX Test: format error
}
