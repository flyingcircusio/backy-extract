//! Fuse-driven access to revisions with in-memory COW

use crate::backend::{self, Backend};
use crate::chunkvec::{ChunkID, RevisionMap};
use crate::{pos2chunk, RevID, CHUNKSZ_LOG, ZERO_CHUNK};
use chrono::{DateTime, TimeZone, Utc};
use fnv::FnvHashMap as HashMap;
use serde::{Deserialize, Deserializer};
use std::cmp::min;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io;
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
    #[error("Invalid revision file name '{}'", path.display())]
    InvalidName { path: PathBuf },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
struct Page(Box<[u8]>);

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page[...]")
    }
}

#[derive(Debug, Clone)]
struct Cache {
    seq: usize,
    data: Page,
}

impl Cache {
    fn new(seq: usize, data: Page) -> Self {
        Self { seq, data }
    }

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
        let r: Self =
            serde_yaml::from_reader(fs::File::open(&rev)?).map_err(|source| Error::ParseRev {
                path: rev.to_path_buf(),
                source,
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

fn copy(target: &mut [u8], source: &[u8], offset: usize) -> usize {
    let n = min(target.len(), source.len() - offset);
    target[0..n].clone_from_slice(&source[offset..(offset + n)]);
    n
}

#[derive(Debug, Clone)]
pub struct FuseAccess {
    pub path: PathBuf,
    pub rev: Rev,
    pub size: u64,
    map: Chunks,
    backend: Backend,
    cache: Option<Cache>,
    cow: HashMap<usize, Page>,
}

const OFFSET_MASK: u64 = (1 << CHUNKSZ_LOG) - 1;

impl FuseAccess {
    fn new<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let dir = dir.as_ref();
        let path = dir.join(id.as_ref());
        let rev = Rev::load(dir, id)?;
        let backend = Backend::open(dir)?;
        // XXX lazy map loading?
        let revmap_s = fs::read_to_string(&path)?;
        let revmap: RevisionMap =
            serde_json::from_str(&revmap_s).map_err(|source| Error::ParseMap {
                path: path.to_owned(),
                source,
            })?;
        let size = revmap.size;
        let map = revmap.into_iter().map(|(_seq, id)| id).collect();
        Ok(Self {
            path,
            rev,
            size,
            map,
            backend,
            cache: None,
            cow: HashMap::default(),
        })
    }

    #[allow(unused)]
    pub fn file_name(&self) -> &OsStr {
        OsStr::new(
            self.path
                .file_name()
                .expect("Internal error: revision without path"),
        )
    }

    #[allow(unused)]
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if offset > self.size {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "read beyond end of image").into(),
            );
        } else if offset == self.size {
            return Ok(0);
        }
        let seq = pos2chunk(offset);
        let off = (offset & OFFSET_MASK) as usize;
        if let Some(page) = self.cow.get(&seq) {
            return Ok(copy(buf, &page.0, off));
        }
        if let Some(cache) = &self.cache {
            if seq == cache.seq {
                return Ok(copy(buf, &cache.data.0, off));
            }
        }
        if let Some(chunk_id) = self.map[seq].clone() {
            let data = self
                .backend
                .load(chunk_id.as_str())
                .map_err(|e| Error::BackendLoad {
                    chunk_id,
                    source: e,
                })?;
            let n = copy(buf, &data, off);
            if let Some(cache) = &mut self.cache {
                cache.update(seq, Page(data.into_boxed_slice()));
            } else {
                self.cache = Some(Cache::new(seq, Page(data.into_boxed_slice())));
            }
            Ok(n)
        } else {
            Ok(copy(buf, &ZERO_CHUNK, 0))
        }
    }

    #[allow(unused)]
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        if offset >= self.size {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "write beyond end of image").into(),
            );
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
                        + self.write_at(offset + in_first_chunk as u64, &&buf[in_first_chunk..])?)
                });
        }
        assert!(off + buf.len() <= 1 << CHUNKSZ_LOG);
        if let Some(page) = self.cow.get_mut(&seq) {
            page.0[off..off + buf.len()].clone_from_slice(buf);
        } else {
            let mut p = vec![0u8; 1 << CHUNKSZ_LOG];
            self.read_at(&mut p, offset)?;
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
        let mut d = Self {
            basedir: dir.as_ref().to_owned(),
            revs: HashMap::default(),
        };
        for entry in fs::read_dir(&dir)? {
            let e = entry?;
            let p = PathBuf::from(e.file_name());
            if p.extension().unwrap_or_default() == "rev" {
                let ino = ID_SEQ.fetch_add(1, Ordering::SeqCst);
                let rid = p
                    .file_stem()
                    .ok_or_else(|| Error::InvalidName { path: p.to_owned() })?
                    .to_str()
                    .ok_or_else(|| Error::InvalidName { path: p.to_owned() })?;
                let f = FuseAccess::new(&dir, rid)?;
                d.revs.insert(ino, f);
            }
        }
        Ok(d)
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
        let ra = FuseAccess::new(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
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
        let mut fuse = FuseAccess::new(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        let read_size = 1 << (CHUNKSZ_LOG - 3);
        let mut buf = vec![0; read_size];
        assert!(fuse.cache.is_none());
        assert_eq!(fuse.read_at(&mut buf, 0)?, read_size);
        assert_eq!(&buf, &vec![1u8; read_size]);
        assert_eq!(fuse.cache.as_ref().map(|c| c.seq), Some(0));
        // empty chunk -> zeroes
        assert_eq!(fuse.read_at(&mut buf, 1 << CHUNKSZ_LOG)?, read_size);
        assert_eq!(&buf, &vec![0; read_size]);
        assert_eq!(fuse.cache.as_ref().map(|c| c.seq), Some(0));
        // another chunk
        assert_eq!(fuse.read_at(&mut buf, 2 << CHUNKSZ_LOG)?, read_size);
        assert_eq!(&buf, &vec![3u8; read_size]);
        assert_eq!(fuse.cache.as_ref().map(|c| c.seq), Some(2));
        // read over the end -> short read
        assert_eq!(fuse.read_at(&mut buf, (3 << CHUNKSZ_LOG) - 32)?, 32);
        assert_eq!(&buf[0..32], &[3u8; 32]);
        // read at end -> 0
        assert_eq!(fuse.read_at(&mut buf, 3 << CHUNKSZ_LOG)?, 0);
        // offset > len
        assert!(fuse.read_at(&mut buf, (3 << CHUNKSZ_LOG) + 1).is_err());
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
        let mut fuse = FuseAccess::new(s.path(), "pqEKi7Jfq4bps3NVNEU400")?;
        let mut buf = vec![0; 1 << CHUNKSZ_LOG];
        assert_eq!(fuse.read_at(&mut buf, 0)?, 1 << CHUNKSZ_LOG);
        assert_eq!(&buf, &vec![0; 1 << CHUNKSZ_LOG]);
        assert_eq!(fuse.read_at(&mut buf, 1 << CHUNKSZ_LOG)?, 1 << CHUNKSZ_LOG);
        assert_eq!(&buf, &vec![0; 1 << CHUNKSZ_LOG]);
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
        let mut fuse = FuseAccess::new(s.path(), "MmE1MThjMDZmMWQ5Y2JkMG")?;
        let mut buf = [0u8; 8];
        assert_eq!(fuse.read_at(&mut buf, (1 << CHUNKSZ_LOG) + 4)?, 8);
        assert_eq!(&buf, &[4, 5, 6, 7, 8, 9, 11, 11]);
        Ok(())
    }

    #[test]
    fn write_cow() -> Result<()> {
        let s = store(hashmap! {
            rid("XmE1MThjMDZmMWQ5Y2JkMG") => vec![
                Some((cid("00d4f2a86deb5e2574bb3210b67bb24f"), vec![10u8; 1<<CHUNKSZ_LOG])),
                Some((cid("01fb50e6c86fae1679ef3351296fd671"), vec![11u8; 1<<CHUNKSZ_LOG])),
            ]
        });
        let mut fuse = FuseAccess::new(s.path(), "XmE1MThjMDZmMWQ5Y2JkMG")?;
        let mut buf = vec![0; 5];
        assert!(fuse.cow.is_empty());
        // write at beginning boundary
        assert_eq!(fuse.write_at(0, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(&mut buf, 0)?, 5);
        assert_eq!(&buf, &[0, 1, 2, 3, 10]);
        // write at page boundary
        assert_eq!(fuse.write_at((1 << CHUNKSZ_LOG) - 4, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(&mut buf, (1 << CHUNKSZ_LOG) - 5)?, 5);
        assert_eq!(&buf, &[10, 0, 1, 2, 3]);
        // -> should not have loaded page 1 into the cache!
        assert_eq!(fuse.cache.as_ref().map(|c| c.seq), Some(0));
        // write over page boundary
        assert_eq!(fuse.write_at((1 << CHUNKSZ_LOG) - 2, &[4, 5, 6, 7])?, 4);
        assert_eq!(fuse.read_at(&mut buf, (1 << CHUNKSZ_LOG) - 4)?, 4);
        assert_eq!(
            &buf[0..4],
            &[
                0, 1, // from last write
                4, 5, // newly written to page 0
            ]
        );
        assert_eq!(fuse.read_at(&mut buf, 1 << CHUNKSZ_LOG)?, 5);
        assert_eq!(
            &buf,
            &[
                6, 7, // newly written to page 1
                11, 11, 11
            ]
        ); // original contents of page 1
           // write over EOF
        assert!(fuse
            .write_at((2 << CHUNKSZ_LOG) - 2, &[4, 5, 6, 7])
            .is_err());
        Ok(())
    }

    #[test]
    fn write_cow_empty_page() -> Result<()> {
        let s = store(hashmap! {rid("YmE1MThjMDZmMWQ5Y2JkMG") => vec![None]});
        let mut fuse = FuseAccess::new(s.path(), "YmE1MThjMDZmMWQ5Y2JkMG")?;
        assert_eq!(fuse.write_at(2, &[1])?, 1);
        let mut buf = vec![0; 5];
        assert_eq!(fuse.read_at(&mut buf, 0)?, 5);
        assert_eq!(&buf, &[0, 0, 1, 0, 0]);
        Ok(())
    }

    // XXX Test: missing files
    // XXX Test: format error
}
