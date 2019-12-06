//! Fuse-driven access to revisions with in-memory COW

use crate::backend::{self, Backend};
use crate::chunkvec::{ChunkID, RevisionMap};
use crate::{chunk2pos, pos2chunk, RevID, CHUNKSZ_LOG, ZERO_CHUNK};
use chrono::{DateTime, TimeZone, Utc};
use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use log::{debug, info};
use serde::{Deserialize, Deserializer};
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io;
use std::ops::{Deref, DerefMut};
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

type Chunks = Vec<Option<ChunkID>>;

#[derive(Clone, Default)]
struct Page(Box<[u8]>);

impl Page {
    /// Fetches a chunk from the backend
    ///
    /// Panics if the requested page is a zero page and thus not present in the backend.
    fn load(map: &Chunks, backend: &Backend, seq: usize) -> Result<Self> {
        let cid = map[seq]
            .clone()
            .expect(&format!("failed to locate chunk {} in map", seq));
        Ok(backend
            .load(cid.as_str())
            .map_err(|e| Error::BackendLoad {
                chunk_id: cid,
                source: e,
            })?
            .into())
    }
}

impl From<Vec<u8>> for Page {
    fn from(data: Vec<u8>) -> Self {
        Self(data.into_boxed_slice())
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            write!(f, "Page[]")
        } else {
            write!(f, "Page[...]")
        }
    }
}

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

#[derive(Debug, Clone, Default)]
struct ROCache {
    seq: usize,
    data: Page,
}

impl ROCache {
    fn matches(&self, seq: usize) -> bool {
        self.seq == seq && !self.data.is_empty()
    }

    fn update(&mut self, seq: usize, data: Page) {
        self.seq = seq;
        self.data = data;
    }
}

impl Deref for ROCache {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.data.deref()
    }
}

#[derive(Debug, Clone)]
pub struct FuseAccess {
    pub path: PathBuf,
    pub name: OsString,
    pub rev: Rev,
    pub size: u64,
    map: Chunks,
    backend: Backend,
    current: ROCache,
    seen_before: HashSet<usize>,
    cow: HashMap<usize, Page>,
}

const OFFSET_MASK: u64 = (1 << CHUNKSZ_LOG) - 1;

impl FuseAccess {
    fn new<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let dir = dir.as_ref();
        let path = dir.join(id.as_ref());
        let name = OsString::from(path.file_name().expect("revision without path"));
        let rev = Rev::load(dir, id)?;
        let backend = Backend::open(dir)?;
        Ok(Self {
            path,
            name,
            rev,
            size: 0,             // initialized by load()
            map: Vec::default(), // initialized by load()
            backend,
            current: ROCache::default(),
            seen_before: HashSet::default(),
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
    /// Loads chunk map from basedir
    ///
    /// Returns number of chunks found in the map. This function is idempotent: If the map is
    /// already initialized, nothing happens.
    pub fn load_if_empty(&mut self) -> Result<()> {
        if self.map.is_empty() {
            let n = self.load_map()?;
            debug!("Loaded {} chunks from chunk map {:?}", n, self.name);
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
        Ok(self.map.len())
    }

    #[allow(unused)]
    pub fn read_at(&mut self, offset: u64, size: usize) -> Result<&[u8]> {
        match offset {
            // XXX std::cmp::Ordering
            o if o == self.size => Ok(&[]),
            o if o > self.size => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read beyond end of image",
            )
            .into()),
            _ => {
                let off = (offset & OFFSET_MASK) as usize;
                self.read(pos2chunk(offset), off, min(off + size, 1 << CHUNKSZ_LOG))
            }
        }
    }

    fn read(&mut self, seq: usize, off: usize, end: usize) -> Result<&[u8]> {
        match self.cow.entry(seq) {
            Entry::Vacant(e) => {
                if !self.current.matches(seq) && self.seen_before.contains(&seq) {
                    info!("{:?}: cache #{} (seen twice)", self.name, seq);
                    let page = Page::load(&self.map, &self.backend, seq)?;
                    return Ok(&e.insert(page)[off..end]);
                }
            }
            Entry::Occupied(e) => return Ok(&e.into_mut()[off..end]),
        }
        if self.map[seq].is_some() {
            if self.current.matches(seq) {
                return Ok(&self.current[off..end]);
            }
            info!("{:?}: load #{} (ro)", self.name, seq);
            self.current
                .update(seq, Page::load(&self.map, &self.backend, seq)?);
            self.seen_before.insert(seq);
            Ok(&self.current[off..end])
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
        let off = (offset & OFFSET_MASK);
        // split large writes that go over a page boundary
        if pos2chunk(offset + buf.len() as u64 - 1) != seq {
            let in_first_chunk = chunk2pos(1) - off;
            return self
                .write_at(offset, &buf[..in_first_chunk as usize])
                .and_then(|written| {
                    Ok(written
                        + self.write_at(
                            offset + in_first_chunk,
                            &&buf[in_first_chunk as usize..],
                        )?)
                });
        }
        assert!(off + buf.len() as u64 <= 1 << CHUNKSZ_LOG);
        let off = off as usize;
        if let Some(page) = self.cow.get_mut(&seq) {
            page.0[off..off + buf.len()].clone_from_slice(buf);
        } else {
            info!("{:?}: cache #{} (write)", self.name, seq);
            let mut p = if self.map[seq].is_some() {
                Page::load(&self.map, &self.backend, seq)?
            } else {
                Vec::from(ZERO_CHUNK.as_ref()).into()
            };
            p[off..off + buf.len()].clone_from_slice(buf);
            self.cow.insert(seq, p);
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
        const READ_SIZE: usize = 1 << (CHUNKSZ_LOG - 4);
        assert_eq!(
            *fuse.read_at(chunk2pos(2), READ_SIZE)?,
            *vec![3u8; READ_SIZE]
        );
        // empty chunk -> zeroes
        assert_eq!(*fuse.read_at(chunk2pos(1), READ_SIZE)?, *vec![0; READ_SIZE]);
        // nothing should have ended up in the CoW cache until now
        assert!(fuse.cow.is_empty());
        // another chunk
        assert!(fuse.read_at(chunk2pos(0), READ_SIZE).is_ok());
        // read over the end -> short read
        assert_eq!(fuse.read_at((chunk2pos(3)) - 32, READ_SIZE)?, &[3u8; 32]);
        // read at end -> []
        assert!(fuse.read_at(chunk2pos(3), READ_SIZE)?.is_empty());
        // offset > len
        assert!(fuse.read_at(chunk2pos(3) + 1, READ_SIZE).is_err());
        Ok(())
    }

    #[test]
    fn caching() -> Result<()> {
        let s = store(hashmap! {
            rid("cachingfq4bps3NVNEU49K") => vec![
                Some((cid("6746af67f7734d0e9d248a520674f20b"), vec![0u8; 1<<CHUNKSZ_LOG])),
                Some((cid("7605119f98fc6d4630e1ed49e5f32a6d"), vec![1u8; 1<<CHUNKSZ_LOG])),
                Some((cid("854e1ff5d5300914e3e65a0cefdc4baf"), vec![2u8; 1<<CHUNKSZ_LOG])),
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "cachingfq4bps3NVNEU49K")?;
        assert!(fuse.current.is_empty());
        assert!(fuse.seen_before.is_empty());
        // set current to 1, but does not load chunk into CoW
        fuse.read_at(chunk2pos(1), 1)?;
        assert!(!fuse.current.is_empty());
        assert_eq!(fuse.current.seq, 1);
        assert!(fuse.cow.is_empty());
        assert!(fuse.seen_before.contains(&1));
        // set current to 0, but does not load chunk into CoW
        fuse.read_at(chunk2pos(0), 1)?;
        assert_eq!(fuse.current.seq, 0);
        assert!(fuse.cow.is_empty());
        assert!(fuse.seen_before.contains(&0));
        // second access to 1 loads chunk into CoW
        fuse.read_at(chunk2pos(1), 1)?;
        assert_eq!(fuse.current.seq, 0);
        assert!(fuse.cow.contains_key(&1));
        // write access to 2 loads chunk right away into CoW
        assert!(!fuse.cow.contains_key(&2));
        fuse.write_at(chunk2pos(2), &[1])?;
        assert!(fuse.cow.contains_key(&2));
        assert_eq!(fuse.current.seq, 0);
        assert!(!fuse.seen_before.contains(&2));
        Ok(())
    }

    #[test]
    fn all_zero_chunks() -> Result<()> {
        let s = store(hashmap! {rid("pqEKi7Jfq4bps3NVNEU400") => vec![None, None] });
        let mut fuse = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU400")?;
        assert_eq!(
            *fuse.read_at(chunk2pos(0), 1 << CHUNKSZ_LOG)?,
            *vec![0; 1 << CHUNKSZ_LOG]
        );
        assert_eq!(
            *fuse.read_at(chunk2pos(1), 1 << CHUNKSZ_LOG)?,
            *vec![0; 1 << CHUNKSZ_LOG]
        );
        Ok(())
    }

    #[test]
    fn read_offset() -> Result<()> {
        let mut data = vec![11u8; 1 << CHUNKSZ_LOG];
        data[..10].clone_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s = store(hashmap! {rid("MmE1MThjMDZmMWQ5Y2JkMG") => vec![
            Some((cid("25d4f2a86deb5e2574bb3210b67bb24f"), vec![1u8; 1<<CHUNKSZ_LOG])),
            Some((cid("a1fb50e6c86fae1679ef3351296fd671"), data)),
        ]});
        let mut fuse = FuseAccess::load(s.path(), "MmE1MThjMDZmMWQ5Y2JkMG")?;
        assert_eq!(
            fuse.read_at(chunk2pos(1) + 4, 8)?,
            &[4, 5, 6, 7, 8, 9, 11, 11]
        );
        Ok(())
    }

    #[test]
    fn write_cow() -> Result<()> {
        let mut data = vec![11u8; 1 << CHUNKSZ_LOG];
        data[..10].clone_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s = store(hashmap! {rid("XmE1MThjMDZmMWQ5Y2JkMG") => vec![
            Some((cid("00d4f2a86deb5e2574bb3210b67bb24f"), vec![10u8; 1<<CHUNKSZ_LOG])),
            Some((cid("01fb50e6c86fae1679ef3351296fd671"), data))
        ]});
        let mut fuse = FuseAccess::load(s.path(), "XmE1MThjMDZmMWQ5Y2JkMG")?;
        assert!(fuse.cow.is_empty());
        // write at beginning boundary
        assert_eq!(fuse.write_at(0, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(0, 5)?, &[0, 1, 2, 3, 10]);
        // write at page boundary
        assert_eq!(fuse.write_at(chunk2pos(1) - 4, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(chunk2pos(1) - 5, 5)?, &[10, 0, 1, 2, 3]);
        // write over page boundary
        assert_eq!(fuse.write_at(chunk2pos(1) - 2, &[20, 21, 22, 23])?, 4);
        assert_eq!(
            fuse.read_at(chunk2pos(1) - 4, 4)?,
            &[
                0, 1, // from last write
                20, 21 // newly written to page 0
            ]
        );
        assert_eq!(
            fuse.read_at(chunk2pos(1), 5)?,
            &[
                22, 23, // newly written to page 1
                2, 3, 4 // original contents of page 1
            ]
        );
        // write over EOF
        assert!(fuse.write_at(chunk2pos(2) - 2, &[4, 5, 6, 7]).is_err());
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

    #[test]
    fn missing_chunk() -> Result<()> {
        let s = store(hashmap! {rid("missingjMDZmMWQ5Y2JkMG") => vec![
            Some((cid("7d0eae958fb3a9889774603cd049716b"), vec![1u8; 1<<CHUNKSZ_LOG])),
            Some((cid("95d2a0ce9869d4ec1395b7a7c8fae0c9"), vec![2u8; 1<<CHUNKSZ_LOG])),
        ]});
        fs::remove_file(
            s.path()
                .join("chunks/95/95d2a0ce9869d4ec1395b7a7c8fae0c9.chunk.lzo"),
        )?;
        let mut fuse = FuseAccess::load(s.path(), "missingjMDZmMWQ5Y2JkMG")?;
        assert_eq!(fuse.read_at(chunk2pos(0), 1)?, &[1]);
        match fuse.read_at(chunk2pos(1), 1) {
            Err(e @ Error::BackendLoad { .. }) => println!("expected Err: {}", e),
            res @ _ => panic!("unexpected result: {:?}", res),
        }
        Ok(())
    }

    #[test]
    fn broken_map() -> Result<()> {
        let s = store(hashmap! {rid("brokenhjMDZmMWQ5Y2JkMG") => vec![
            Some((cid("6f9d15addefbbee6d9190df64706c58f"), vec![0u8; 1<<CHUNKSZ_LOG])),
        ]});
        fs::OpenOptions::new()
            .write(true)
            .open(s.path().join("brokenhjMDZmMWQ5Y2JkMG"))?
            .set_len(30)?;
        // expected to succeed because map is not read at this step
        let mut fuse = FuseAccess::new(s.path(), "brokenhjMDZmMWQ5Y2JkMG")?;
        match fuse.load_if_empty() {
            Err(e @ Error::ParseMap { .. }) => println!("expected Err: {}", e),
            res @ _ => panic!("Unexpected result: {:?}", res),
        }
        Ok(())
    }
}
