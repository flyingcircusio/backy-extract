//! Fuse-driven access to revisions with in-memory COW

use crate::backend::{self, Backend};
use crate::chunkvec::{ChunkId, RevisionMap};
use crate::{chunk2pos, pos2chunk, CHUNKSZ, ZERO_CHUNK};

use bitvec::vec::BitVec;
use chrono::{DateTime, TimeZone, Utc};
use fnv::FnvHashMap as HashMap;
use log::{debug, info};
use serde::{Deserialize, Deserializer};
use smallstr::SmallString;
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
        chunk_id: ChunkId,
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

type Chunks = Vec<Option<ChunkId>>;

#[derive(Clone, Default)]
struct Page(Box<[u8]>);

impl Page {
    /// Fetches a chunk from the backend
    ///
    /// Panics if the requested page is a zero page and thus not present in the backend.
    fn load(map: &[Option<ChunkId>], backend: &Backend, seq: u32) -> Result<Self> {
        let cid = map[seq as usize]
            .clone()
            .unwrap_or_else(|| panic!("failed to locate chunk {} in map", seq));
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

type RevID = SmallString<[u8; 24]>;

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

#[derive(Debug, Clone, Default)]
struct ROCache {
    seq: u32,
    data: Page,
}

impl ROCache {
    fn matches(&self, seq: u32) -> bool {
        self.seq == seq && !self.data.is_empty()
    }

    fn update(&mut self, seq: u32, data: Page) {
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
    seen_before: BitVec,
    cache: HashMap<u32, Page>,
}

const OFFSET_MASK: u64 = CHUNKSZ as u64 - 1;

/// API to read/write images from the upper-level FUSE driver
///
/// This layer implements simple CoW caching. Pages are put into the cache
/// either when they are written to or when they are read for the second time.
/// Modifications are only stored in memory and never written to disk. This
/// enables filesystem tools like fsck to perform recovery.
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
            size: 0,             // initialized by load_map()
            map: Vec::default(), // initialized by load_map()
            backend,
            current: ROCache::default(),
            seen_before: BitVec::default(),
            cache: HashMap::default(),
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
        self.seen_before.resize(self.map.len(), false);
        Ok(self.map.len())
    }

    /// Seeks to offset and reads the specified amount of bytes. Note that this function may
    /// return less than `size` bytes.
    pub fn read_at(&mut self, offset: u64, size: usize) -> Result<&[u8]> {
        match offset {
            o if o == self.size => Ok(&[]),
            o if o > self.size => {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "read beyond end of image").into())
            }
            _ => {
                let off = (offset & OFFSET_MASK) as usize;
                self.read(pos2chunk(offset), off, min(off + size, CHUNKSZ as usize))
            }
        }
    }

    /// Returns data from chunk `seq`. Data is fetched from the cache or loaded from disk if
    /// necessary. The cache is filled only on the second read access to a specific page. This
    /// keeps the cache clean of purely sequential reads.
    ///
    /// Note that this function may return less that the requested amount of data.
    fn read(&mut self, seq: u32, off: usize, end: usize) -> Result<&[u8]> {
        match self.cache.entry(seq) {
            Entry::Vacant(e) => {
                if !self.current.matches(seq) && self.seen_before[seq as usize] {
                    info!("{:?}: cache #{} (seen twice)", self.name, seq);
                    let page = Page::load(&self.map, &self.backend, seq)?;
                    return Ok(&e.insert(page)[off..end]);
                }
            }
            Entry::Occupied(e) => return Ok(&e.into_mut()[off..end]),
        }
        if self.map[seq as usize].is_some() {
            if self.current.matches(seq) {
                return Ok(&self.current[off..end]);
            }
            info!("{:?}: load #{} (read)", self.name, seq);
            self.current
                .update(seq, Page::load(&self.map, &self.backend, seq)?);
            self.seen_before.set(seq as usize, true);
            Ok(&self.current[off..end])
        } else {
            Ok(&ZERO_CHUNK[off..end])
        }
    }

    /// Saved dirty data to the CoW cache in memory. Data is never written to disk. Note that not
    /// all bytes may be written. In this case, the returned number is less than buf.len() and the
    /// write operation should be retried with the remainder.
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        if offset + buf.len() as u64 > self.size {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "write beyond end of image").into(),
            );
        }
        let seq = pos2chunk(offset);
        let off = offset & OFFSET_MASK;
        // writes that go over a page boundary result in a short write
        if pos2chunk(offset + buf.len() as u64 - 1) != seq {
            let in_first_chunk = chunk2pos(1) - off;
            return self.write_at(offset, &buf[..in_first_chunk as usize]);
        }
        assert!(off as usize + buf.len() <= CHUNKSZ as usize);
        let off = off as usize;
        if let Some(page) = self.cache.get_mut(&seq) {
            page.0[off..off + buf.len()].clone_from_slice(buf);
        } else {
            info!("{:?}: cache #{} (write)", self.name, seq);
            let mut p = if self.map[seq as usize].is_some() {
                Page::load(&self.map, &self.backend, seq)?
            } else {
                Vec::from(ZERO_CHUNK.as_ref()).into()
            };
            p[off..off + buf.len()].clone_from_slice(buf);
            self.cache.insert(seq, p);
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
}

impl Deref for FuseDirectory {
    type Target = HashMap<u64, FuseAccess>;

    fn deref(&self) -> &Self::Target {
        &self.revs
    }
}

impl DerefMut for FuseDirectory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.revs
    }
}

impl Drop for FuseDirectory {
    fn drop(&mut self) {
        let mut total = 0;
        for (ino, a) in &self.revs {
            let b = a.cache.len() as u64 * CHUNKSZ as u64;
            debug!(
                "{}: {}/{} pages ({} bytes) cached",
                ino,
                a.cache.len(),
                pos2chunk(a.size),
                b
            );
            total += b;
        }
        debug!("Total cache memory: {} bytes", total);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::chunkvec::ChunkId;
    use crate::CHUNKSZ_LOG;
    use backend::MAGIC;
    use maplit::hashmap;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::fs;
    use std::io::Write;
    use tempdir::TempDir;

    const SZ: usize = CHUNKSZ as usize;

    // Identical to the one in chunkvec.rs, but without borrows
    #[derive(Debug, Default, Serialize)]
    struct RevisionMap {
        mapping: HashMap<String, String>,
        size: u64,
    }

    pub fn cid(id: &str) -> ChunkId {
        ChunkId::from_str(id)
    }

    pub fn rid(id: &str) -> RevID {
        RevID::from_str(id)
    }

    pub fn store(spec: HashMap<RevID, Vec<Option<(ChunkId, Vec<u8>)>>>) -> TempDir {
        let td = TempDir::new("backy-store-test").expect("tempdir");
        let p = td.path();
        for (rev, chunks) in spec {
            fs::write(
                p.join(&rev.as_str()).with_extension("rev"),
                format!(
                    r#"backend_type: chunked
parent: JZ3zfSHq24Fy5ENgTgYLGF
stats:
    bytes_written: {written}
    ceph-verification: partial
    chunk_stats: {{write_full: {nchunks}, write_partial: 0}}
    duration: 216.60814833641052
tags: [daily]
timestamp: 2019-11-14 14:21:18.289+00:00
trust: trusted
uuid: {rev}
"#,
                    written = chunk2pos(chunks.len() as u32),
                    nchunks = chunks.len(),
                    rev = rev.as_str()
                ),
            )
            .expect("write .rev");
            let mut map = RevisionMap {
                size: (chunks.len() << CHUNKSZ_LOG) as u64,
                mapping: Default::default(),
            };
            fs::create_dir(p.join("chunks")).ok();
            fs::write(p.join("chunks/store"), "v2").unwrap();
            for (i, chunk) in chunks.iter().enumerate() {
                if let Some(c) = chunk {
                    let id = c.0.to_string();
                    let file = p
                        .join("chunks")
                        .join(&id[0..2])
                        .join(format!("{}.chunk.lzo", id));
                    fs::create_dir_all(file.parent().unwrap()).ok();
                    let mut f = fs::File::create(file).unwrap();
                    f.write(&MAGIC).unwrap();
                    f.write(&minilzo::compress(&c.1).unwrap()).unwrap();
                    map.mapping.insert(i.to_string(), id);
                }
            }
            let f = fs::File::create(p.join(rev.as_str())).unwrap();
            serde_json::to_writer(f, &map).unwrap();
        }
        td
    }

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
                Some((cid("4355a46b19d348dc2f57c046f8ef63d4"), vec![1u8; SZ])),
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
                Some((cid("4355a46b19d348dc2f57c046f8ef63d4"), vec![1u8; SZ])),
                None,
                Some((cid("1121cfccd5913f0a63fec40a6ffd44ea"), vec![3u8; SZ])),
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
        assert!(fuse.cache.is_empty());
        // another chunk
        assert!(fuse.read_at(chunk2pos(0), READ_SIZE).is_ok());
        // read over page boundary -> short read
        assert_eq!(fuse.read_at((chunk2pos(1)) - 32, READ_SIZE)?, &[1u8; 32]);
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
                Some((cid("6746af67f7734d0e9d248a520674f20b"), vec![0u8; SZ])),
                Some((cid("7605119f98fc6d4630e1ed49e5f32a6d"), vec![1u8; SZ])),
                Some((cid("854e1ff5d5300914e3e65a0cefdc4baf"), vec![2u8; SZ])),
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "cachingfq4bps3NVNEU49K")?;
        assert!(fuse.current.is_empty());
        assert!(fuse.seen_before.not_any());
        // set current to 1, but does not load chunk into CoW
        fuse.read_at(chunk2pos(1), 1)?;
        assert!(!fuse.current.is_empty());
        assert_eq!(fuse.current.seq, 1);
        assert!(fuse.cache.is_empty());
        assert!(fuse.seen_before[1]);
        // set current to 0, but does not load chunk into CoW
        fuse.read_at(chunk2pos(0), 1)?;
        assert_eq!(fuse.current.seq, 0);
        assert!(fuse.cache.is_empty());
        assert!(fuse.seen_before[0]);
        // second access to 1 loads chunk into CoW
        fuse.read_at(chunk2pos(1), 1)?;
        assert_eq!(fuse.current.seq, 0);
        assert!(fuse.cache.contains_key(&1));
        // write access to 2 loads chunk right away into CoW
        assert!(!fuse.cache.contains_key(&2));
        fuse.write_at(chunk2pos(2), &[1])?;
        assert!(fuse.cache.contains_key(&2));
        assert_eq!(fuse.current.seq, 0);
        assert!(!fuse.seen_before[2]);
        Ok(())
    }

    #[test]
    fn all_zero_chunks() -> Result<()> {
        let s = store(hashmap! {rid("pqEKi7Jfq4bps3NVNEU400") => vec![None, None] });
        let mut fuse = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU400")?;
        assert_eq!(*fuse.read_at(chunk2pos(0), SZ)?, *vec![0; SZ]);
        assert_eq!(*fuse.read_at(chunk2pos(1), SZ)?, *vec![0; SZ]);
        Ok(())
    }

    #[test]
    fn read_offset() -> Result<()> {
        let mut data = vec![11u8; SZ];
        data[..10].clone_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s = store(hashmap! {rid("MmE1MThjMDZmMWQ5Y2JkMG") => vec![
            Some((cid("25d4f2a86deb5e2574bb3210b67bb24f"), vec![1u8; SZ])),
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
        let mut data = vec![11u8; SZ];
        data[..10].clone_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let s = store(hashmap! {rid("XmE1MThjMDZmMWQ5Y2JkMG") => vec![
            Some((cid("00d4f2a86deb5e2574bb3210b67bb24f"), vec![10u8; SZ])),
            Some((cid("01fb50e6c86fae1679ef3351296fd671"), data))
        ]});
        let mut fuse = FuseAccess::load(s.path(), "XmE1MThjMDZmMWQ5Y2JkMG")?;
        assert!(fuse.cache.is_empty());
        // write at beginning boundary
        assert_eq!(fuse.write_at(0, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(0, 5)?, &[0, 1, 2, 3, 10]);
        // write at page boundary
        assert_eq!(fuse.write_at(chunk2pos(1) - 4, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(chunk2pos(1) - 5, 5)?, &[10, 0, 1, 2, 3]);
        // write over page boundary -> short write
        assert_eq!(fuse.write_at(chunk2pos(1) - 2, &[20, 21, 22, 23])?, 2);
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
                0, 1, 2, 3, 4 // original contents of page 1
            ]
        );
        // write exactly to EOF
        assert_eq!(fuse.write_at(chunk2pos(2) - 2, &[8, 9])?, 2);
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
            Some((cid("7d0eae958fb3a9889774603cd049716b"), vec![1u8; SZ])),
            Some((cid("95d2a0ce9869d4ec1395b7a7c8fae0c9"), vec![2u8; SZ])),
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
            Some((cid("6f9d15addefbbee6d9190df64706c58f"), vec![0u8; SZ])),
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
