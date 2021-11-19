//! Fuse-driven access to revisions with in-memory COW

use crate::backend::{self, Backend, Rev, RevError};
use crate::chunkvec::{ChunkId, RevisionMap};
use crate::{pos2chunk, CHUNKSZ, CHUNKSZ_LOG, ZERO_CHUNK};

use fnv::FnvHashMap as HashMap;
use log::{debug, info};
use lru::LruCache;
use murmur3::murmur3_x64_128;
use std::cmp::min;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

static ID_SEQ: AtomicU64 = AtomicU64::new(4);

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] io::Error),
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
    #[error("Invalid revision file name '{}'", .0.display())]
    InvalidName(PathBuf),
    #[error("'{}' contains no revision or no chunks - is this really a backy directory?",
            .0.display())]
    NoRevisions(PathBuf),
    #[error(transparent)]
    Rev(#[from] RevError),
}

type Result<T, E = Error> = std::result::Result<T, E>;

type Chunks = Vec<Option<ChunkId>>;

#[derive(Clone)]
struct Page {
    data: Rc<Vec<u8>>,
    seq: u32,
}

impl Page {
    /// Fetches a chunk from the backend.
    ///
    /// Panics if the requested page is a zero page and thus not present in the backend.
    fn load(map: &[Option<ChunkId>], backend: &Backend, seq: u32) -> Result<Self> {
        let cid = map[seq as usize]
            .clone()
            .unwrap_or_else(|| panic!("failed to locate chunk {} in map", seq));
        Ok((
            backend.load(cid.as_str()).map_err(|e| Error::BackendLoad {
                chunk_id: cid,
                source: e,
            })?,
            seq,
        )
            .into())
    }

    #[cfg(test)]
    fn new(data: Vec<u8>, seq: u32) -> Self {
        Self {
            data: Rc::new(data),
            seq,
        }
    }

    /// Overwrites data region inside page. Panics if updated regions exceeds boundaries.
    ///
    /// The page gets copied before writing if other pages are around.
    fn update(&mut self, off: usize, new: &[u8]) {
        assert!(off + new.len() <= CHUNKSZ, "Page update exceeds page size");
        Rc::make_mut(&mut self.data)[off..off + new.len()].clone_from_slice(new);
    }

    fn set_seq(mut self, seq: u32) -> Self {
        self.seq = seq;
        self
    }

    fn hash(&self) -> ChunkId {
        ChunkId::from(hex::encode(
            murmur3_x64_128(&mut io::Cursor::new(self.data.as_ref()), 0)
                .unwrap()
                .to_le_bytes(),
        ))
    }
}

impl From<(Vec<u8>, u32)> for Page {
    fn from(data: (Vec<u8>, u32)) -> Self {
        Self {
            data: data.0.into(),
            seq: data.1,
        }
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            data: Default::default(),
            seq: u32::MAX,
        }
    }
}
impl Deref for Page {
    type Target = Rc<Vec<u8>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.data.is_empty() {
            write!(f, "Page[]")
        } else {
            write!(f, "Page[...]")
        }
    }
}

#[derive(Debug)]
pub struct FuseAccess {
    pub name: OsString,
    pub rev: Rev,
    pub size: u64,
    map: Chunks,
    backend: Backend,
    open_page: Page,
    zero_page: Page,
    dirty: LruCache<u32, Page>,
    ro_cache: LruCache<u32, Page>,
}

const OFFSET_MASK: u64 = CHUNKSZ as u64 - 1;

/// API to read/write images from the upper-level FUSE driver.
///
/// This layer implements simple CoW caching. Pages are put into the cache
/// either when they are written to or when they are read for the second time.
/// Modifications are only stored in memory and never written to disk. This
/// enables filesystem tools like fsck to perform recovery.
impl FuseAccess {
    fn new<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I, cache_size: usize) -> Result<Self> {
        let dir = dir.as_ref();
        let rev = Rev::load(dir, id.as_ref())?;
        let backend = Backend::open(dir)?;
        Ok(Self {
            name: OsString::from(id.as_ref()),
            rev,
            size: 0,             // initialized by load_map()
            map: Vec::default(), // initialized by load_map()
            backend,
            open_page: Page::default(),
            zero_page: Page::from((Vec::from(ZERO_CHUNK.as_ref()), u32::MAX)),
            dirty: LruCache::new(cache_size >> CHUNKSZ_LOG),
            ro_cache: LruCache::new(cache_size >> CHUNKSZ_LOG),
        })
    }

    #[cfg(test)]
    fn load<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let mut res = Self::new(dir, id, 12 << 20)?;
        res.load_map()?;
        Ok(res)
    }

    /// Loads chunk map from basedir.
    ///
    /// Returns number of chunks found in the map. This function is idempotent: If the map is
    /// already initialized, nothing happens.
    pub fn load_if_empty(&mut self) -> Result<()> {
        if self.map.is_empty() {
            self.load_map()?;
            debug!(
                "Loaded {} chunks from chunk map {:?}",
                self.map.len(),
                self.name
            );
        }
        Ok(())
    }

    fn load_map(&mut self) -> Result<()> {
        let path = self.backend.dir.join(&self.name);
        let revmap_s = fs::read_to_string(&path)?;
        let revmap: RevisionMap =
            serde_json::from_str(&revmap_s).map_err(|source| Error::ParseMap { path, source })?;
        self.size = revmap.size;
        self.map = revmap.into_iter().map(|(_seq, id)| id).collect();
        Ok(())
    }

    /// Drops read only cache to conserve memory. Note that the dirty page cache remains.
    pub fn cleanup(&mut self) {
        self.ro_cache.clear()
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
                let seq = pos2chunk(offset);
                if self.open_page.seq != seq {
                    self.open_page = self.read(seq)?;
                }
                Ok(&self.open_page[off..min(off + size, CHUNKSZ)])
            }
        }
    }

    /// Returns data from chunk `seq`. Data is fetched from the cache or loaded from disk if
    /// necessary.
    fn read(&mut self, seq: u32) -> Result<Page> {
        if let Some(page) = self.dirty.get(&seq) {
            debug!("{:?}: hit #{} (dirty)", self.name, seq);
            Ok(page.clone())
        } else if let Some(page) = self.ro_cache.get(&seq) {
            debug!("{:?}: hit #{}", self.name, seq);
            Ok(page.clone())
        } else if self.map[seq as usize].is_none() {
            debug!("{:?}: zero #{}", self.name, seq);
            Ok(self.zero_page.clone())
        } else {
            info!("{:?}: load #{}", self.name, seq);
            let page = Page::load(&self.map, &self.backend, seq)?;
            self.ro_cache.put(seq, page.clone());
            Ok(page)
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
        // writes that go over a page boundary are only partially written
        let buf = if pos2chunk(offset + buf.len() as u64 - 1) != seq {
            &buf[..CHUNKSZ - off as usize]
        } else {
            buf
        };
        self.write(seq, off as usize, buf)
    }

    fn write(&mut self, seq: u32, off: usize, buf: &[u8]) -> Result<usize> {
        // resets reference count
        self.open_page = Page::default();
        if let Some(page) = self.dirty.get_mut(&seq) {
            debug!("{:?}: hit #{} (write)", self.name, seq);
            page.update(off, buf);
        } else if let Some(mut page) = self.ro_cache.pop(&seq) {
            info!("{:?}: dirty #{}", self.name, seq);
            page.update(off, buf);
            self.dirty.put(seq, page);
        } else {
            self.alloc(seq, off, buf)?;
        }
        Ok(buf.len())
    }

    /// Creates a new page in the dirty cache either from disk or as empty page. Writes `buf` into
    /// that page.
    fn alloc(&mut self, seq: u32, off: usize, buf: &[u8]) -> Result<()> {
        let mut page = if self.map[seq as usize].is_some() {
            info!("{:?}: load #{} (write)", self.name, seq);
            Page::load(&self.map, &self.backend, seq)?
        } else {
            debug!("{:?}: zero #{} (write)", self.name, seq);
            self.zero_page.clone().set_seq(seq)
        };
        page.update(off, buf);
        self.dirty.put(seq, page);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct FuseDirectory {
    pub basedir: PathBuf,
    revs: HashMap<u64, FuseAccess>,
}

impl FuseDirectory {
    pub fn init<P: AsRef<Path>>(dir: P, cache_size: usize) -> Result<Self> {
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
                let f = FuseAccess::new(&dir, rid, cache_size)?;
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::chunkvec::ChunkId;
    use crate::test_helper::*;
    use crate::{chunk2pos, CHUNKSZ_LOG};
    use backend::RevId;

    use chrono::{TimeZone, Utc};
    use maplit::hashmap;
    use std::collections::HashMap;
    use std::fs;
    use tempdir::TempDir;

    const SZ: usize = CHUNKSZ;

    pub fn cid(id: &str) -> ChunkId {
        ChunkId::from_str(id)
    }

    pub fn rid(id: &str) -> RevId {
        RevId::from_str(id)
    }

    pub fn store(spec: HashMap<RevId, Vec<Option<Vec<u8>>>>) -> TempDir {
        let td = TempDir::new("backy-store-test").expect("tempdir");
        let p = td.path();
        for (rev, data) in spec {
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
                    written = chunk2pos((data.len() + 1) as u32),
                    nchunks = data.len(),
                    rev = rev.as_str()
                ),
            )
            .expect("write .rev");
            let mut map = RevisionMap {
                size: (data.len() << CHUNKSZ_LOG) as u64,
                mapping: Default::default(),
            };
            fs::create_dir(p.join("chunks")).ok();
            fs::write(p.join("chunks/store"), "v2").unwrap();
            let be = Backend::open(&p).unwrap();
            for (i, chunk) in data.into_iter().enumerate() {
                if let Some(c) = chunk {
                    let c = Page::new(c, i as u32);
                    let id = c.hash();
                    be.save(&id, &c.data).unwrap();
                    map.mapping.insert(i.to_string().into(), id);
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
            rid("pqEKi7Jfq4bps3NVNEU49K") => vec![Some(vec![1u8; SZ]), None],
        });
        let ra = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        assert_eq!(
            ra.map,
            &[Some(cid("ad92954d3d9926b51a07c64d0ee79f85")), None]
        );
        Ok(())
    }

    #[test]
    fn read_data() -> Result<()> {
        let s = store(hashmap! {
            rid("pqEKi7Jfq4bps3NVNEU49K") => vec![Some(vec![1u8; SZ]), None, Some(vec![3u8; SZ])],
        });
        let mut fuse = FuseAccess::load(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        const READ_SIZE: usize = 1 << (CHUNKSZ_LOG - 4);
        assert_eq!(
            *fuse.read_at(chunk2pos(2), READ_SIZE)?,
            *vec![3u8; READ_SIZE]
        );
        // empty chunk -> zeroes
        assert_eq!(*fuse.read_at(chunk2pos(1), READ_SIZE)?, *vec![0; READ_SIZE]);
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
                Some(vec![0u8; SZ]),
                Some(vec![1u8; SZ]),
                Some(vec![2u8; SZ]),
            ]
        });
        let mut fuse = FuseAccess::load(s.path(), "cachingfq4bps3NVNEU49K")?;
        assert!(fuse.ro_cache.is_empty());
        fuse.read_at(chunk2pos(1), 1)?;
        assert!(fuse.ro_cache.get(&1).is_some());
        fuse.read_at(chunk2pos(0), 1)?;
        assert_eq!(fuse.ro_cache.len(), 2);
        assert!(fuse.ro_cache.get(&0).is_some());
        fuse.read_at(chunk2pos(1), 1)?;
        assert_eq!(fuse.ro_cache.len(), 2);
        fuse.write_at(chunk2pos(2), &[1])?;
        assert!(fuse.ro_cache.get(&2).is_none());
        assert!(fuse.dirty.get(&2).is_some());
        assert_eq!(fuse.dirty.len(), 1);
        assert_eq!(fuse.ro_cache.len(), 2);
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
        let s = store(hashmap! {
            rid("MmE1MThjMDZmMWQ5Y2JkMG") => vec![Some(vec![1u8; SZ]), Some(data)]
        });
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
        let s = store(hashmap! {
            rid("XmE1MThjMDZmMWQ5Y2JkMG") => vec![Some(vec![10u8; SZ]), Some(data)]
        });
        let mut fuse = FuseAccess::load(s.path(), "XmE1MThjMDZmMWQ5Y2JkMG")?;
        assert!(fuse.ro_cache.is_empty());
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
        assert!(fuse.ro_cache.get(&0).is_none());
        assert_eq!(fuse.write_at(2, &[1])?, 1);
        assert!(fuse.ro_cache.get(&0).is_none());
        assert_eq!(&fuse.dirty.get(&0).unwrap().data[0..5], &[0, 0, 1, 0, 0]);
        assert_eq!(fuse.read_at(0, 5)?, &[0, 0, 1, 0, 0]);
        Ok(())
    }

    #[test]
    fn missing_chunk() -> Result<()> {
        let s = store(hashmap! {
            rid("missingjMDZmMWQ5Y2JkMG") => vec![Some(vec![1u8; SZ]), Some(vec![2u8; SZ])]
        });
        // std::thread::sleep(Duration::from_secs(1000));
        fs::remove_file(
            s.path()
                .join("chunks/16/164570efa9d3d3354db15e99e2f6c781.chunk.lzo"),
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
        let s = store(hashmap! { rid("brokenhjMDZmMWQ5Y2JkMG") => vec![Some(vec![0u8; SZ])] });
        fs::OpenOptions::new()
            .write(true)
            .open(s.path().join("brokenhjMDZmMWQ5Y2JkMG"))?
            .set_len(30)?;
        // expected to succeed because map is not read at this step
        let mut fuse = FuseAccess::new(s.path(), "brokenhjMDZmMWQ5Y2JkMG", 0)?;
        match fuse.load_if_empty() {
            Err(e @ Error::ParseMap { .. }) => println!("expected Err: {}", e),
            res @ _ => panic!("Unexpected result: {:?}", res),
        }
        Ok(())
    }

    #[test]
    /// Verify that the hash function produces the same hash value as found in the supplied
    /// fixture.
    fn hash_chunk() {
        let s = store_tar();
        let mut f = FuseAccess::load(s.path(), "VNzWKjnMqd6w58nzJwUZ98").unwrap();
        let p = f.read(0).unwrap();
        assert_eq!(p.hash(), "4db6e194fd398e8edb76e11054d73eb0");
    }
}
