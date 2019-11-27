use crate::backend::{self, Backend};
/// Fuse-driven access to revisions with in-memory COW
use crate::chunkvec::{ChunkID, RevisionMap};
use crate::{pos2chunk, RevID, CHUNKSZ_LOG, ZERO_CHUNK};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    IO(#[from] io::Error),
    #[error("Invalid data in .rev file {path:?}")]
    ParseRev {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("Invalid data in chunk map file {path:?}")]
    ParseMap {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("Chunked backend error")]
    Backend(#[from] backend::Error),
    #[error("Unknown backend_type '{}' found in {}", betype, path.display())]
    WrongType { betype: String, path: PathBuf },
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
    cow: HashMap<usize, Page>,
}

impl Cache {
    fn new(seq: usize, data: Page) -> Self {
        Self {
            seq,
            data,
            cow: HashMap::new(),
        }
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

static TIMESTAMP_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S%.f%z";
fn timestamp_de<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    Utc.datetime_from_str(&String::deserialize(deserializer)?, TIMESTAMP_FORMAT)
        .map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
struct Rev {
    backend_type: String,
    #[serde(deserialize_with = "revid_de")]
    parent: RevID,
    #[serde(deserialize_with = "timestamp_de")]
    timestamp: DateTime<Utc>,
    trust: String,
    #[serde(deserialize_with = "revid_de")]
    uuid: RevID,
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
            }
            .into());
        }
        Ok(r)
    }
}

#[derive(Debug)]
pub struct FuseAccess {
    rev: Rev,
    size: u64,
    map: Chunks,
    backend: Backend,
    cache: Option<Cache>,
}

fn copy(target: &mut [u8], source: &[u8], offset: usize) -> usize {
    assert!(offset < source.len(), "offset if beyond data.len()");
    let n = min(target.len(), source.len() - offset);
    target[0..n].clone_from_slice(&source[offset..(offset + n)]);
    n
}

const OFFSET_MASK: u64 = (1 << CHUNKSZ_LOG) - 1;

impl FuseAccess {
    fn new<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let dir = dir.as_ref();
        let map = dir.join(id.as_ref());
        let rev = Rev::load(dir, id)?;
        let backend = Backend::open(dir)?;
        // XXX lazy map loading?
        let revmap_s = fs::read_to_string(&map)?;
        let revmap: RevisionMap =
            serde_json::from_str(&revmap_s).map_err(|source| Error::ParseMap {
                path: map.to_path_buf(),
                source,
            })?;
        let size = revmap.size;
        let map = revmap.into_iter().map(|(_seq, id)| id).collect();
        Ok(Self {
            rev,
            size,
            map,
            backend,
            cache: None,
        })
    }

    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if offset > self.size {
            return Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "read beyond image length").into(),
            );
        } else if offset == self.size {
            return Ok(0);
        }
        let seq = pos2chunk(offset);
        let off = (offset & OFFSET_MASK) as usize;
        if let Some(cache) = &self.cache {
            if let Some(page) = cache.cow.get(&seq) {
                return Ok(copy(buf, &page.0, off));
            }
            if seq == cache.seq {
                return Ok(copy(buf, &cache.data.0, off));
            }
        }
        if let Some(chunk_id) = self.map[seq].clone() {
            let data = self.backend.load(chunk_id.as_str())?;
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::{cid, rid, store};
    use maplit::hashmap;

    #[test]
    fn initialize_rev() -> Result<()> {
        let s = store(hashmap! {
            rid("pqEKi7Jfq4bps3NVNEU49K") => vec![
                Some((cid("4355a46b19d348dc2f57c046f8ef63d4"), vec![1u8; 1<<CHUNKSZ_LOG])),
                Some((cid("53c234e5e8472b6ac51c1ae1cab3fe06"), vec![2u8; 1<<CHUNKSZ_LOG]))
            ]
        });
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
        assert!(fuse.cache.is_some());
        // empty chunk -> zeroes
        assert_eq!(fuse.read_at(&mut buf, 1 << CHUNKSZ_LOG)?, read_size);
        assert_eq!(&buf, &vec![0; read_size]);
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
        assert!(fuse.cache.unwrap().cow.is_empty());
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
                Some((cid("00d4f2a86deb5e2574bb3210b67bb24f"), vec![11u8; 1<<CHUNKSZ_LOG])),
                Some((cid("01fb50e6c86fae1679ef3351296fd671"), vec![12u8; 1<<CHUNKSZ_LOG])),
            ]
        });
        let mut fuse = FuseAccess::new(s.path(), "XmE1MThjMDZmMWQ5Y2JkMG")?;
        let mut buf = vec![0; 5];
        assert_eq!(fuse.read_at(&mut buf, 0)?, 5);
        assert_eq!(&buf, &[11, 11, 11, 11, 11]);
        assert_eq!(fuse.write_at(0, &[0, 1, 2, 3])?, 4);
        assert_eq!(fuse.read_at(&mut buf, 0)?, 5);
        assert_eq!(&buf, &[0, 1, 2, 3, 11]);
        // write over page boundary
        Ok(())
    }

    // XXX Test: missing files
    // XXX Test: format error
}
