/// Random access to revisions with in-memory COW
///
/// This functionality is required by the FUSE driver.
use crate::chunkvec::{ChunkID, RevisionMap};
use crate::CHUNKSZ_LOG;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer};
use smallstr::SmallString;
use snafu::{ResultExt, Snafu}; // XXX switch to ThisError
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Snafu)]
pub enum Error {
    IO {
        source: io::Error,
    },
    OpenFailed {
        p: PathBuf,
        source: io::Error,
    },
    ParseRev {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    ParseMap {
        path: PathBuf,
        source: serde_json::Error,
    },
    Test
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IO { source: err }
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
struct Page([u8; 1 << CHUNKSZ_LOG]);

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page[...]")
    }
}

impl Default for Page {
    fn default() -> Self {
        Self([0; 1 << CHUNKSZ_LOG])
    }
}

#[derive(Debug, Clone, Default)]
struct Cache {
    pos: u64,
    cache_seq: usize,
    cache_data: Box<Page>,
    dirty: HashMap<usize, Page>,
}

type RevID = SmallString<[u8; 24]>;

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
pub struct Rev {
    backend_type: String,
    #[serde(deserialize_with = "revid_de")]
    parent: RevID,
    #[serde(deserialize_with = "timestamp_de")]
    timestamp: DateTime<Utc>,
    trust: String,
    #[serde(deserialize_with = "revid_de")]
    uuid: RevID,
    #[serde(skip)]
    map: Chunks,
    #[serde(skip)]
    cache: Option<Cache>,
}

impl Rev {
    pub fn load<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let map = dir.as_ref().join(id.as_ref());
        let rev = map.with_extension("rev");
        eprintln!("size(Rev)={}", std::mem::size_of::<Rev>());
        let mut res: Self =
            serde_yaml::from_reader(fs::File::open(&rev).context(OpenFailed { p: &rev })?)
                .context(ParseRev { path: &rev })?;
        res.map = serde_json::from_str::<RevisionMap>(
            &fs::read_to_string(&map).context(OpenFailed { p: &rev })?,
        )
        .context(ParseMap { path: &map })?
        .into_iter()
        .map(|(_seq, id)| id)
        .collect();
        Ok(res)
    }
}

impl io::Read for Rev {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        Ok(0)
    }
}

impl io::Seek for Rev {
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64, io::Error> {
        Ok(0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use maplit::hashmap;
    use serde::Serialize;
    use tempdir::TempDir;

    // Identical to the one in chunkvec.rs, but without borrows
    #[derive(Debug, Default, Serialize)]
    struct RevisionMap {
        mapping: HashMap<String, String>,
        size: u64,
    }

    fn cid(id: &str) -> ChunkID {
        ChunkID::from_str(id)
    }

    fn rid(id: &str) -> RevID {
        RevID::from_str(id)
    }

    fn store(spec: HashMap<RevID, Vec<Option<(ChunkID, Vec<u8>)>>>) -> TempDir {
        let td = TempDir::new("backy").expect("tempdir");
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
                    written = chunks.len() << CHUNKSZ_LOG,
                    nchunks = chunks.len(),
                    rev = rev.as_str()
                ),
            )
            .expect("write .rev");
            let mut map = RevisionMap {
                size: (chunks.len() << CHUNKSZ_LOG) as u64,
                mapping: Default::default(),
            };
            for (i, chunk) in chunks.iter().enumerate() {
                if let Some(c) = chunk {
                    map.mapping.insert(i.to_string(), c.0.to_string());
                }
            }
            let f = fs::File::create(p.join(rev.as_str())).unwrap();
            serde_json::to_writer(f, &map).unwrap();
        }
        td
    }

    #[test]
    fn initialize_map() -> Result<()> {
        let s = store(hashmap! {
            rid("pqEKi7Jfq4bps3NVNEU49K") => vec![
                Some((cid("4355a46b19d348dc2f57c046f8ef63d4"), vec![1u8; 1<<CHUNKSZ_LOG])),
                Some((cid("53c234e5e8472b6ac51c1ae1cab3fe06"), vec![2u8; 1<<CHUNKSZ_LOG]))
            ]
        });
        let rev = Rev::load(s.path(), "pqEKi7Jfq4bps3NVNEU49K")?;
        assert_eq!(
            rev.map,
            &[
                Some(cid("4355a46b19d348dc2f57c046f8ef63d4")),
                Some(cid("53c234e5e8472b6ac51c1ae1cab3fe06"))
            ]
        );
        assert_eq!(rev.timestamp, Utc.ymd(2019, 11, 14).and_hms_milli(14, 21, 18, 289));
        assert_eq!(rev.uuid, rid("pqEKi7Jfq4bps3NVNEU49K"));
        Ok(())
    }

    // XXX Test: missing files
    // XXX Test: format error
}
