//! Rev files (*.rev) contain additional information to the chunk map

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer};
use smallstr::SmallString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("I/O error")]
    Io(#[from] io::Error),
    #[error("Invalid data in .rev file '{}'", path.display())]
    ParseRev {
        path: PathBuf,
        source: serde_yaml::Error,
    },
    #[error("Unknown backend_type '{}' found in {}", betype, path.display())]
    WrongType { betype: String, path: PathBuf },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub type RevId = SmallString<[u8; 24]>;

fn revid_de<'de, D>(deserializer: D) -> Result<RevId, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(RevId::from(String::deserialize(deserializer)?))
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
pub struct Rev {
    pub backend_type: String,
    #[serde(deserialize_with = "timestamp_de")]
    pub timestamp: DateTime<Utc>,
    #[serde(deserialize_with = "revid_de")]
    pub uuid: RevId,
}

impl Rev {
    pub fn load<P: AsRef<Path>, I: AsRef<str>>(dir: P, id: I) -> Result<Self> {
        let map = dir.as_ref().join(id.as_ref());
        let rev = map.with_extension("rev");
        let r: Self =
            serde_yaml::from_reader(fs::File::open(&rev)?).map_err(|source| Error::ParseRev {
                path: rev.to_path_buf(),
                source,
            })?;
        if r.backend_type != "chunked" {
            return Err(Error::WrongType {
                betype: r.backend_type,
                path: rev,
            });
        }
        Ok(r)
    }
}

// see src/fuse/access.rb for tests
