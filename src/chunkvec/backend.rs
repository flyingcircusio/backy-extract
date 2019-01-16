//! Abstract over chunked backend store.

use crate::CHUNKSIZE;
use byteorder::{BigEndian, WriteBytesExt};
use failure::{format_err, Fail, Fallible, ResultExt};
use lazy_static::lazy_static;
use std::fs;
use std::path::Path;

lazy_static! {
    static ref MAGIC: Vec<u8> = {
        let mut m = vec![0xF0];
        m.write_u32::<BigEndian>(CHUNKSIZE).unwrap();
        m
    };
}

pub fn check(dir: &Path) -> Fallible<()> {
    let version_tag = fs::read_to_string(dir.join("chunks/store"))?;
    if version_tag.trim() != "v2" {
        Err(format_err!("expected `v2', got `{}'", version_tag))
    } else {
        Ok(())
    }
}

pub fn load(dir: &Path, id: &str) -> Fallible<Vec<u8>> {
    let f = dir.join(format!("chunks/{}/{}.chunk.lzo", &id[0..2], id));
    Ok(fs::read(&f).with_context(|_| DecompressError::Read(f.display().to_string()))?)
}

pub fn decompress(comp: &[u8]) -> Result<Vec<u8>, DecompressError> {
    if comp[0..5] != MAGIC[..] {
        return Err(DecompressError::Magic);
    }
    // skip 5 header bytes
    Ok(minilzo::decompress(&comp[5..], CHUNKSIZE as usize).map_err(DecompressError::LZO)?)
}

#[derive(Fail, Debug)]
pub enum DecompressError {
    #[fail(display = "LZO format error: {}", _0)]
    LZO(minilzo::Error),
    #[fail(display = "Compressed chunk does not start with magic number")]
    Magic,
    #[fail(display = "Could not read chunk file `{}'", _0)]
    Read(String),
}
