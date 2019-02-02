//! Abstract over chunked backend store.

use crate::CHUNKSZ_LOG;
use byteorder::{BigEndian, WriteBytesExt};
use failure::{Fail, Fallible};
use lazy_static::lazy_static;
#[cfg(os = "linux")]
use libc::{c_int, posix_fadvise, POSIX_FADV_NOREUSE, POSIX_FADV_SEQUENTIAL};
use memmap::Mmap;
use smallvec::{smallvec, SmallVec};
use std::fs::{read_to_string, File};
#[cfg(os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

lazy_static! {
    static ref MAGIC: SmallVec<[u8; 5]> = {
        let mut m = smallvec![0xF0];
        m.write_u32::<BigEndian>(1 << CHUNKSZ_LOG).unwrap();
        m
    };
}

#[cfg(os = "linux")]
fn fadvise(f: &File, advise: c_int) {
    unsafe {
        posix_fadvise(f.as_raw_fd(), 0, 0, advise);
        // Swallow return code since we wouldn't bail out on error anyway
    }
}

#[cfg(not(os = "linux"))]
fn fadvise(_f: &File, _advise: DummyFAdvise) {}

#[cfg(not(os = "linux"))]
#[allow(non_camel_case_types)]
enum DummyFAdvise {
    POSIX_FADV_SEQUENTIAL,
    POSIX_FADV_NOREUSE,
}

#[cfg(not(os = "linux"))]
use self::DummyFAdvise::*;

/// Computes file name for chunk with ID (relativ to backup base directory).
pub fn filename(dir: &Path, id: &str) -> PathBuf {
    dir.join(format!("chunks/{}/{}.chunk.lzo", &id[0..2], id))
}

/// Verifies store format. Currently only backy chunked v2 backends are supported.
pub fn check(dir: &Path) -> Fallible<()> {
    let version_tag = read_to_string(dir.join("chunks/store"))?;
    let version_tag = version_tag.trim();
    if version_tag != "v2" {
        Err(LoadError::VersionTag(version_tag.to_owned()).into())
    } else {
        Ok(())
    }
}

fn decompress(f: &File) -> Fallible<Vec<u8>> {
    let compressed = unsafe { Mmap::map(&f)? };
    // first 5 bytes contain header
    if compressed[0..5] != MAGIC[..] {
        return Err(LoadError::Magic.into());
    }
    Ok(minilzo::decompress(&compressed[5..], 1 << CHUNKSZ_LOG).map_err(LoadError::LZO)?)
}

/// Loads compressed chunk identified by `id` from a backend store located in `path`. The chunk is
/// decompressed on the fly and returned as raw data.
pub fn load(dir: &Path, id: &str) -> Fallible<Vec<u8>> {
    let p = filename(dir, id);
    let f = File::open(&p)?;
    fadvise(&f, POSIX_FADV_SEQUENTIAL);
    let data = decompress(&f)?;
    fadvise(&f, POSIX_FADV_NOREUSE);
    if data.len() != 1 << CHUNKSZ_LOG {
        Err(LoadError::Missized(data.len()).into())
    } else {
        Ok(data)
    }
}

#[derive(Fail, Debug, PartialEq)]
pub enum LoadError {
    #[fail(display = "Unexpected version tag in chunk store: {}", _0)]
    VersionTag(String),
    #[fail(display = "Decompressed chunk has wrong size: {} B", _0)]
    Missized(usize),
    #[fail(display = "Compressed chunk does not start with magic number")]
    Magic,
    #[fail(display = "LZO compression format error: {}", _0)]
    LZO(minilzo::Error),
}

// TODO:
// Test correupted chunk
// Test short chunk
