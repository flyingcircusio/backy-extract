//! Generic test helpers

use flate2::read::GzDecoder;
use lazy_static::lazy_static;
use std::fs::{write, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use tar::Archive;
use tempdir::TempDir;

lazy_static! {
    pub static ref FIXTURES: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
}

#[allow(dead_code)]
pub fn image() -> Vec<u8> {
    let f = File::open(FIXTURES.join("image.gz")).expect("fopen image.gz");
    let mut buf = Vec::new();
    GzDecoder::new(f)
        .read_to_end(&mut buf)
        .expect("gunzip image.gz");
    buf
}

pub fn store_tar() -> TempDir {
    let tmp = TempDir::new("store").expect("create tempdir");
    Archive::new(File::open(FIXTURES.join("store.tar")).unwrap())
        .unpack(&tmp)
        .expect("unpack store.tar");
    File::create(tmp.path().join(".purge")).unwrap();
    tmp
}

#[allow(dead_code)]
pub fn store_with_rev(json: &str) -> (TempDir, PathBuf) {
    let tmp = store_tar();
    let rev = tmp.path().join("REV0000000000000000000");
    write(&rev, &json).expect("write revspec");
    (tmp, rev)
}
