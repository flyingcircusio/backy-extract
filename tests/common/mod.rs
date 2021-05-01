//! Generic test helpers

use crate::CHUNKSZ_LOG;

use flate2::bufread::GzDecoder;
use lazy_static::lazy_static;
use std::fs::{write, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use tar::Archive;
use tempdir::TempDir;

lazy_static! {
    pub static ref FIXTURES: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    pub static ref IMAGE: Vec<u8> = {
        let f = BufReader::new(File::open(FIXTURES.join("image.gz")).expect("fopen image.gz"));
        let mut buf = Vec::with_capacity(4 << CHUNKSZ_LOG);
        GzDecoder::new(f).read_to_end(&mut buf).expect("gunzip");
        buf
    };
}

pub fn store_tar() -> TempDir {
    let tmp = TempDir::new("store").expect("create tempdir");
    Archive::new(BufReader::new(
        File::open(FIXTURES.join("store.tar")).unwrap(),
    ))
    .unpack(&tmp)
    .expect("unpack store.tar");
    File::create(tmp.path().join(".purge")).unwrap();
    tmp
}

#[allow(unused)]
pub fn store_with_rev(json: &str) -> (TempDir, PathBuf) {
    let tmp = store_tar();
    let rev = tmp.path().join("REV0000000000000000000");
    write(&rev, &json).expect("write revspec");
    (tmp, rev)
}
