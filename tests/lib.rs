use backy_extract::*;

use failure::{ensure, Fallible};
use flate2::read::GzDecoder;
use lazy_static::lazy_static;
use std::fs::{read, remove_file, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tar::Archive;
use tempdir::TempDir;

//
// Generic test helpers
//

lazy_static! {
    pub static ref FIXTURES: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
}

pub fn image() -> Vec<u8> {
    let f = File::open(FIXTURES.join("image.gz")).expect("fopen image.gz");
    let mut buf = Vec::new();
    GzDecoder::new(f)
        .read_to_end(&mut buf)
        .expect("gunzip image.gz");
    buf
}

pub fn store() -> TempDir {
    let tmp = TempDir::new("store").expect("create tempdir");
    let mut a = Archive::new(File::open(FIXTURES.join("store.tar")).expect("fopen store.tar"));
    a.unpack(&tmp).expect("unpack store.tar");
    tmp
}

pub fn store_with_rev(json: &str) -> (TempDir, Extractor) {
    let tmp = TempDir::new("store").expect("create tempdir");
    let mut a = Archive::new(File::open(FIXTURES.join("store.tar")).expect("fopen store.tar"));
    a.unpack(&tmp).expect("unpack store.tar");
    let rev = tmp.path().join("REV0000000000000000000");
    writeln!(File::create(&rev).unwrap(), "{}", json).expect("write revspec");
    (tmp, Extractor::init(rev).expect("create extractor"))
}

#[test]
fn restore_to_stream() -> Fallible<()> {
    let store = store();
    let mut e = Extractor::init(store.path().join("VNzWKjnMqd6w58nzJwUZ98"))?;
    let expected = image();
    for t in &[1, 2] {
        let mut buf = Vec::with_capacity(4 << CHUNKSZ_LOG);
        e.threads(*t).extract(Stream::new(&mut buf))?;
        assert_eq!(buf.len(), expected.len(), "length mismatch for t={}", t);
        ensure!(buf == expected, "restored image contents mismatch");
    }
    Ok(())
}

#[test]
fn restore_to_file() -> Fallible<()> {
    let store = store();
    let tgt = store.path().join("target_image");
    let mut e = Extractor::init(store.path().join("VNzWKjnMqd6w58nzJwUZ98"))?;
    let expected = image();
    for &sparse in &[true, false] {
        remove_file(&tgt).ok();
        e.threads(3)
            .extract(RandomAccess::new(&tgt, Some(sparse)))?;
        ensure!(
            read(&tgt)? == expected,
            "restored image contents mismatch (sparse={})",
            sparse
        );
    }
    Ok(())
}

#[test]
fn restore_rev_with_holes() -> Fallible<()> {
    let (_store, mut e) = store_with_rev(
        r#"{"mapping": {"0": "4db6e194fd398e8edb76e11054d73eb0"}, "size": 16777216}"#,
    );
    let mut buf = Vec::new();
    e.threads(4).extract(Stream::new(&mut buf))?;
    ensure!(buf == image(), "restored image contents mismatch");
    Ok(())
}

#[test]
fn chunk_over_size() {
    let (_store, e) = store_with_rev(
        r#"
{"mapping": {"0": "4db6e194fd398e8edb76e11054d73eb0", "1": "c72b4ba82d1f51b71c8a18195ad33fc8",
             "2": "c72b4ba82d1f51b71c8a18195ad33fc8", "3": "c72b4ba82d1f51b71c8a18195ad33fc8"},
 "size": 12582912}"#,
    );
    if let Err(err) = e.extract(Stream::new(&mut Vec::new())) {
        assert_eq!(
            err.downcast::<ExtractError>().unwrap(),
            ExtractError::OutOfBounds(3, 3)
        );
    } else {
        panic!("expected ExtractError::OutOfBounds");
    }
}

#[test]
fn unaligned_size() {
    let (_store, e) = store_with_rev(
        r#"{"mapping": {"0": "4db6e194fd398e8edb76e11054d73eb0"}, "size": 1234567}"#,
    );
    if let Err(err) = e.extract(Stream::new(&mut Vec::new())) {
        assert_eq!(
            err.downcast::<ExtractError>().unwrap(),
            ExtractError::UnalignedSize(1234567)
        );
    } else {
        panic!("expected ExtractError::OutOfBounds");
    }
}
