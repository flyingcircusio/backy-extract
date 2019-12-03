mod helper;

use anyhow::{ensure, Result};
use backy_extract::*;
use helper::*;
use std::fs::{read, remove_file};

#[test]
fn restore_to_stream() -> Result<()> {
    let store = store_tar();
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
fn restore_to_file() -> Result<()> {
    let store = store_tar();
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
fn restore_rev_with_holes() -> Result<()> {
    let (_store, rev) = store_with_rev(
        r#"{"mapping": {"0": "4db6e194fd398e8edb76e11054d73eb0"}, "size": 16777216}"#,
    );
    let mut e = Extractor::init(rev)?;
    let mut buf = Vec::new();
    e.threads(4).extract(Stream::new(&mut buf))?;
    ensure!(buf == image(), "restored image contents mismatch");
    Ok(())
}

#[test]
fn unaligned_size() {
    let (_store, rev) = store_with_rev(
        r#"{"mapping": {"0": "4db6e194fd398e8edb76e11054d73eb0"}, "size": 1234567}"#,
    );
    let e = Extractor::init(rev).unwrap();
    match e.extract(Stream::new(&mut Vec::new())) {
        Err(ExtractError::UnalignedSize(n)) => assert_eq!(n, 1234567),
        _ => panic!("expected ExtractError::UnalignedSize"),
    }
}
