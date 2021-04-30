mod common;

use anyhow::{ensure, Result};
use backy_extract::*;
use common::{store_tar, store_with_rev, IMAGE};
use std::fs::{read, remove_file};

#[test]
fn restore_to_stream() -> Result<()> {
    let store = store_tar();
    let mut e = Extractor::init(store.path().join("VNzWKjnMqd6w58nzJwUZ98"))?;
    let mut buf = Vec::with_capacity(4 << CHUNKSZ_LOG);
    e.threads(2).extract(Stream::new(&mut buf))?;
    assert_eq!(buf.len(), IMAGE.len(), "image length mismatch");
    ensure!(buf == *IMAGE, "restored image contents mismatch");
    Ok(())
}

#[test]
fn restore_to_file() -> Result<()> {
    let store = store_tar();
    let mut e = Extractor::init(store.path().join("VNzWKjnMqd6w58nzJwUZ98"))?;
    for &sparse in &[true, false] {
        let tgt = store.path().join(format!("target_image_sparse={}", sparse));
        e.threads(3)
            .extract(RandomAccess::new(&tgt, Some(sparse)))?;
        ensure!(
            read(&tgt)? == *IMAGE,
            "restored image contents mismatch (sparse={})",
            sparse
        );
        remove_file(&tgt).ok();
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
    ensure!(buf == *IMAGE, "restored image contents mismatch");
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
