#![cfg(feature = "fuse_driver")]

mod common;

use backy_extract::*;

use shells::sh;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{prelude::*, SeekFrom};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use tempdir::TempDir;
use twoway::find_bytes;

// waits for mount to be fully completed
fn wait_for_mtab(path: &Path) {
    for _ in 0..100 {
        thread::sleep(Duration::new(0, 100000));
        if find_bytes(&fs::read("/etc/mtab").unwrap(), path.as_os_str().as_bytes()).is_some() {
            return;
        }
    }
    panic!("mount {:?} not found in /etc/mtab", path);
}

struct FuseMount {
    dir: TempDir,
    mnt: PathBuf,
}

impl FuseMount {
    fn new() -> Self {
        let tmp = common::store_tar();
        let mnt = tmp.path().join("mnt");
        fs::create_dir(&mnt).unwrap();
        let basedir = tmp.path().to_owned();
        thread::spawn(move || {
            let mountpoint = basedir.join("mnt");
            env_logger::try_init().ok();
            fuse::App {
                basedir,
                mountopts: Vec::new(),
                mountpoint,
                ..Default::default()
            }
            .run()
            .unwrap();
        });
        wait_for_mtab(&mnt);
        Self { dir: tmp, mnt }
    }
}

impl Drop for FuseMount {
    fn drop(&mut self) {
        // need to reference self.dir to ensure drop order
        sh!("fusermount -u {:?}", self.dir.path().join("mnt"));
    }
}

#[test]
fn read_aligned() {
    let m = FuseMount::new();
    // readdir should return the only rev present
    let files: Vec<OsString> = fs::read_dir(&m.mnt)
        .unwrap()
        .into_iter()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(files, &[OsStr::new("VNzWKjnMqd6w58nzJwUZ98")]);
    let revfile = m.mnt.join("VNzWKjnMqd6w58nzJwUZ98");
    assert_eq!(fs::metadata(&revfile).unwrap().len(), 4 << CHUNKSZ_LOG);
    assert_eq!(fs::read(revfile).unwrap(), *common::IMAGE);
}

#[test]
fn read_unaligned() {
    let m = FuseMount::new();
    let size = 4 << CHUNKSZ_LOG;
    let mut f = File::open(m.mnt.join("VNzWKjnMqd6w58nzJwUZ98")).unwrap();
    let mut image = Vec::with_capacity(size);
    image.resize(size, 0);
    let mut cursor = 0;
    let mut readahead = 4096;
    while cursor < size {
        if let Some(mut buf) = image.get_mut(cursor..usize::min(cursor + readahead, size)) {
            match f.read(&mut buf).unwrap() {
                0 => break,
                n => cursor += n,
            }
            readahead = usize::min(2 * readahead, 128 * 1024);
        } else {
            panic!("cannot get slice from bytes buffer")
        }
    }
    drop(f);
    assert_eq!(image, *common::IMAGE);
}

#[test]
fn write_unaligned() {
    let m = FuseMount::new();
    let mut f = OpenOptions::new()
        .write(true)
        .open(m.mnt.join("VNzWKjnMqd6w58nzJwUZ98"))
        .unwrap();
    f.seek(SeekFrom::Start(CHUNKSZ as u64 - 2)).unwrap();
    assert_eq!(f.write(&[4, 5, 6, 7]).unwrap(), 4);
    drop(f);
    let mut f = File::open(m.mnt.join("VNzWKjnMqd6w58nzJwUZ98")).unwrap();
    f.seek(SeekFrom::Start(CHUNKSZ as u64 - 2)).unwrap();
    let mut buf = [0u8; 4];
    assert_eq!(f.read(&mut buf).unwrap(), 4);
    assert_eq!(&buf[..], &[4, 5, 6, 7]);
}
