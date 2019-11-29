#[macro_use]
extern crate log;

use backy_extract::{FuseAccess, FuseDirectory};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
    FUSE_ROOT_ID,
};
use libc::{EINVAL, ENOENT, ENOTDIR};
use std::collections::HashMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::path::PathBuf;
use structopt::StructOpt;
use time::Timespec;

static TTL: Timespec = Timespec { sec: 1, nsec: 1 };

static UNIX_EPOCH: Timespec = Timespec { sec: 0, nsec: 0 };

static ROOT_NODE: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH,
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o0755,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,
    flags: 0,
};

fn fileattr(ino: u64, entry: &FuseAccess) -> FileAttr {
    let timestamp = Timespec::new(entry.rev.timestamp.timestamp(), 0);
    FileAttr {
        ino: ino,
        size: entry.size,
        blocks: (entry.rev.stats.bytes_written + 511) / 512,
        atime: timestamp,
        mtime: timestamp,
        ctime: timestamp,
        crtime: timestamp,
        kind: FileType::RegularFile,
        perm: 0o0644,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

struct BackyFS {
    dir: FuseDirectory,
    reverse: HashMap<OsString, u64>,
}

impl BackyFS {
    fn init<P: AsRef<Path>>(dir: P) -> Result<Self, Box<dyn Error>> {
        let dir = FuseDirectory::init(dir)?;
        let mut reverse = HashMap::new();
        for (ino, entry) in dir.iter() {
            reverse.insert(entry.file_name().to_owned(), *ino);
        }
        Ok(Self { dir, reverse })
    }
}

impl Filesystem for BackyFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, re: ReplyEntry) {
        if parent != 1 {
            warn!("lookup(): trying to use an invalid base directory");
            re.error(ENOENT);
            return;
        }
        if name == "." || name == ".." {
            re.entry(&TTL, &ROOT_NODE, 0);
        } else {
            let path = PathBuf::from(name);
            if let Some(ino) = self.reverse.get(name) {
                if let Some(entry) = self.dir.get(*ino) {
                    re.entry(&TTL, &fileattr(*ino, entry), 0)
                } else {
                    error!(
                        "lookup(): '{}' -> inode {} -> no entry found",
                        path.display(),
                        ino
                    );
                    re.error(EINVAL)
                }
            } else {
                warn!("lookup(): '{}' not found", path.display());
                re.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, re: ReplyAttr) {
        if ino == 1 {
            re.attr(&TTL, &ROOT_NODE);
            return;
        }
        if let Some(entry) = self.dir.get(ino) {
            re.attr(&TTL, &fileattr(ino, entry));
        } else {
            error!("getattr(): cannot find inode {}", ino);
            re.error(ENOENT);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut re: ReplyDirectory) {
        if ino != FUSE_ROOT_ID {
            error!("readdir() failed - inode {} is not a directory", ino);
            re.error(ENOTDIR);
            return;
        }
        if offset == 0 {
            re.add(1, 1, FileType::Directory, ".");
            re.add(1, 2, FileType::Directory, "..");
        }
        if offset >= 2 {
            for (n, (ino, entry)) in self.dir.iter().enumerate().skip((offset - 2) as usize) {
                re.add(
                    *ino,
                    (n + 3) as i64,
                    FileType::RegularFile,
                    entry.file_name(),
                );
            }
        }
        re.ok()
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Exports all backup images in a FUSE filesystem")]
struct App {
    /// Backy base directory [example: /srv/backy]
    #[structopt(short, long, default_value = ".")]
    basedir: PathBuf,
    /// Where to mount the FUSE filesystem [example: /mnt/backy-fuse]
    mountpoint: PathBuf,
}

// XXX anyhow
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let app = App::from_args();
    let fs = BackyFS::init(&app.basedir)?;
    fuse::mount(fs, &app.mountpoint, &[&OsStr::new("-ofsname=backy")]).map_err(Into::into)
}
