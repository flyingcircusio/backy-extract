#[macro_use]
extern crate log;

use anyhow::{Context, Result};
use backy_extract::{purgelock, FuseAccess, FuseDirectory};
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::{EINVAL, EIO, ENOENT, ENOTDIR};
use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
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
        ino,
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
    buf: Vec<u8>,
}

impl BackyFS {
    fn init<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = FuseDirectory::init(dir)?;
        let mut reverse = HashMap::new();
        for (ino, entry) in dir.iter() {
            reverse.insert(entry.file_name().to_owned(), *ino);
        }
        let buf = Vec::with_capacity(1 << 15);
        Ok(Self { dir, reverse, buf })
    }
}

impl Filesystem for BackyFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, re: ReplyEntry) {
        if parent != FUSE_ROOT_ID {
            warn!("lookup(): trying to use an invalid base directory");
            re.error(ENOENT);
            return;
        }
        if name == "." || name == ".." {
            re.entry(&TTL, &ROOT_NODE, 0);
        } else {
            let path = PathBuf::from(name);
            if let Some(ino) = self.reverse.get(name) {
                if let Some(entry) = self.dir.get_mut(*ino) {
                    match entry.load_if_empty() {
                        Ok(_) => re.entry(&TTL, &fileattr(*ino, entry), 0),
                        Err(e) => {
                            error!("lookup({:?}): {}", name, e);
                            re.error(EINVAL);
                        }
                    }
                } else {
                    panic!(
                        "Internal error: lookup({:?}) -> inode {} -> no entry found",
                        name, ino
                    );
                }
            } else {
                info!("lookup(): '{}' not found", path.display());
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
            re.add(FUSE_ROOT_ID, 1, FileType::Directory, ".");
            re.add(FUSE_ROOT_ID, 2, FileType::Directory, "..");
        }
        if offset < (self.dir.len() as i64) + 2 {
            for (n, (ino, entry)) in self
                .dir
                .iter()
                .enumerate()
                .skip((offset - 2).max(0) as usize)
            {
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

    fn open(&mut self, _req: &Request, ino: u64, _flags: u32, re: ReplyOpen) {
        if ino == FUSE_ROOT_ID {
            error!("read(): trying to access directory as regular file");
            re.error(EINVAL);
            return;
        }
        if let Some(entry) = self.dir.get_mut(ino) {
            match entry.load_if_empty() {
                Ok(_) => re.opened(0, 0),
                Err(e) => {
                    error!("open(0x{:x}: {}", ino, e);
                    re.error(EINVAL);
                }
            }
        } else {
            info!("open(0x{:x}): not found", ino);
            re.error(ENOENT);
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, size: u32, re: ReplyData) {
        if ino == FUSE_ROOT_ID {
            error!("read(): trying to access directory as regular file");
            re.error(EINVAL);
            return;
        }
        if let Some(entry) = self.dir.get_mut(ino) {
            let size = size as usize;
            if self.buf.len() < size {
                self.buf.resize(size, 0);
            }
            match entry.read_at(&mut self.buf[..size], offset.try_into().unwrap()) {
                Ok(n) => re.data(&self.buf[..n]),
                Err(e) => {
                    error!("read(0x{:x} @ {}): {}", ino, offset, e);
                    re.error(EIO)
                }
            }
        } else {
            info!("read(0x{:x}): not found", ino);
            re.error(ENOENT);
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        re: ReplyWrite,
    ) {
        if ino == FUSE_ROOT_ID {
            error!("write(): trying to write to the directory");
            re.error(EINVAL);
            return;
        }
        if let Some(entry) = self.dir.get_mut(ino) {
            match entry.write_at(offset.try_into().unwrap(), &data) {
                Ok(n) if n == data.len() => re.written(n.try_into().expect("overflow")),
                Ok(n) => {
                    error!(
                        "write(0x{:x} @ {}): short write ({} of {})",
                        ino,
                        offset,
                        n,
                        data.len()
                    );
                    re.error(EIO)
                }
                Err(e) => {
                    error!("write(0x{:x} @ {}): {}", ino, offset, e);
                    re.error(EIO)
                }
            }
        } else {
            info!("write(0x{:x}): not found", ino);
            re.error(ENOENT);
        }
    }
}

#[derive(Debug, StructOpt)]
/// Access backy images via FUSE
///
/// backy-fuse maps all revisions for a specific target under a FUSE
/// mountpoint. These can be loop-mounted to extract individual files.
struct App {
    /// Backy base directory
    ///
    /// Example: /srv/backy/vm0
    #[structopt(short = "d", long, default_value = ".")]
    basedir: PathBuf,
    /// FUSE mount options
    ///
    /// See fuse(8) for possible values. Accepts multiple comma-separated
    /// values.
    #[structopt(short = "o", long, default_value = "allow_root")]
    mountopts: Vec<String>,
    /// Where to mount the FUSE filesystem [example: /mnt/backy-fuse]
    mountpoint: PathBuf,
}

fn main() -> Result<()> {
    env_logger::init();
    let app = App::from_args();
    let lock = purgelock(&app.basedir).context("Failed to acquire .purge lock")?;
    info!("Loading revisions");
    let fs = BackyFS::init(&app.basedir)?;
    fuse::mount(
        fs,
        &app.mountpoint,
        &[&OsStr::new(&format!(
            "-ofsname=backy,{}",
            app.mountopts.join(",")
        ))],
    )
    .context("Failed to mount FUSE filesystem")?;
    drop(lock);
    Ok(())
}

// XXX tests missing
