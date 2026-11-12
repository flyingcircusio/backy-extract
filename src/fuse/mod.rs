mod access;

use self::access::{FuseAccess, FuseDirectory};
use crate::purgelock;

use anyhow::Context;
use fuse3::raw::prelude::*;
use fuse3::{Errno, Inode, MountOptions, Result, Timestamp};
use futures_util::stream;
use futures_util::stream::Stream;
use libc;
use log::{error, info, warn};
use std::cmp::max;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::{OsStr, OsString};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};
use structopt::StructOpt;

const TTL: Duration = Duration::from_secs(1);
const BLOCK_SIZE: u32 = 512;

static ROOT_NODE: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: Timestamp { sec: 0, nsec: 0 },
    mtime: Timestamp { sec: 0, nsec: 0 },
    ctime: Timestamp { sec: 0, nsec: 0 },
    kind: FileType::Directory,
    perm: 0o0755,
    nlink: 2,
    uid: 0,
    gid: 0,
    rdev: 0,
    blksize: BLOCK_SIZE,
    #[cfg(target_os = "macos")]
    crtime: Timestamp { sec: 0, nsec: 0 },
    #[cfg(target_os = "macos")]
    flags: 0,
};

fn fileattr(ino: u64, entry: &FuseAccess) -> FileAttr {
    let timestamp: Timestamp = Into::<SystemTime>::into(entry.rev.timestamp).into();

    FileAttr {
        ino,
        size: entry.size,
        blocks: (entry.size + 511) / 512,
        atime: timestamp,
        mtime: timestamp,
        ctime: timestamp,
        kind: FileType::RegularFile,
        perm: 0o0644,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: BLOCK_SIZE,
        #[cfg(target_os = "macos")]
        crtime: timestamp,
        #[cfg(target_os = "macos")]
        flags: 0,
    }
}

struct BackyFs {
    dir: Mutex<FuseDirectory>,
    reverse: HashMap<OsString, u64>,
}

impl BackyFs {
    fn init<P: AsRef<Path>>(dir: P, cache_size: usize) -> anyhow::Result<Self> {
        let dir = FuseDirectory::init(dir, cache_size)?;
        let mut reverse = HashMap::new();
        for (ino, entry) in dir.iter() {
            reverse.insert(entry.name.to_owned(), *ino);
        }
        Ok(Self {
            dir: Mutex::new(dir),
            reverse,
        })
    }
}

macro_rules! reject_node1 {
    ($op:expr, $ino:expr) => {
        if $ino == 1 {
            error!("{}: trying to access directory as regular file", $op);
            return Err(Errno::from(libc::EINVAL));
        }
    };
}

impl Filesystem for BackyFs {
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit::new(NonZeroU32::new(16 * 1024).unwrap()))
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: Inode, name: &OsStr) -> Result<ReplyEntry> {
        let mut dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Errno::from(libc::EIO)),
        };

        if parent != 1 {
            warn!("lookup(): trying to use an invalid base directory");
            return Err(Errno::from(libc::ENOENT));
        }

        if name == "." || name == ".." {
            Ok(ReplyEntry {
                ttl: TTL,
                attr: ROOT_NODE,
                generation: 0,
            })
        } else {
            let path = PathBuf::from(name);
            if let Some(ino) = self.reverse.get(name) {
                if let Some(entry) = dir_guard.get_mut(ino) {
                    match entry.load_if_empty() {
                        Ok(_) => Ok(ReplyEntry {
                            ttl: TTL,
                            attr: fileattr(*ino, entry),
                            generation: 0,
                        }),
                        Err(e) => {
                            error!("lookup({:?}): {}", name, e);
                            Err(Errno::from(libc::EINVAL))
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
                Err(Errno::from(libc::ENOENT))
            }
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: Inode,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        let dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Errno::from(libc::EIO)),
        };

        if inode == 1 {
            return Ok(ReplyAttr {
                ttl: TTL,
                attr: ROOT_NODE,
            });
        }

        if let Some(entry) = dir_guard.get(&inode) {
            Ok(ReplyAttr {
                ttl: TTL,
                attr: fileattr(inode, entry),
            })
        } else {
            error!("getattr(): cannot find inode {}", inode);
            Err(Errno::from(libc::ENOENT))
        }
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + '_>> {
        let dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Errno::from(libc::EIO)),
        };

        if inode != 1 {
            error!("readdir() failed - inode {} is not a directory", inode);
            return Err(Errno::from(libc::ENOTDIR));
        }

        let mut entries = Vec::new();

        // Standard FUSE entries: . and ..
        if offset == 0 {
            entries.push(Ok(DirectoryEntry {
                inode: 1,
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: 1,
            }));
            entries.push(Ok(DirectoryEntry {
                inode: 1,
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            }));
        }

        // XXX: Consider simplifying
        if offset < (dir_guard.len() as i64) + 2 {
            for (n, (inode, entry)) in dir_guard
                .iter()
                .enumerate()
                .skip((offset - 2).max(0) as usize)
            {
                entries.push(Ok(DirectoryEntry {
                    inode: *inode,
                    offset: (n + 3) as i64,
                    kind: FileType::RegularFile,
                    name: entry.name.clone(),
                }))
            }
        }

        // Create a stream from the vector and box-pin it to satisfy trait bounds
        let stream = Box::pin(stream::iter(entries));

        Ok(ReplyDirectory { entries: stream })
    }

    async fn open(&self, _req: Request, inode: Inode, _flags: u32) -> Result<ReplyOpen> {
        reject_node1!("open", inode);
        let mut dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Errno::from(libc::EIO)),
        };

        if let Some(entry) = dir_guard.get_mut(&inode) {
            match entry.load_if_empty() {
                Ok(_) => Ok(ReplyOpen { fh: 0, flags: 0 }),
                Err(e) => {
                    error!("open(0x{:x}): {}", inode, e);
                    Err(Errno::from(libc::EINVAL))
                }
            }
        } else {
            info!("open(0x{:x}): not found", inode);
            Err(Errno::from(libc::ENOENT))
        }
    }

    async fn release(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        let mut dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Errno::from(libc::EIO)),
        };
        if let Some(entry) = dir_guard.get_mut(&inode) {
            entry.cleanup();
        }
        Ok(())
    }

    async fn read(
        &self,
        _r: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        reject_node1!("read", inode);

        let offset_usize: usize = offset.try_into().unwrap();
        let size_usize = size as usize;
        let mut buf;

        {
            let mut dir_guard = match self.dir.lock() {
                Ok(guard) => guard,
                Err(_) => return Err(Errno::from(libc::EIO)),
            };

            let entry = match dir_guard.get_mut(&inode) {
                Some(e) => e,
                None => {
                    info!("read(0x{:x}): not found", inode);
                    return Err(Errno::from(libc::ENOENT));
                }
            };

            let first_data = match entry.read_at(offset, size_usize) {
                Ok(d) => d,
                Err(e) => {
                    error!("read(0x{:x} @ {}): {}", inode, offset, e);
                    return Err(Errno::from(libc::ENOENT));
                }
            };

            if first_data.len() >= size_usize || first_data.is_empty() {
                // fast path: read_at gave us all data at once
                buf = first_data.to_vec();
            } else {
                buf = Vec::with_capacity(size_usize);
                buf.extend_from_slice(first_data);
                while buf.len() < size_usize {
                    let slice = match entry
                        .read_at((offset_usize + buf.len()) as u64, size_usize - buf.len())
                    {
                        Ok([]) => break, // reached EOF
                        Ok(data) => data,
                        Err(e) => {
                            error!("read(0x{:x} @ {}): {}", inode, offset, e);
                            return Err(Errno::from(libc::EIO));
                        }
                    };
                    buf.extend_from_slice(slice);
                }
            }
            Ok(ReplyData { data: buf.into() })
        }
    }

    async fn write(
        &self,
        _req: Request,
        inode: Inode,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        reject_node1!("write", inode);
        let mut dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_) => return Err(Errno::from(libc::EIO)),
        };

        if let Some(entry) = dir_guard.get_mut(&inode) {
            let mut total_written = 0;

            // Loop because we get lager write data than write_at can process
            while total_written < data.len() {
                let current_offset = offset + total_written as u64;
                let remaining_data = &data[total_written..];

                match entry.write_at(current_offset, remaining_data) {
                    Ok(0) => {
                        // 0 bytes written without an error means a stall or unexpected EOF
                        error!(
                            "write(0x{:x} @ {}): unexpected zero-length write progress",
                            inode, current_offset
                        );
                        return Err(Errno::from(libc::EIO));
                    }
                    Ok(n) => {
                        total_written += n;
                    }
                    Err(e) => {
                        error!("write(0x{:x} @ {}): {}", inode, current_offset, e);
                        return Err(Errno::from(libc::EIO));
                    }
                }
            }

            Ok(ReplyWrite {
                written: total_written.try_into().unwrap(),
            })
        } else {
            info!("write(0x{:x}): not found", inode);
            Err(Errno::from(libc::ENOENT))
        }
    }

    async fn statfs(&self, _r: Request, _ino: Inode) -> Result<ReplyStatFs> {
        let mut dir_guard = match self.dir.lock() {
            Ok(guard) => guard,
            Err(_err) => return Err(Errno::from(libc::EIO)),
        };
        let total: u64 = dir_guard
            .values_mut()
            .map(|fa| {
                fa.load_if_empty().ok();
                fa.size
            })
            .sum();

        Ok(ReplyStatFs {
            blocks: (total + 4095) / 4096,
            bfree: 0,
            bavail: 0,
            files: dir_guard.len() as u64 + 2,
            ffree: 0,
            bsize: 4096,
            namelen: 1024,
            frsize: 4096,
        })
    }
}

#[derive(Debug, Default, StructOpt)]
/// Access backy images via FUSE
///
/// backy-fuse maps all revisions for a specific target under a FUSE
/// mountpoint. These can be loop-mounted to extract individual files.
pub struct App {
    /// Backy base directory
    ///
    /// Example: /srv/backy/vm0
    #[structopt(short = "d", long, value_name = "DIRECTORY", default_value = ".")]
    pub basedir: PathBuf,
    /// FUSE mount options
    ///
    /// See fuse(8) for possible values. Accepts multiple comma-separated
    /// values
    #[structopt(short = "o", long, value_name = "OPTIONS", default_value = "")]
    pub mountopts: Vec<String>,
    /// Size of the chunk caches in MiB
    #[structopt(short, long, value_name = "SIZE", default_value = "1024")]
    pub cache: usize,
    #[structopt(name = "MOUNTPOINT")]
    /// Where to mount the FUSE filesystem [example: /mnt/backy-fuse]
    pub mountpoint: PathBuf,
}

impl App {
    pub async fn run(&self) -> anyhow::Result<()> {
        let lock = purgelock(&self.basedir).context("Failed to acquire .purge lock")?;
        info!("Loading revisions");
        let fs = BackyFs::init(&self.basedir, max(self.cache, 16) << 20)?;
        println!(
            "Mounting FUSE fileystem... unmount with: fusermount3 -u '{}'",
            self.mountpoint.display()
        );

        let mut mount_options = MountOptions::default();
        mount_options
            .fs_name("backy")
            .custom_options(self.mountopts.join(","));

        Session::new(mount_options)
            .mount(fs, &self.mountpoint)
            .await
            .unwrap()
            .await
            .unwrap();

        drop(lock);
        Ok(())
    }
}
