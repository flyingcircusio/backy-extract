use super::{RawChunk, WriteOut};
use crate::{CHUNKSIZE, ZERO_CHUNK};

use crossbeam::channel::{Receiver, Sender};
use crossbeam::thread;
use failure::{format_err, Fallible, ResultExt};
use std::fmt;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

// This method will be part of std::os::unix::fs::FileExt from 1.33.0 on
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match file.write_at(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ))
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

trait Writer {
    fn data_chunk(&self, file: &File, pos: usize, data: &[u8]) -> io::Result<()>;
    fn zero_chunk(&self, file: &File, pos: usize) -> io::Result<()>;
}

struct Continuous;

impl Writer for Continuous {
    fn data_chunk(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        write_all_at(f, data, seq as u64 * u64::from(CHUNKSIZE))
    }

    fn zero_chunk(&self, f: &File, seq: usize) -> io::Result<()> {
        write_all_at(f, &ZERO_CHUNK, seq as u64 * u64::from(CHUNKSIZE))
    }
}

struct Sparse;

const BLKSIZE: usize = 64 * 1024;

impl Writer for Sparse {
    fn data_chunk(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        let mut pos = seq as u64 * u64::from(CHUNKSIZE);
        for slice in data.chunks(BLKSIZE) {
            if slice != &ZERO_CHUNK[..BLKSIZE] {
                write_all_at(f, slice, pos)?;
            }
            pos += BLKSIZE as u64;
        }
        Ok(())
    }

    fn zero_chunk(&self, _f: &File, _seq: usize) -> io::Result<()> {
        Ok(())
    }
}

pub struct RandomAccess {
    path: PathBuf,
    size: u64,
    threads: u8,
    progress: Option<Sender<u32>>,
    writer: Box<dyn Writer + Send + Sync>,
}

impl RandomAccess {
    pub fn new<P: AsRef<Path>>(path: P, sparse: bool) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            size: 0,
            threads: 1,
            progress: None,
            writer: if sparse {
                Box::new(Sparse)
            } else {
                Box::new(Continuous)
            },
        }
    }

    fn open(&self) -> Fallible<File> {
        let path = self.path.display();
        let f = File::create(&self.path)
            .with_context(|_| format_err!("Failed to open output file `{}'", path))?;
        match f.set_len(self.size) {
            // 22 (Invalid argument): cannot resize block devices
            Err(ref err) if err.raw_os_error().unwrap_or_default() == 22 => Ok(()),
            res => res,
        }
        .context(format_err!("Failed to resize output file `{}'", path))?;
        Ok(f)
    }

    fn run(&self, f: &File, rx: Receiver<RawChunk>) -> Fallible<()> {
        for chunk in rx {
            match chunk.data {
                Some(ref data) => self.writer.data_chunk(&f, chunk.seq, data),
                None => self.writer.zero_chunk(&f, chunk.seq),
            }
            .with_context(|_| {
                format_err!(
                    "Failed to write chunk #{} to `{}'",
                    chunk.seq,
                    self.path.display()
                )
            })?;
            if let Some(ref prog) = self.progress {
                prog.send(CHUNKSIZE)?;
            }
        }
        Ok(())
    }
}

impl WriteOut for RandomAccess {
    fn configure(&mut self, size: u64, threads: u8, progress: Sender<u32>) {
        self.size = size;
        self.threads = threads;
        self.progress = Some(progress);
    }

    fn receive(self, chunks: Receiver<RawChunk>) -> Fallible<()> {
        let file = self.open()?;
        thread::scope(|s| {
            let handles: Vec<_> = (0..self.threads)
                .map(|_| {
                    let rx = chunks.clone();
                    s.spawn(|_| self.run(&file, rx))
                })
                .collect();
            handles
                .into_iter()
                .map(|hdl| hdl.join().expect("thread panic"))
                .collect::<Result<(), _>>()
        })
        .expect("subthread panic")?;
        Ok(())
    }

    fn name(&self) -> String {
        self.path.display().to_string()
    }
}

impl fmt::Debug for RandomAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<RandomAccess {}>", self.path.display())
    }
}
