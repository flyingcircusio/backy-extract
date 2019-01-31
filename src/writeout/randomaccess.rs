use super::compat::write_all_at;
use super::{RawChunk, WriteOut};
use crate::{PathExt, CHUNKSIZE, ZERO_CHUNK};

use crossbeam::channel::{Receiver, Sender};
use crossbeam::thread;
use failure::{format_err, Fail, Fallible, ResultExt};
use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

trait Writer {
    fn data_chunk(&self, file: &File, pos: usize, data: &[u8]) -> io::Result<()>;
    fn zero_chunk(&self, file: &File, pos: usize) -> io::Result<()>;
}

struct Continuous;

impl Writer for Continuous {
    fn data_chunk(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        write_all_at(f, data, seq as u64 * CHUNKSIZE as u64)
    }

    fn zero_chunk(&self, f: &File, seq: usize) -> io::Result<()> {
        write_all_at(f, &ZERO_CHUNK, seq as u64 * CHUNKSIZE as u64)
    }
}

struct Sparse;

const BLKSIZE: usize = 64 * 1024;

impl Writer for Sparse {
    fn data_chunk(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        let mut pos = seq as u64 * CHUNKSIZE as u64;
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
    progress: Option<Sender<usize>>,
    sparse: Option<bool>,
}

impl RandomAccess {
    pub fn new<P: AsRef<Path>>(path: P, sparse: Option<bool>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            size: 0,
            threads: 1,
            progress: None,
            sparse,
        }
    }

    fn guess_sparse(&self) -> bool {
        false
    }

    fn open(&self) -> Fallible<(File, Box<dyn Writer + Send + Sync>)> {
        let f = File::create(&self.path)?;
        let use_sparse = match f.set_len(self.size) {
            Err(err) => {
                if err.raw_os_error().unwrap_or_default() == 22 {
                    // 22 (Invalid argument): cannot resize block devices
                    self.sparse.unwrap_or_else(|| self.guess_sparse())
                } else {
                    // something is really wrong here
                    return Err(err.into());
                }
            }
            Ok(_) => self.sparse.unwrap_or(true),
        };
        Ok((
            f,
            if use_sparse {
                Box::new(Sparse)
            } else {
                Box::new(Continuous)
            },
        ))
    }

    fn run(
        &self,
        f: &File,
        rx: Receiver<RawChunk>,
        writer: &(dyn Writer + Send + Sync),
    ) -> Fallible<()> {
        for chunk in rx {
            match chunk.data {
                Some(ref data) => writer.data_chunk(&f, chunk.seq, data),
                None => writer.zero_chunk(&f, chunk.seq),
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
    fn configure(&mut self, size: u64, threads: u8, progress: Sender<usize>) {
        self.size = size;
        self.threads = threads;
        self.progress = Some(progress);
    }

    fn receive(self, chunks: Receiver<RawChunk>) -> Fallible<()> {
        let (file, writer) = self.open().with_context(|_| OutputFile(self.path.disp()))?;
        thread::scope(|s| {
            let handles: Vec<_> = (0..self.threads)
                .map(|_| {
                    let rx = chunks.clone();
                    s.spawn(|_| self.run(&file, rx, &*writer))
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

#[derive(Fail, Debug)]
#[fail(display = "Failed to open output file `{}`", _0)]
struct OutputFile(String);
