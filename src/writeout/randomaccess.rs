use super::compat::write_all_at;
use super::{RawChunk, WriteOut};
use crate::{PathExt, CHUNKSZ_LOG, ZERO_CHUNK, chunk2pos, pos2chunk};

use crossbeam::channel::{Receiver, Sender};
use crossbeam::thread;
use failure::{Fail, Fallible, ResultExt};
use rand::distributions::Uniform;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

trait Writer {
    fn data(&self, file: &File, pos: usize, data: &[u8]) -> io::Result<()>;
    fn zero(&self, file: &File, pos: usize) -> io::Result<()>;
}

struct Continuous;

impl Writer for Continuous {
    fn data(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        write_all_at(f, data, chunk2pos(seq))
    }

    fn zero(&self, f: &File, seq: usize) -> io::Result<()> {
        write_all_at(f, &ZERO_CHUNK, chunk2pos(seq))
    }
}

struct Sparse;

const BLKSIZE: usize = 64 * 1024;

impl Writer for Sparse {
    fn data(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        let mut pos = chunk2pos(seq);
        for slice in data.chunks(BLKSIZE) {
            if slice != &ZERO_CHUNK[..BLKSIZE] {
                write_all_at(f, slice, pos)?;
            }
            pos += BLKSIZE as u64;
        }
        Ok(())
    }

    fn zero(&self, _f: &File, _seq: usize) -> io::Result<()> {
        Ok(())
    }
}

struct SampleIter {
    chunks: usize,
    i: usize,
    n: usize,
    dist: Uniform<usize>,
    rng: ThreadRng,
}

impl SampleIter {
    fn new(chunks: usize) -> Self {
        Self {
            chunks,
            i: 0,
            n: chunks / 100,
            dist: Uniform::from(0..chunks),
            rng: rand::thread_rng(),
        }
    }
}

impl Iterator for SampleIter {
    type Item = usize;

    // Cover the first and last chunk in any case and (n-2) random samples in between
    fn next(&mut self) -> Option<Self::Item> {
        self.i += 1;
        match self.i {
            1 => Some(0),
            2 => Some(self.chunks - 1),
            _ if self.i <= self.n => Some(self.dist.sample(&mut self.rng)),
            _ => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.n.max(2)))
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

    // Heuristic: read the first and the last chunk and 1% of the chunks in between. If there
    // is any non-zero data detected, this device has not been discarded and must be writte in
    // non-sparse mode.
    fn guess_sparse(&self, dev: &mut File) -> Fallible<bool> {
        // not worth the effort
        if self.size <= 2 << CHUNKSZ_LOG {
            return Ok(false);
        }
        let mut buf = vec![0; 1 << CHUNKSZ_LOG];
        for chunk in SampleIter::new(pos2chunk(self.size)) {
            dev.seek(io::SeekFrom::Start(chunk2pos(chunk)))?;
            dev.read_exact(&mut buf)?;
            if buf != &ZERO_CHUNK[..] {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // Opens restore target (file/dev) as stated in self.path. Resizes file accordingly and gives a
    // guess if sparse mode can be used or not.
    fn open(&self) -> Fallible<(File, bool)> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.path)?;
        let sparse_guess = match f.set_len(self.size) {
            Err(err) => {
                if err.raw_os_error().unwrap_or_default() == 22 {
                    // 22 (Invalid argument): cannot resize block devices
                    self.guess_sparse(&mut f)
                        .context("Patrol read failed (is the restore device large enough?)")?
                } else {
                    // truncate failed with errno != 22 => we're borked
                    return Err(err.into());
                }
            }
            Ok(_) => true
        };
        f.seek(io::SeekFrom::Start(0))?;
        Ok((f, sparse_guess))
    }

    fn run(
        &self,
        f: &File,
        rx: Receiver<RawChunk>,
        writer: &(dyn Writer + Send + Sync),
    ) -> Fallible<()> {
        for chunk in rx {
            match chunk.data {
                Some(ref data) => writer.data(&f, chunk.seq, data),
                None => writer.zero(&f, chunk.seq),
            }
            .with_context(|_| WriteChunkErr(chunk.seq, self.path.disp()))?;
            if let Some(ref prog) = self.progress {
                prog.send(1 << CHUNKSZ_LOG)?;
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
        let (f, guess) = self.open().with_context(|_| OutputFileErr(self.path.disp()))?;
        let writer: Box<dyn Writer + Send + Sync> = if self.sparse.unwrap_or(guess) {
            Box::new(Sparse)
        } else {
            Box::new(Continuous)
        };
        thread::scope(|s| {
            let handles: Vec<_> = (0..self.threads)
                .map(|_| {
                    let rx = chunks.clone();
                    s.spawn(|_| self.run(&f, rx, &*writer))
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
#[fail(display = "Failed to open output file `{}'", _0)]
struct OutputFileErr(String);

#[derive(Fail, Debug)]
#[fail(display = "Failed to write chunk #{} to `{}'", _0, _1)]
struct WriteChunkErr(usize, String);

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn sparse_mode_should_be_guessed_on_empty_file() -> Fallible<()> {
        let td = TempDir::new("sparse")?;
        let p = td.path().join("dev");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&p)?;
        for _ in 0..4 {
            f.write_all(&ZERO_CHUNK[..])?;
        }
        let mut ra = RandomAccess::new(p, None);
        ra.size = 4 << CHUNKSZ_LOG;
        Ok(assert!(ra.guess_sparse(&mut f)?))
    }

    #[test]
    fn sparse_mode_should_not_be_guessed_on_nonempty_file() -> Fallible<()> {
        let td = TempDir::new("sparse")?;
        let p = td.path().join("dev");
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&p)?;
        for _ in 0..4 {
            f.write_all(&ZERO_CHUNK[..])?;
        }
        f.seek(io::SeekFrom::Start(3 << CHUNKSZ_LOG))?;
        f.write_all(b"1")?;
        let mut ra = RandomAccess::new(p, None);
        ra.size = 4 << CHUNKSZ_LOG;
        Ok(assert!(!ra.guess_sparse(&mut f)?))
    }
}
