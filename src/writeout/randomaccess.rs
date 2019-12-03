use super::{Error, Result, WriteOut, WriteOutBuilder};
use crate::{chunk2pos, pos2chunk, Chunk, Data, CHUNKSZ_LOG, ZERO_CHUNK};

use crossbeam::channel::{Receiver, Sender};
use crossbeam::thread;
use rand::distributions::Uniform;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use std::fmt;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

/// File/block device restore target.
///
/// Chunks are written out-of-order as they are sent to the writer. Zeros are not written (i.e.,
/// skipped over) if sparse mode is enabled.
#[derive(Debug, Clone)]
pub struct RandomAccess {
    path: PathBuf,
    sparse: Option<bool>,
}

impl RandomAccess {
    /// Creates a builder which is finalized later by the [Extractor](struct.Extractor.html). If
    /// the user explicitely requests sparse to be active or not, set `sparse` to some bool. If
    /// sparse mode is not explicitely set, a heuristic is applied: files which can be truncated
    /// are written sparsely. Block devices are written sparsely if a patrol read suggests that
    /// they do not contain previously written data, e.g. after a discard.
    pub fn new<P: AsRef<Path>>(path: P, sparse: Option<bool>) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            sparse,
        }
    }
}

impl WriteOutBuilder for RandomAccess {
    type Impl = RAWriteOut;

    fn build(self, size: u64, threads: u8) -> Self::Impl {
        RAWriteOut {
            path: self.path,
            sparse: self.sparse,
            size,
            threads,
        }
    }
}

#[derive(Clone, Default)]
pub struct RAWriteOut {
    path: PathBuf,
    sparse: Option<bool>,
    size: u64,
    threads: u8,
}

impl RAWriteOut {
    // Heuristic: read the first and the last chunk and 1% of the chunks in between. If there
    // is any non-zero data detected, this device has not been discarded and must be writte in
    // non-sparse mode.
    fn guess_sparse(&self) -> io::Result<bool> {
        // not worth the effort
        if self.size <= 2 << CHUNKSZ_LOG {
            return Ok(false);
        }
        let mut buf = vec![0; 1 << CHUNKSZ_LOG];
        let mut dev = File::open(&self.path)?;
        for chunk in RandomSample::new(pos2chunk(self.size)) {
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
    fn open(&self) -> Result<(File, bool), io::Error> {
        let mut f = File::create(&self.path)?;
        let sparse_guess = match f.set_len(self.size) {
            Err(err) => {
                if err.raw_os_error().unwrap_or_default() == 22 {
                    // 22 (Invalid argument): cannot resize block devices
                    self.guess_sparse()?
                } else {
                    // truncate failed with errno != 22 => we're borked
                    return Err(err);
                }
            }
            Ok(_) => true,
        };
        f.seek(io::SeekFrom::Start(0))?;
        Ok((f, sparse_guess))
    }

    fn run(
        &self,
        f: &File,
        rx: &Receiver<Chunk>,
        prog: &Sender<usize>,
        writer: &(dyn Writer + Send + Sync),
    ) -> Result<()> {
        rx.into_iter()
            .map(|chunk| -> Result<()> {
                match chunk.data {
                    Data::Some(ref data) => {
                        for seq in &chunk.seqs {
                            writer.data(&f, *seq, data).map_err(|e| {
                                Error::WriteChunkFile(*seq, self.path.to_owned(), e)
                            })?;
                        }
                    }
                    Data::Zero => {
                        for seq in &chunk.seqs {
                            writer.zero(&f, *seq).map_err(|e| {
                                Error::WriteChunkFile(*seq, self.path.to_owned(), e)
                            })?;
                        }
                    }
                }
                prog.send(chunk.seqs.len() << CHUNKSZ_LOG)?;
                Ok(())
            })
            .collect()
    }
}

impl WriteOut for RAWriteOut {
    fn receive(self, chunks: Receiver<Chunk>, progress: Sender<usize>) -> Result<()> {
        let (f, guess) = self
            .open()
            .map_err(|e| Error::OutputFile(self.path.to_owned(), e))?;
        let writer: Box<dyn Writer + Send + Sync> = if self.sparse.unwrap_or(guess) {
            Box::new(Sparse)
        } else {
            Box::new(Continuous)
        };
        thread::scope(|s| {
            let handles: Vec<_> = (0..self.threads)
                .map(|_| s.spawn(|_| self.run(&f, &chunks, &progress, &*writer)))
                .collect();
            handles
                .into_iter()
                .map(|hdl| hdl.join().expect("thread panic"))
                .collect::<Result<()>>()
        })
        .expect("subthread panic")?;
        Ok(())
    }

    fn name(&self) -> String {
        self.path.display().to_string()
    }
}

impl fmt::Debug for RAWriteOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<RAWriteOut {}>", self.path.display())
    }
}

struct RandomSample {
    chunks: usize,
    i: usize,
    n: usize,
    dist: Uniform<usize>,
    rng: ThreadRng,
}

impl RandomSample {
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

impl Iterator for RandomSample {
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

trait Writer {
    fn data(&self, file: &File, pos: usize, data: &[u8]) -> io::Result<()>;
    fn zero(&self, file: &File, pos: usize) -> io::Result<()>;
}

struct Continuous;

impl Writer for Continuous {
    fn data(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        f.write_all_at(data, chunk2pos(seq))
    }

    fn zero(&self, f: &File, seq: usize) -> io::Result<()> {
        f.write_all_at(&ZERO_CHUNK, chunk2pos(seq))
    }
}

struct Sparse;

const BLKSIZE: usize = 64 * 1024;

impl Writer for Sparse {
    fn data(&self, f: &File, seq: usize, data: &[u8]) -> io::Result<()> {
        let mut pos = chunk2pos(seq);
        for slice in data.chunks(BLKSIZE) {
            if slice != &ZERO_CHUNK[..BLKSIZE] {
                f.write_all_at(slice, pos)?;
            }
            pos += BLKSIZE as u64;
        }
        Ok(())
    }

    fn zero(&self, _f: &File, _seq: usize) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    fn sparse_mode_test<F>(modifier: F) -> io::Result<bool>
    where
        F: FnOnce(&mut File) -> io::Result<()>,
    {
        let td = TempDir::new("sparse")?;
        let p = td.path().join("dev");
        {
            let mut f = File::create(&p)?;
            modifier(&mut f)?;
            f.seek(io::SeekFrom::Start((4 << CHUNKSZ_LOG) - 1))?;
            f.write(b"\0")?;
        }
        let mut ra = RAWriteOut::default();
        ra.path = p;
        ra.size = 4 << CHUNKSZ_LOG;
        ra.guess_sparse()
    }

    #[test]
    fn sparse_mode_should_be_guessed_on_empty_file() {
        assert!(sparse_mode_test(|_| Ok(())).unwrap())
    }

    #[test]
    fn sparse_mode_should_not_be_guessed_on_nonempty_file() {
        assert!(!sparse_mode_test(|f| {
            f.seek(io::SeekFrom::Start(3 << CHUNKSZ_LOG))?;
            f.write_all(b"1")
        })
        .unwrap())
    }
}
