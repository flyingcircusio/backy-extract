//! High-performance, multi-threaded backy image restore library.
//!
//! `backy_extract` reads an backup revision from a
//! [backy](https://bitbucket.org/flyingcircus/backy) *chunked v2* data store, decompresses it
//! on the fly and writes it to a restore target using pluggable writeout modules.

mod backend;
mod chunkvec;
#[cfg(feature = "fuse_driver")]
mod fuse_access;
#[cfg(test)]
mod test_helper;
mod writeout;

use self::backend::Backend;
use self::chunkvec::ChunkVec;
#[cfg(feature = "fuse_driver")]
pub use self::fuse_access::{FuseAccess, FuseDirectory};
pub use self::writeout::{RandomAccess, Stream};
use self::writeout::{WriteOut, WriteOutBuilder};

use console::{style, StyledObject};
use crossbeam::channel::{bounded, unbounded, Receiver};
use crossbeam::thread;
use fs2::FileExt;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use memmap::MmapMut;
use smallvec::SmallVec;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExtractError {
    #[error("Failed to load revision spec '{0}'")]
    LoadSpec(PathBuf, #[source] io::Error),
    #[error("Failed to parse revision map JSON: {0}")]
    DecodeMap(String, #[source] serde_json::Error),
    #[error("Image size {0} is not a multiple of chunk size")]
    UnalignedSize(u64),
    #[error("Unexpected file format in backup dir '{}'", .0.display())]
    BackupFormat(PathBuf),
    #[error("Failed to acquire purge lock for backup dir '{}'", .0.display())]
    Lock(PathBuf, #[source] io::Error),
    #[error("Error while loading chunk #{seq} ({id})")]
    InvalidChunk {
        seq: usize,
        id: String,
        source: backend::Error,
    },
    #[error("Chunked backend error")]
    Backend(#[from] backend::Error),
    #[error("IPC error")]
    SendChunk(#[from] crossbeam::SendError<Chunk>),
    #[error("Write error")]
    WriteError(#[from] writeout::Error),
}

type Result<T, E = ExtractError> = std::result::Result<T, E>;

/// Size of an uncompressed Chunk in the backy store as 2's exponent.
// The resulting value must be a u32 because it is encoded as 32 bit uint the chunk file header.
pub const CHUNKSZ_LOG: usize = 22; // 4 MiB

lazy_static! {
    static ref ZERO_CHUNK: MmapMut = MmapMut::map_anon(1 << CHUNKSZ_LOG).expect("mmap");
}

/// Transport of a single image data chunk.
///
/// A chunk needs to be placed into all logical positions that are listed in the `seqs`
/// attribute. Each seq starts at offset (seq << CHUNKSZ_LOG) bytes in the restored image.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Chunk {
    pub data: Data,
    pub seqs: SmallVec<[usize; 4]>,
}

/// Block of uncompressed image contents of length (1 << CHUNKSZ_LOG).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Data {
    Some(Vec<u8>),
    /// Shortcut if the whole block consists only of zeros.
    Zero,
}

// Converts file position/size into chunk sequence number
fn pos2chunk(pos: u64) -> usize {
    (pos >> CHUNKSZ_LOG) as usize
}

// Converts chunk sequence number into file offset
fn chunk2pos(seq: usize) -> u64 {
    (seq as u64) << CHUNKSZ_LOG
}

/// Aqcuire 'purge' lock which prevents backy from deleting chunks
pub fn purgelock(basedir: &Path) -> Result<File, io::Error> {
    let f = OpenOptions::new()
        .write(true)
        .create(false)
        .open(basedir.join(".purge"))?;
    f.try_lock_shared()?;
    Ok(f)
}

fn step(i: u32) -> StyledObject<String> {
    style(format!("[{}/4]", i)).blue()
}

/// Controls the extraction process.
///
/// An `Extractor` must be initialized with a backy revision specification and a writer. It then
/// reads chunks from the revision, decompresses them in parallel and dumps them to the
/// caller-supplied writer.
#[derive(Debug)]
pub struct Extractor {
    revision: String,
    threads: u8,
    basedir: PathBuf,
    lock: File,
    progress: ProgressBar,
}

impl Extractor {
    /// Creates new `Extractor` instance.
    ///
    /// The revision specification is loaded from `revfile`. The data directory is assumed to be
    /// the same directory as where revfile is located.
    pub fn init<P: AsRef<Path>>(revfile: P) -> Result<Self> {
        let revfile = revfile.as_ref();
        let basedir = revfile
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let lock = purgelock(&basedir).map_err(|e| ExtractError::Lock(basedir.clone(), e))?;
        let revision = fs::read_to_string(revfile)
            .map_err(|e| ExtractError::LoadSpec(revfile.to_owned(), e))?;
        Ok(Self {
            revision,
            threads: Self::default_threads(),
            basedir,
            lock,
            progress: ProgressBar::hidden(),
        })
    }

    /// Sets number of decompression threads. Heuristics apply in this method is never called.
    pub fn threads(&mut self, n: u8) -> &mut Self {
        if n > 0 {
            self.threads = n
        }
        self
    }

    fn default_threads() -> u8 {
        (num_cpus::get() / 2).max(1).min(60) as u8
    }

    /// Enables/disables a nice progress bar on stderr while restoring.
    pub fn progress(&mut self, show: bool) -> &mut Self {
        self.progress = if show {
            ProgressBar::new(1)
        } else {
            ProgressBar::hidden()
        };
        self
    }

    fn print_start(&self) {
        self.progress
            .println(format!("{} Loading chunk map", step(1)));
    }

    fn print_decompress(&self, nchunks: usize) {
        self.progress.println(format!(
            "{} Decompressing {} chunks in background using {} thread(s)",
            step(2),
            style(nchunks.to_string()).cyan(),
            self.threads
        ));
    }

    fn print_progress(&self, total_size: u64, name: &str, written: Receiver<usize>) -> u64 {
        self.progress
            .println(format!("{} Restoring to {}", step(3), style(name).yellow()));
        self.progress.set_length(total_size);
        self.progress
            .set_style(ProgressStyle::default_bar().template(
                "{bytes:>9.yellow}/{total_bytes:.green} {bar:52.cyan/blue} ({elapsed}/{eta})",
            ));
        self.progress.inc(0);
        self.progress.set_draw_delta(total_size / 1000);
        let mut total = 0;
        for bytes in written {
            let bytes = bytes as u64;
            self.progress.inc(bytes);
            total += bytes;
        }
        self.progress.finish_and_clear();
        total
    }

    fn print_finished(&self, written: u64, started: Instant) {
        let rt = Instant::now().duration_since(started);
        let runtime = rt.as_secs() as f64 + f64::from(rt.subsec_micros()) / 1e6;
        let rate = written as f64 / runtime.max(1.0);
        self.progress.println(format!(
            "{} Finished restoring {} in {:.1}s ({}/s)",
            step(4),
            style(HumanBytes(written)).green(),
            runtime,
            HumanBytes(rate.round() as u64)
        ));
    }

    /// Initiates the restore process.
    ///
    /// Accepts a `WriteOutBuilder` which is used to instantiate the final writer. Currently
    /// supported WriteOutBuilders are [Stream](struct.Stream.html) and
    /// [RandomAccess](struct.RandomAccess.html).
    pub fn extract<W>(&self, w: W) -> Result<()>
    where
        W: WriteOutBuilder,
    {
        self.print_start();
        let start = Instant::now();
        let be = Backend::open(&self.basedir)?;
        let chunks = ChunkVec::decode(&self.revision)?;

        self.print_decompress(chunks.len());
        let (progress, progress_rx) = unbounded();
        let writer = w.build(chunks.size, self.threads);
        let name = writer.name();

        let (chunk_tx, chunk_rx) = bounded(self.threads as usize);
        let total_bytes = thread::scope(|s| -> Result<u64> {
            let mut hdl = vec![s.spawn(|_| writer.receive(chunk_rx, progress).map_err(Into::into))];
            for threadid in 0..self.threads {
                let c_tx = chunk_tx.clone();
                let sd = |t| (&chunks).send_decompressed(t, self.threads, &be, c_tx);
                hdl.push(s.spawn(move |_| sd(threadid)));
            }
            hdl.push(s.spawn(|_| (&chunks).send_zero(chunk_tx)));
            let total_bytes = self.print_progress(chunks.size, &name, progress_rx);
            hdl.into_iter()
                .try_for_each(|h| h.join().expect("unhandled panic"))?;
            Ok(total_bytes)
        })
        .expect("subthread panic")?;
        self.print_finished(total_bytes, start);
        Ok(())
    }
}
