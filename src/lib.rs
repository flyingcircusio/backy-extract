mod backend;
mod chunkvec;
#[cfg(test)]
mod test_helper;
mod writeout;

use self::backend::Backend;
use self::chunkvec::{ChunkVec, Data};
pub use self::writeout::{RandomAccess, Stream};
use self::writeout::{WriteOut, WriteOutBuilder};

use console::{style, StyledObject};
use crossbeam::channel::{bounded, unbounded, Receiver};
use crossbeam::thread;
use failure::{Fail, Fallible, ResultExt};
use fs2::FileExt;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use memmap::MmapMut;
use num_cpus;
use smallvec::SmallVec;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;

// Size of an uncompressed Chunk in the backy store.
// This value must be a u32 because it is encoded as 32 bit uint the chunk file header.
pub const CHUNKSZ_LOG: usize = 22; // 4 MiB

lazy_static! {
    static ref ZERO_CHUNK: MmapMut = MmapMut::map_anon(1 << CHUNKSZ_LOG).expect("mmap");
}

// Converts file position/size into chunk sequence number
fn pos2chunk(pos: u64) -> usize {
    (pos >> CHUNKSZ_LOG) as usize
}

// Converts chunk sequence number into file offset
fn chunk2pos(seq: usize) -> u64 {
    (seq as u64) << CHUNKSZ_LOG
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Chunk {
    data: Data,
    seqs: SmallVec<[usize; 4]>,
}

fn purgelock(basedir: &Path) -> Fallible<File> {
    let f = File::create(basedir.join(".purge"))?;
    f.try_lock_exclusive()?;
    Ok(f)
}

fn step(i: u32) -> StyledObject<String> {
    style(format!("[{}/4]", i)).blue()
}

/// Controls the extraction process.
///
/// An `Extractor` must be fed an backy revision specification and a writer. It then
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
    pub fn init<P: AsRef<Path>>(revfile: P) -> Fallible<Self> {
        let revfile = revfile.as_ref();
        let basedir = revfile
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let lock = purgelock(&basedir).with_context(|_| ExtractError::Lock(basedir.disp()))?;
        let revision = fs::read_to_string(revfile)
            .context(ExtractError::LoadSpec(revfile.display().to_string()))?;
        Ok(Self {
            revision,
            threads: Self::default_threads(),
            basedir,
            lock,
            progress: ProgressBar::hidden(),
        })
    }

    fn default_threads() -> u8 {
        (num_cpus::get() / 2).max(1).min(60) as u8
    }

    /// Sets number of decompression threads. Heuristics apply in this method is never called.
    pub fn threads(&mut self, n: u8) -> &mut Self {
        if n > 0 {
            self.threads = n
        }
        self
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
    pub fn extract<W>(&self, w: W) -> Fallible<()>
    where
        W: WriteOutBuilder,
    {
        self.print_start();
        let start = Instant::now();
        let be = Backend::open(&self.basedir)?;
        let chunks = ChunkVec::decode(&self.revision, &self.basedir)?;

        self.print_decompress(chunks.len());
        let (progress, progress_rx) = unbounded();
        let writer = w.build(chunks.size, self.threads);
        let name = writer.name();

        let (chunk_tx, chunk_rx) = bounded(self.threads as usize);
        let total_bytes = thread::scope(|s| -> Fallible<u64> {
            let mut hdl = Vec::new();
            hdl.push(s.spawn(|_| writer.receive(chunk_rx, progress)));
            for threadid in 0..self.threads {
                let c_tx = chunk_tx.clone();
                let sd = |t| (&chunks).send_decompressed(t, self.threads, &be, c_tx);
                hdl.push(s.spawn(move |_| sd(threadid)));
            }
            hdl.push(s.spawn(|_| (&chunks).send_zero(chunk_tx)));
            let total_bytes = self.print_progress(chunks.size, &name, progress_rx);
            hdl.into_iter()
                .map(|h| h.join().expect("unhandled panic"))
                .collect::<Fallible<()>>()?;
            Ok(total_bytes)
        })
        .expect("subthread panic")?;
        self.print_finished(total_bytes, start);
        Ok(())
    }
}

trait PathExt {
    fn disp(&self) -> String;
}

impl PathExt for Path {
    fn disp(&self) -> String {
        format!("{}", style(self.display()).yellow())
    }
}

#[derive(Fail, Debug, PartialEq, Eq)]
pub enum ExtractError {
    #[fail(display = "Failed to load revision spec `{}'", _0)]
    LoadSpec(String),
    #[fail(display = "Chunk #{} is out of bounds (0..{})", _0, _1)]
    OutOfBounds(usize, usize),
    #[fail(display = "Image size {} is not a multiple of chunk size", _0)]
    UnalignedSize(u64),
    #[fail(display = "Unexpected file format in backup dir: {}", _0)]
    BackupFormat(String),
    #[fail(display = "Failed to acquire purge lock for backup dir `{}'", _0)]
    Lock(String),
}
