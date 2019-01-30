mod writeout;

pub use self::writeout::{RandomAccess, Stream, WriteOut};
use backend;
use chunk_bucket::Bucket;

use console::{style, StyledObject};
use crossbeam::channel::{bounded, unbounded, Receiver};
use crossbeam::thread;
use failure::{format_err, Fail, Fallible, ResultExt};
use fs2::FileExt;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use memmap::MmapMut;
use num_cpus;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Size of an uncompressed Chunk in the backy store. This value must fit into u32 because of the
/// chunk file header's size limit.
pub const CHUNKSIZE: u64 = 4 * 1024 * 1024;

lazy_static! {
    static ref ZERO_CHUNK: MmapMut = MmapMut::map_anon(CHUNKSIZE as usize).expect("mmap zero");
}

fn acquire_lock(basedir: &Path) -> Fallible<File> {
    let f = File::create(basedir.join(".purge"))?;
    f.try_lock_exclusive()?;
    Ok(f)
}

fn step(i: u32) -> StyledObject<String> {
    style(format!("[{}/4]", i)).blue()
}

/// Controls the extraction process
///
/// An `Extractor` must be fed an backy revision specification and a writer. It then
/// reads chunks from the revision, decompresses them in parallel and dumps them to the
/// caller-supplied `WriteOut` instance.
#[derive(Debug)]
pub struct Extractor {
    basedir: PathBuf,
    revfile: PathBuf,
    revision: String,
    threads: u8,
    lock: File,
    started: Instant;
    progress: ProgressBar,
}

impl Extractor {
    /// Creates new `Extractor` instance
    ///
    /// The revision specification is loaded from `revfile`. The data directory is assumed to be
    /// the same directory as where revfile is located. If `show_progress` is true, a progress bar
    /// is displayed on stdout.
    pub fn init<P: AsRef<Path>>(revfile: P) -> Fallible<Self> {
        let revfile = revfile.as_ref();
        let revision = fs::read_to_string(revfile)
            .context(ExtractError::LoadSpec(revfile.display().to_string()))?;
        let basedir = revfile.parent().unwrap().to_path_buf(); // revfile must be != "/" here
        let lock = acquire_lock(&basedir).context(format_err!(
            "Cannot lock backup dir `{}'",
            basedir.display()
        ))?;
        Ok(Self {
            basedir,
            revfile: revfile.to_path_buf(),
            revision,
            threads: Self::default_threads(),
            lock,
            started: Instant::now(),
            progress: ProgressBar::hidden(),
        })
    }

    fn default_threads() -> u8 {
        num_cpus::get().max(1).min(60) as u8
    }

    /// Sets number of decompression threads. Heuristics apply in this method is never called.
    pub fn threads(&mut self, n: u8) -> &mut Self {
        if n > 0 {
            self.threads = n
        }
        self
    }

    /// Enables a nice progress bar on stderr while restoring.
    pub fn progress(&mut self, show: bool) -> &mut Self {
        self.progress = if show {
            ProgressBar::new(1)
        } else {
            ProgressBar::hidden()
        };
        self
    }

    fn print_init(&self, nchunks: usize) {
        self.progress.println(format!("{} Loading chunk map", step(1)));
    }

    fn print_decompress(&self, nchunks: usize) {
        self.progress.println(format!(
            "{} Decompressing {} chunks in background using {} thread(s)",
            step(2),
            style(nchunks.to_string()).cyan(),
            self.threads
        ));
    }

    fn print_progress(&self, total_size: u64, name: &str, written: Receiver<u32>) -> u64 {
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
            let bytes = u64::from(bytes);
            self.progress.inc(bytes);
            total += bytes;
        }
        self.progress.finish_and_clear();
        total
    }

    fn print_finished(&self, written: u64) {
        let t = Instant::now().duration_since(self.started);
        let runtime = t.as_secs() as f64 + f64::from(t.subsec_micros()) / 1e6;
        let rate = written as f64 / runtime.max(1.0);
        self.progress.println(format!(
            "{} Finished restoring {} in {:.1}s ({}/s)",
            step(4),
            style(HumanBytes(written)).green(),
            runtime,
            HumanBytes(rate.round() as u64)
        ));
    }

    /// Initiates the restore process to an already instantiated `writer` object.
    pub fn extract<W>(&self, write: W) -> Fallible<()>
    where
        W: WriteBuilder,
    {
        self.print_init();
        let backend = backend::Chunked_V2::init(&self.revision, &self.revfile)?;
        let bucket = Bucket::new(&backend);

        self.print_decompress(backend.chunks());
        let (res, res_rx) = bounded(self.threads as usize);
        let (progress_tx, progress_rx) = bounded(self.threads as usize);
        let writer = write.writer(progress_tx, &bucket, backend.size(), self.threads)

        let name = writer.name();
        let total_bytes = thread::scope::<Fallible<u64>>(|s| {
            let (load_tx, load_rx) = bounded(self.threads as usize);
            let (write_tx, write_rx) = bounded(self.threads as usize);
            for _ in 0..self.threads {
                s.spawn(|_| (&load_rx).iter().map(|id| {
                    bucket.add(id, &write_tx)
                }).collect::<Fallible<()>>())?;
            }
            drop(load_tx);
            s.spawn(|_| backend.zero_ids().map(|id| write_tx.send(id)).collect::<Fallible<()>>())?;
            drop(write_tx);
            s.spawn(|_| res.send(writer.receive(write_tx)));
            for id in backend.data_ids() {
                load_tx.send(id)
            }
            Ok(self.print_progress(chunks.size, &name, progress_rx))
        })
        .expect("subthread panic")?;

        drop(res);
        self.print_finished(total_bytes, start);
        res_rx.iter().collect()
    }
}

#[derive(Fail, Debug, PartialEq, Eq)]
pub enum ExtractError {
    #[fail(display = "Failed to load revision spec `{:50.50}…'", _0)]
    LoadSpec(String),
    #[fail(display = "Chunk #{} is out of bounds (0..{})", _0, _1)]
    OutOfBounds(usize, usize),
    #[fail(display = "Image size {} is not a multiple of chunk size", _0)]
    UnalignedSize(u64),
    #[fail(display = "Unexpected file format in backup dir: {}", _0)]
    BackupFormat(String),
}
