use backy_extract::CHUNKSZ_LOG;
use std::error::Error;
use smallstr::SmallString;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use structopt::StructOpt;

type RevID = SmallString<[u8; 32]>;

type Revisions = HashMap<RevID, RevCache>;

struct Page([u8; 1<<CHUNKSZ_LOG]);

impl Default for Page {
    fn default() -> Self {
        Self([0; 1<<CHUNKSZ_LOG])
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Page[...]")
    }
}

#[derive(Debug, Default)]
struct RevCache {
    open_seq: Option<usize>,
    open: Page,
    dirty: HashMap<usize, Page>,
}

#[derive(Debug, StructOpt)]
#[structopt(about="Exports all backup images in a FUSE filesystem")]
struct App {
    /// Backy base directory [example: /srv/backy]
    #[structopt(short, long, default_value=".")]
    basedir: PathBuf,
    /// Where to mount the FUSE filesystem [example: /mnt/backy-fuse]
    mountpoint: PathBuf,
    #[structopt(skip)]
    revisions: Revisions,
}

impl App {
    fn read_revs(&mut self) {
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut app = App::from_args();
    app.read_revs();
    println!("{:?}", app);
    Ok(())
}
