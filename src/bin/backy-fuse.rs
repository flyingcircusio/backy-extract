#[macro_use]
extern crate log;

use backy_extract::randomaccess::Revisions;
use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, Request,
};
use libc::ENOENT;
use smallstr::SmallString;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::time::SystemTime;
use structopt::StructOpt;

#[derive(Debug, Clone, Default)]
struct Revisions {
    dir: PathBuf,
    cache: HashMap<RevID, Rev>,
}

impl Revisions {
    fn init<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        if !dir.join("chunks").exists() {
            return Err(Error::InitIncomplete {
                dir: dir.to_owned(),
            }
            .into());
        }
        let mut r = Self {
            dir: dir.to_owned(),
            cache: HashMap::new(),
        };
        Ok(r)
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Exports all backup images in a FUSE filesystem")]
struct App {
    /// Backy base directory [example: /srv/backy]
    #[structopt(short, long, default_value = ".")]
    basedir: PathBuf,
    /// Where to mount the FUSE filesystem [example: /mnt/backy-fuse]
    mountpoint: PathBuf,
}

impl App {
    fn init_revs(&mut self) {}
}

fn run(app: App) -> Result<(), Box<dyn Error>> {
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let app = App::from_args();
    let revs = Revisions::init(&app.basedir)?;
    println!("{:?}", app);
    run(app)
}
