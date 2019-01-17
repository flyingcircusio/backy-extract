//! CLI wrapper for backy-extract.
//!
//! This code uses the public crate interface to provide a handy shell command.

use atty::{self, Stream::Stdout};
use backy_extract::{Extractor, RandomAccess, Stream};
use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version, Arg};
use console::style;
use failure::{ensure, Fallible, ResultExt};
use std::ffi::OsStr;
use std::io;

fn run() -> Fallible<()> {
    let m = app_from_crate!()
        .arg(
            Arg::with_name("THREADS")
                .value_name("N")
                .long("threads")
                .short("t")
                .help("Uses N parallel threads for decompression [default: auto]"),
        )
        .arg(
            Arg::with_name("SPARSE")
                .long("sparse")
                .short("s")
                .help("Skips over contiguous regions of NUL bytes [default]")
                .display_order(500),
        )
        .arg(
            Arg::with_name("NOSPARSE")
                .long("no-sparse")
                .short("S")
                .help("Writes out contiguous regions of NUL bytes")
                .conflicts_with("SPARSE")
                .display_order(501),
        )
        .arg(
            Arg::with_name("QUIET")
                .long("quiet")
                .short("q")
                .help("Suppresses progress indication")
                .conflicts_with("SPARSE"),
        )
        .arg(
            Arg::with_name("REVISION")
                .help("Backy backup revision file [e.g., `2hQmTeMjRaFG9jonuXeCnR' or `last']")
                .required(true),
        )
        .arg(Arg::with_name("OUTPUT").help("Output file or block device (or stdout if absent)"))
        .get_matches();
    let mut e = Extractor::init(m.value_of_os("REVISION").unwrap())?;
    if let Some(t) = m.value_of("THREADS") {
        e.threads(t.parse::<u8>().context("Invalid number of threads")?);
    }
    if !m.is_present("QUIET") {
        e.progress(true);
    }
    let output = m.value_of_os("OUTPUT").unwrap_or_else(|| OsStr::new("-"));
    if output.to_string_lossy() == "-" {
        ensure!(
            atty::isnt(Stdout),
            "cowardly refusing to restore to the terminal"
        );
        e.extract(Stream::new(io::stdout()))
    } else {
        let sparse = m.is_present("SPARSE") || !m.is_present("NOSPARSE");
        e.extract(RandomAccess::new(output, sparse))
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{}  {}", style("Error:").red().bold(), e);
        for cause in e.iter_causes() {
            eprintln!("{} {}", style("Detail:").yellow().bold(), cause);
        }
        std::process::exit(1);
    }
}
