mod randomaccess;
mod stream;

pub use self::randomaccess::RandomAccess;
pub use self::stream::Stream;
use crate::Chunk;

use crossbeam::channel::{Receiver, SendError, Sender};
use std::fmt::Debug;
use std::io;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to open output file `{}'", .0.display())]
    OutputFile(PathBuf, #[source] io::Error),
    #[error("Failed to write chunk #{0}")]
    WriteChunk(usize, #[source] io::Error),
    #[error("Failed to write chunk #{} to `{}'", .0, .1.display())]
    WriteChunkFile(usize, PathBuf, #[source] io::Error),
    #[error("IPC error")]
    ChannelSend(#[from] SendError<usize>),
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// WriteOut factory.
///
/// We use the factory approach to have only the minimum of parameters to the
/// user-facing constructor. Further parameters from the Exctractor are supplied internally by
/// invoking `build` to get the final WriteOut object.
pub trait WriteOutBuilder {
    type Impl: WriteOut + Sync + Send;
    fn build(self, total_size: u64, threads: u8) -> Self::Impl;
}

/// Abstract writeout (restore) plugin.
///
/// A concrete writer is instantiated via `WriteOutBuilder.build()`.
pub trait WriteOut: Debug {
    /// Gets an unordered stream of `Chunk`s which must be written to the restore target according
    /// to the chunks' sequence numbers. Writer must send the number of bytes written to the
    /// `progress` channel to indicate restore progress in real time.
    fn receive(self, chunks: Receiver<Chunk>, progress: Sender<usize>) -> Result<()>;

    /// Short idenfication for user display. Should contain plugin type and file name.
    fn name(&self) -> String;
}
