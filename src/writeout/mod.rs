mod compat;
mod randomaccess;
mod stream;

pub use self::randomaccess::RandomAccess;
pub use self::stream::Stream;

use super::Chunk;
use crossbeam::channel::{Receiver, Sender};
use failure::Fallible;
use std::fmt::Debug;

/// WriteOut factory.
///
/// We use the factory approach to have only the minimum of parameters to the
/// user-facing constructor. Further parameters from the Exctractor are supplied internally by
/// invoking `build` to get the final WriteOut object.
pub trait WriteOutBuilder {
    type Impl: WriteOut + Sync + Send;
    fn build(self, total_size: u64, threads: u8) -> Self::Impl;
}

/// Abstracts over restore backends.
pub trait WriteOut: Debug {
    fn receive(self, chunks: Receiver<Chunk>, progress: Sender<usize>) -> Fallible<()>;
    fn name(&self) -> String;
}
