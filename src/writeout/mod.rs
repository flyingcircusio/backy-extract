mod compat;
mod randomaccess;
mod stream;

pub use self::randomaccess::RandomAccess;
pub use self::stream::Stream;

use super::RawChunk;
use crossbeam::channel::{Receiver, Sender};
use failure::Fallible;
use std::fmt::Debug;

pub trait WriteOut: Debug {
    fn configure(&mut self, total_size: u64, threads: u8, progress: Sender<usize>);
    fn receive(self, chunks: Receiver<RawChunk>) -> Fallible<()>;
    fn name(&self) -> String;
}
