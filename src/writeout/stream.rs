use super::{RawChunk, WriteOut};
use crate::{CHUNKSIZE, ZERO_CHUNK};

use crossbeam::channel::{Receiver, Sender};
use failure::Fallible;
use std::collections::BinaryHeap;
use std::fmt;
use std::io::Write;

pub struct Stream<W: ?Sized + Write> {
    out: Box<W>,
    progress: Option<Sender<u32>>,
}

impl<W: Write + Send + Sync> Stream<W> {
    pub fn new(out: W) -> Self {
        Self {
            out: Box::new(out),
            progress: None,
        }
    }

    fn write(&mut self, chunk: &RawChunk) -> Fallible<()> {
        self.out.write_all(match &chunk.data {
            Some(d) => &d,
            None => &ZERO_CHUNK,
        })?;
        if let Some(ref p) = self.progress {
            p.send(CHUNKSIZE)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
struct Waiting {
    prio: isize,
    chunk: RawChunk,
}

#[derive(Debug, Default)]
struct Queue(BinaryHeap<Waiting>);

impl Queue {
    fn new() -> Self {
        Queue(BinaryHeap::new())
    }

    fn put(&mut self, chunk: RawChunk) {
        self.0.push(Waiting {
            prio: -(chunk.seq as isize),
            chunk,
        })
    }

    fn get(&mut self, exp_seq: usize) -> Option<RawChunk> {
        if let Some(e) = self.0.peek() {
            if e.prio == -(exp_seq as isize) {
                return Some(self.0.pop().unwrap().chunk);
            }
        }
        None
    }
}

impl<W: Write + Send + Sync> WriteOut for Stream<W> {
    fn configure(&mut self, _size: u64, _threads: u8, progress: Sender<u32>) {
        self.progress = Some(progress);
    }

    fn receive(mut self, chunks: Receiver<RawChunk>) -> Fallible<()> {
        let mut queue = Queue::new();
        let mut exp_seq = 0;
        for chunk in chunks {
            if chunk.seq == exp_seq {
                self.write(&chunk)?;
                exp_seq += 1;
            } else {
                queue.put(chunk);
            }
            while let Some(c) = queue.get(exp_seq) {
                self.write(&c)?;
                exp_seq += 1;
            }
        }
        Ok(())
    }

    fn name(&self) -> String {
        "stdout".to_owned()
    }
}

impl<W: Write + Send + Sync> fmt::Debug for Stream<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<Stream>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;
    use lazy_static::lazy_static;

    const CS: usize = CHUNKSIZE as usize;

    lazy_static! {
        static ref CHUNKS: Vec<Vec<u8>> = vec![vec![0; CS], vec![1; CS], vec![2; CS], vec![3; CS]];
    }

    #[test]
    fn reorder() -> Fallible<()> {
        let mut buf = Vec::with_capacity(4 * CS);
        let (raw, raw_rx) = unbounded();
        for &i in &[1, 3, 0, 2] {
            raw.send(RawChunk {
                seq: i,
                data: Some(CHUNKS[i].to_vec()),
            })?
        }
        drop(raw);
        let s = Stream::new(&mut buf);
        s.receive(raw_rx)?;
        assert_eq!(
            (0..4).map(|i| buf[i * CS]).collect::<Vec<_>>(),
            &[0, 1, 2, 3]
        );
        Ok(())
    }
}
