use crate::{Chunk, Data, WriteOutBuilder, WriteOut, CHUNKSZ_LOG, ZERO_CHUNK};

use crossbeam::channel::{Receiver, Sender};
use failure::Fallible;
use std::collections::BinaryHeap;
use std::fmt;
use std::io::Write;
use std::rc::Rc;

/// Streaming restore target, i.e. write to stdout.
///
/// The incoming chunk stream is assembled into sequence order in memory. Chunks are
/// written out eagerly to keep memory usage to a minimum.
pub struct Stream<W: ?Sized + Write> {
    out: Box<W>,
}

// Stream is so simple that it can easily implement WriteOutBuilder for itself.
impl<W: Write + Send + Sync> WriteOutBuilder for Stream<W> {
    type Impl = Stream<W>;

    // Does not really do anything except fulfilling the trait requirement.
    fn build(self, _size: u64, _threads: u8) -> Self::Impl {
        self
    }
}

impl<W: Write + Send + Sync> Stream<W> {
    pub fn new(out: W) -> Self {
        Self { out: Box::new(out) }
    }

    fn write(&mut self, data: &Data, prog: &Sender<usize>) -> Fallible<()> {
        self.out.write_all(match data {
            Data::Some(d) => d,
            Data::Zero => &ZERO_CHUNK,
        })?;
        prog.send(1 << CHUNKSZ_LOG)?;
        Ok(())
    }
}

impl<W: Write + Send + Sync> WriteOut for Stream<W> {
    fn receive(mut self, chunks: Receiver<Chunk>, progress: Sender<usize>) -> Fallible<()> {
        let mut queue = Queue::new();
        let mut expect_seq = 0;
        for chunk in chunks {
            let data = Rc::new(chunk.data);
            chunk
                .seqs
                .into_iter()
                .for_each(|seq| queue.put(seq, Rc::clone(&data)));
            while let Some(d) = queue.get(expect_seq) {
                self.write(&d, &progress)?;
                expect_seq += 1;
            }
        }
        assert!(queue.is_empty());
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

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
struct WaitingChunk {
    prio: isize,
    data: Rc<Data>,
}

// Sorts incoming chunks back into order. Chunks may arrive out of order because the Extractor
// reads them in parallel.
#[derive(Debug, Default)]
struct Queue(BinaryHeap<WaitingChunk>);

impl Queue {
    fn new() -> Self {
        Queue(BinaryHeap::new())
    }

    fn put(&mut self, seq: usize, data: Rc<Data>) {
        self.0.push(WaitingChunk {
            prio: -(seq as isize),
            data,
        })
    }

    fn get(&mut self, expect_seq: usize) -> Option<Rc<Data>> {
        if let Some(e) = self.0.peek() {
            if e.prio == -(expect_seq as isize) {
                return Some(self.0.pop().unwrap().data);
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;
    use lazy_static::lazy_static;
    use smallvec::smallvec;

    const CS: usize = 1 << CHUNKSZ_LOG;

    lazy_static! {
        static ref CHUNKS: Vec<Vec<u8>> = vec![vec![0; CS], vec![1; CS], vec![2; CS], vec![3; CS]];
    }

    #[test]
    fn reorder() -> Fallible<()> {
        let mut buf = Vec::with_capacity(4 * CS);
        let (raw, raw_rx) = unbounded();
        for &i in &[1, 3, 0, 2] {
            raw.send(Chunk {
                seqs: smallvec![i],
                data: Data::Some(CHUNKS[i].to_vec()),
            })?
        }
        drop(raw);
        let s = Stream::new(&mut buf);
        let (p_tx, _p_rx) = unbounded();
        s.receive(raw_rx, p_tx)?;
        assert_eq!(
            (0..4).map(|i| buf[i * CS]).collect::<Vec<_>>(),
            &[0, 1, 2, 3]
        );
        Ok(())
    }
}
