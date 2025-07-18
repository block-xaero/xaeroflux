use std::time::Duration;

use crossbeam::{channel::tick, select};
use rusted_ring_new::{PooledEvent, RingPipe};

#[derive(Debug, Copy, Clone)]
pub struct SubjectHash(pub [u8; 32]);

pub struct Subject<const TSHIRT: usize, const CAPACITY: usize> {
    pub subject_hash: SubjectHash,
    pub group: Option<[u8; 32]>,
    pub workspace: Option<[u8; 32]>,
    pub object: Option<[u8; 32]>,
    pub pipe: RingPipe<TSHIRT, CAPACITY>,
}

impl<const TSHIRT: usize, const CAPACITY: usize> Subject<TSHIRT, CAPACITY> {
    // Simple event processing - no fancy operators
    pub fn process_events<F>(&mut self, mut processor: F)
    where
        F: FnMut(PooledEvent<TSHIRT>) -> Option<PooledEvent<TSHIRT>>,
    {
        for event in self.pipe.sink.reader.by_ref() {
            if let Some(processed) = processor(event) {
                self.pipe.source.writer.add(processed);
            }
        }
    }

    // Batch processing with time window
    pub fn process_batched<F>(&mut self, window: Duration, mut batch_processor: F)
    where
        F: FnMut(Vec<PooledEvent<TSHIRT>>) -> Vec<PooledEvent<TSHIRT>>,
    {
        let ticker = tick(window);
        let mut buffer = Vec::new();

        loop {
            select! {
                recv(ticker) -> _ => {
                    if !buffer.is_empty() {
                        let processed = batch_processor(buffer.clone());
                        for event in processed {
                            self.pipe.source.writer.add(event);
                        }
                        buffer.clear();
                    }
                },
                default => {
                    for event in self.pipe.sink.reader.by_ref() {
                        buffer.push(event);
                    }
                }
            }
        }
    }
}
