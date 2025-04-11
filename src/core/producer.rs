use std::{sync::Arc, thread};

use bincode;

use super::XaeroData;
use crate::{
    core::event_buffer::{DecodedEventBuffer, RawEventBuffer},
    event::Event,
};

// Event decoder parses the raw buffer and extracts events
pub trait EventDecoder<T>
where
    T: XaeroData,
{
    fn decode(&self, raw_buffer: &[u8]) -> Result<Event<T>, Box<dyn std::error::Error>>;
}
pub struct RawEventDecoder<T>
where
    T: XaeroData,
{
    pub result: Result<Event<T>, Box<dyn std::error::Error>>,
}
impl<T> EventDecoder<T> for RawEventDecoder<T>
where
    T: XaeroData,
{
    fn decode(&self, raw_buffer: &[u8]) -> Result<Event<T>, Box<dyn std::error::Error>> {
        let r: (Event<T>, usize) =
            bincode::decode_from_slice(raw_buffer, bincode::config::standard()).unwrap();
        Ok(r.0)
    }
}
pub struct Producer<T>
where
    T: XaeroData,
{
    pub event_buffer: Arc<DecodedEventBuffer<T>>,
    pub raw_event_buffer: Arc<RawEventBuffer>,
    pub decoder: Arc<dyn EventDecoder<T> + Send + Sync>,
}
pub trait ProducerOps<T>
where
    T: XaeroData,
{
    fn spin_wait(&self);
}
impl<T> Producer<T>
where
    T: XaeroData,
{
    pub fn new(
        event_buffer: Arc<DecodedEventBuffer<T>>,
        raw_event_buffer: Arc<RawEventBuffer>,
        decoder: Arc<dyn EventDecoder<T> + Send + Sync>,
    ) -> Self {
        Producer {
            raw_event_buffer,
            event_buffer,
            decoder,
        }
    }
}

impl<T> ProducerOps<T> for Producer<T>
where
    T: XaeroData,
{
    fn spin_wait(&self) {
        let raw_event_buffer = self.raw_event_buffer.clone();
        let event_buffer = self.event_buffer.clone();
        let decoder = self.decoder.clone();
        thread::spawn(move || {
            while let Ok(bytes) = raw_event_buffer.rx.recv() {
                if let Ok(e) = decoder.decode(&bytes.data) {
                    event_buffer.tx.send(e).unwrap();
                } else {
                    println!("Failed to decode event");
                }
            }
        });
    }
}
