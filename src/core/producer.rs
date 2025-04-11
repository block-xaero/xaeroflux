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
        bincode::decode_from_slice(raw_buffer, bincode::config::standard())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            .map(|(event, _)| event)
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
                match decoder.decode(&bytes.data) {
                    Ok(e) => {
                        let r = event_buffer.tx.send(e);
                        match r {
                            Ok(_) => {
                                println!("Event sent successfully");
                            }
                            Err(e) => {
                                println!("Failed to send event: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to decode event: {}", e);
                    }
                }
            }
        });
    }
}
