use crate::event::Event;
use crate::event_buffer::DecodedEventBuffer;
use crate::event_buffer::RawEventBuffer;
use bincode;
use bincode::Decode;
use crossbeam::select;
use std::any::Any;
use std::sync::Arc;
use std::thread;

const CAPACITY: usize = 10;
const MULTIPLIER: usize = 4;

// Event decoder parses the raw buffer and extracts events
pub trait EventDecoder<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn decode(&self, raw_buffer: &Vec<u8>) -> Result<Event<T>, Box<dyn std::error::Error>>;
}
pub struct RawEventDecoder<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub result: Result<Event<T>, Box<dyn std::error::Error>>,
}
impl<T> EventDecoder<T> for RawEventDecoder<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn decode(&self, raw_buffer: &Vec<u8>) -> Result<Event<T>, Box<dyn std::error::Error>> {
        let r: (Event<T>, usize) =
            bincode::decode_from_slice(&raw_buffer[..], bincode::config::standard()).unwrap();
        Ok(r.0)
    }
}
pub struct Producer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub event_buffer: Arc<DecodedEventBuffer<T>>,
    pub raw_event_buffer: Arc<RawEventBuffer>,
    pub decoder: Arc<dyn EventDecoder<T> + Send + Sync>,
}

trait ProducerOps<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn spin_wait(&self);
    fn produce(&self, t: T);
}
impl<T> Producer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
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
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn spin_wait(&self) {
        let raw_event_buffer = self.raw_event_buffer.clone();
        let event_buffer = self.event_buffer.clone();
        let decoder = self.decoder.clone();
        thread::spawn(move || {
            select! {
                recv(raw_event_buffer.rx) -> raw_event =>{
                    let bytes = raw_event.unwrap();
                    let e = decoder.decode(&bytes.data).unwrap();
                    event_buffer.tx.send(e).unwrap();
                }
            }
        });
    }

    fn produce(&self, t: T) {
        todo!()
    }
}
