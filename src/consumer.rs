use std::any::Any;

use bincode::{Decode, Encode};

use crate::event_buffer::RawEvent;

#[derive(Encode, Decode, Debug, Clone)]
pub struct Consumer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub event_buffer: Arc<DecodedEventBuffer<T>>,
    pub raw_event_buffer: Arc<RawEventBuffer>,
    pub decoder: Arc<dyn EventDecoder<T> + Send + Sync>,
}

pub trait ConsumerOps<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn consume(&self, event: T);
}

impl<T> ConsumerOps<T> for Consumer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn consume(&self, event: T) {
        todo!()
    }
}
