use std::{any::Any, sync::Arc};

use bincode::{Decode, Encode};

use crate::{
    event_buffer::{DecodedEventBuffer, RawEvent, RawEventBuffer},
    producer::EventDecoder,
};

#[derive(Encode, Decode, Debug, Clone)]
pub struct Consumer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub event_buffer: Arc<DecodedEventBuffer<T>>,
    pub raw_event_buffer: Arc<RawEventBuffer>,
}

pub trait ConsumerOps<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn consume(&self, event: T, decoder: Arc<dyn EventDecoder<T> + Send + Sync>);
}

impl<T> ConsumerOps<T> for Consumer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn consume(&self, event: T, decoder: Arc<dyn EventDecoder<T> + Send + Sync>) {
        todo!()
    }
}
