use std::sync::Arc;

use super::XaeroData;
use crate::core::{
    event_buffer::{DecodedEventBuffer, RawEventBuffer},
    producer::EventDecoder,
};

#[derive(Debug, Clone)]
pub struct Consumer<T>
where
    T: XaeroData,
{
    pub event_buffer: Arc<DecodedEventBuffer<T>>,
    pub raw_event_buffer: Arc<RawEventBuffer>,
}

pub trait ConsumerOps<T>
where
    T: XaeroData,
{
    fn consume(&self, event: T, decoder: Arc<dyn EventDecoder<T> + Send + Sync>);
}

impl<T> ConsumerOps<T> for Consumer<T>
where
    T: XaeroData,
{
    fn consume(&self, _event: T, _decoder: Arc<dyn EventDecoder<T> + Send + Sync>) {
        todo!()
    }
}
