use std::any::Any;

use crate::event::Event;

pub struct DecodedEventBuffer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub tx: crossbeam::channel::Sender<Event<T>>,
    pub rx: crossbeam::channel::Receiver<Event<T>>,
}
impl<T> DecodedEventBuffer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub fn new() -> Self {
        let (tx, rx) = crossbeam::channel::bounded(100);
        DecodedEventBuffer { tx, rx }
    }
}

pub struct RawEventBuffer {
    pub tx: crossbeam::channel::Sender<RawEvent>,
    pub rx: crossbeam::channel::Receiver<RawEvent>,
}
impl RawEventBuffer {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam::channel::bounded(100);
        RawEventBuffer { tx, rx }
    }
}
pub struct RawEvent {
    pub data: Vec<u8>,
}
