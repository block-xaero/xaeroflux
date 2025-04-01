use std::any::Any;

use bincode::{Decode, Encode};

use crate::event::Event;

pub struct DecodedEventBuffer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub tx: crossbeam::channel::Sender<Event<T>>,
    pub rx: crossbeam::channel::Receiver<Event<T>>,
}
impl<T> Default for DecodedEventBuffer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
 {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DecodedEventBuffer<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub fn new() -> Self {
        let (tx, rx): (
            crossbeam::channel::Sender<Event<T>>,
            crossbeam::channel::Receiver<Event<T>>,
        ) = crossbeam::channel::bounded(100);
        DecodedEventBuffer { tx, rx }
    }
}

pub struct RawEventBuffer {
    pub tx: crossbeam::channel::Sender<RawEvent>,
    pub rx: crossbeam::channel::Receiver<RawEvent>,
}

impl Default for RawEventBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl RawEventBuffer {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam::channel::bounded(100);
        RawEventBuffer { tx, rx }
    }
}

#[derive(Encode, Decode, Debug, Clone)]
/// RawEvent is a wrapper around Vec<u8> to represent raw event data
pub struct RawEvent {
    pub data: Vec<u8>,
}
