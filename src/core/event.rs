use std::any::Any;

use bincode::{Decode, Encode};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Event<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub data: T,
    pub timestamp: u64,
}

impl<T> Event<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub fn new(data: T, timestamp: u64) -> Self {
        Event { data, timestamp }
    }
}
