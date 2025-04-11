use bincode::{Decode, Encode};

use super::XaeroData;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Event<T>
where
    T: XaeroData,
{
    pub data: T,
    pub timestamp: u64,
}

impl<T> Event<T>
where
    T: XaeroData,
{
    pub fn new(data: T, timestamp: u64) -> Self {
        Event { data, timestamp }
    }
}
