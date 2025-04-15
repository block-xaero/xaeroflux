use bincode::{Decode, Encode};

use super::XaeroData;

pub enum EventType {
    ApplicationEvent(u8),
    SystemEvent(u8),
    NetworkEvent(u8),
    StorageEvent(u8),
}
impl EventType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => EventType::ApplicationEvent(value),
            1 => EventType::SystemEvent(value),
            2 => EventType::NetworkEvent(value),
            3 => EventType::StorageEvent(value),
            _ => panic!("Invalid event type"),
        }
    }
}
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
