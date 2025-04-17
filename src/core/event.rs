use bincode::{Decode, Encode};

use super::{CONF, XaeroData};

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq, Hash)]
#[repr(u8)]
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
    pub event_type: EventType,
    pub version: u64,
    pub data: T,
    pub ts: u64,
}

impl<T> Event<T>
where
    T: XaeroData,
{
    pub fn new(data: T, e_type: u8) -> Self {
        Event {
            data,
            event_type: EventType::from_u8(e_type),
            version: CONF.get().expect("not initialized").version,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64,
        }
    }
}
