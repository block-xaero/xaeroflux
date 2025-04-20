use bincode::{Decode, Encode};

use super::{CONF, XaeroData};

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventType {
    ApplicationEvent(u8),
    SystemEvent(SystemEventKind),
    NetworkEvent(u8),
    StorageEvent(u8),
}
/// A small list of built‑in system event kinds.
#[derive(Debug, Clone, Encode, Decode, Copy, PartialEq, Eq, Hash)]
pub enum SystemEventKind {
    Start,
    Stop,
    Pause,
    Resume,
    Shutdown,
    Restart,
}

impl EventType {
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => EventType::ApplicationEvent(value),
            1 => EventType::SystemEvent(SystemEventKind::Start),
            2 => EventType::SystemEvent(SystemEventKind::Stop),
            3 => EventType::SystemEvent(SystemEventKind::Pause),
            4 => EventType::SystemEvent(SystemEventKind::Resume),
            5 => EventType::SystemEvent(SystemEventKind::Shutdown),
            6 => EventType::SystemEvent(SystemEventKind::Restart),
            7 => EventType::NetworkEvent(value),
            8 => EventType::StorageEvent(value),
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
