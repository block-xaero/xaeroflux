use std::fmt::Debug;

use rkyv::{Archive, Deserialize, Serialize};

use super::{CONF, XaeroData};

#[repr(C)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum EventType {
    ApplicationEvent(u8),
    SystemEvent(SystemEventKind),
    NetworkEvent(u8),
    StorageEvent(u8),
}
impl Default for EventType {
    fn default() -> Self {
        EventType::ApplicationEvent(0)
    }
}
#[repr(C)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[rkyv(derive(Debug))]
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

    pub fn to_u8(&self) -> u8 {
        match self {
            EventType::ApplicationEvent(value) => *value,
            EventType::SystemEvent(SystemEventKind::Start) => 1,
            EventType::SystemEvent(SystemEventKind::Stop) => 2,
            EventType::SystemEvent(SystemEventKind::Pause) => 3,
            EventType::SystemEvent(SystemEventKind::Resume) => 4,
            EventType::SystemEvent(SystemEventKind::Shutdown) => 5,
            EventType::SystemEvent(SystemEventKind::Restart) => 6,
            EventType::NetworkEvent(value) => *value,
            EventType::StorageEvent(value) => *value,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, Default)]
#[rkyv(
    derive(Debug),
    archive_bounds(T::Archived: Debug),
)]
/// Event is the main data structure for the event system.
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

#[cfg(test)]
mod tests {

    use rkyv::{
        Archived,
        rancor::{Error, Failure},
    };

    use crate::core::{event::Event, initialize};

    #[test]
    pub fn test_basic_serde() {
        initialize();
        let event = crate::core::event::Event::<String>::new("test".to_string(), 1);
        let bytes: rkyv::util::AlignedVec =
            rkyv::to_bytes::<Failure>(&Event::new("hello world".to_string(), 1))
                .expect("failed to serialize");
        let arch_event = rkyv::access::<Archived<Event<String>>, Failure>(&bytes)
            .expect("failed to access archived event");
        tracing::info!("non archived event: {:#?}", event);
        tracing::info!("archived event: {:#?}", arch_event);
        let deserialized_event =
            rkyv::from_bytes::<Event<String>, Error>(&bytes).expect("failed to deserialize");
        assert_eq!(event.event_type, deserialized_event.event_type);
    }
}
