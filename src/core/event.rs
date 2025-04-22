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
impl Default for EventType {
    fn default() -> Self {
        EventType::ApplicationEvent(0)
    }
}
/// A small list of built‑in system event kinds.
#[derive(Debug, Clone, Encode, Decode, Copy, PartialEq, Eq, Hash, Default)]
pub enum SystemEventKind {
    #[default]
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
#[derive(Debug, Clone, Encode, Decode, Default)]
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
    use bincode::config::{self, Configuration};

    use super::*;
    use crate::core::initialize;

    #[test]
    pub fn test_event_system_type() {
        let event = EventType::from_u8(6);
        assert_eq!(event, EventType::SystemEvent(SystemEventKind::Restart));
    }

    #[test]
    pub fn test_event_application_type() {
        let event = EventType::from_u8(0);
        assert_eq!(event, EventType::ApplicationEvent(0));
    }

    // serialize, deserialize code
    #[test]
    pub fn test_basic_serde() {
        initialize();
        let e = Event::<String>::new("test".to_string(), 0);
        let cfg: Configuration<config::LittleEndian, config::Fixint> =
            bincode::config::standard().with_fixed_int_encoding();
        let encoded: Vec<u8> = bincode::encode_to_vec(&e, cfg).expect("failed to encode event");
        println!("Encoded ({} bytes): {:02x?}", encoded.len(), &encoded);
        let mut offset = 0;

        // 3a) enum discriminant (u32)
        let (tag, len) = bincode::decode_from_slice::<
            u32,
            Configuration<config::LittleEndian, config::Fixint>,
        >(&encoded[offset..], cfg)
        .expect("failed to decode enum discriminant");
        println!(
            "discriminant:   offset {} len {} value {}",
            offset, len, tag
        );
        offset += len;
        println!(
            "discriminant:   offset {} len {} value {}",
            offset, len, tag
        );

        let (payload, len) = bincode::decode_from_slice::<
            u8,
            Configuration<config::LittleEndian, config::Fixint>,
        >(&encoded[offset..], cfg)
        .expect("failed to decode event");
        println!(
            "discriminant:   offset {} len {} value {}",
            offset, len, payload
        );
        offset += len;
        println!(
            ">>> discriminant:   offset {} len {} value {}",
            offset, len, payload
        );
        let decoded: (Event<String>, usize) =
            bincode::decode_from_slice(&encoded, cfg).expect("failed to decode event");
        assert_eq!(e.event_type, decoded.0.event_type);
    }
}
