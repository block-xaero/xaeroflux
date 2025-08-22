//! Core event definitions for xaeroflux-core.
//!
//! This module provides:
//! - `Event<T>`: generic event envelope with type, version, timestamp, and payload.
//! - `EventType` and `SystemEventKind`: enums categorizing different event classes.
//! - Serialization support via `rkyv` for zero-copy archiving.
//! - `EVENT_HEADER` magic and `META_BASE` offset for metadata event encoding.

use std::fmt::Debug;

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};

pub use crate::{pool::XaeroEvent, vector_clock::VectorClock};

/// Magic bytes prefix for event headers in paged segments.
/// Used to identify and slice raw event bytes from storage pages.
pub static EVENT_HEADER: &[u8; 4] = b"XAER";
/// Base value offset for encoding `MetaEvent` variants in the event type byte.
pub const META_BASE: u8 = 128;

// CRDT Application Event Constants (Base 30+)
pub const CRDT_BASE: u8 = 30;

// OR-Set CRDT Events
pub const CRDT_SET_ADD: u8 = 30;
pub const CRDT_SET_REMOVE: u8 = 31;
pub const CRDT_SET_STATE: u8 = 32;

// Counter CRDT Events
pub const CRDT_COUNTER_INCREMENT: u8 = 33;
pub const CRDT_COUNTER_DECREMENT: u8 = 34;
pub const CRDT_COUNTER_STATE: u8 = 35;

// RGA Text CRDT Events
pub const CRDT_TEXT_INSERT: u8 = 36;
pub const CRDT_TEXT_DELETE: u8 = 37;
pub const CRDT_TEXT_STATE: u8 = 38;

// Tree CRDT Events
pub const CRDT_TREE_ADD_NODE: u8 = 39;
pub const CRDT_TREE_REMOVE_NODE: u8 = 40;
pub const CRDT_TREE_MOVE_NODE: u8 = 41;
pub const CRDT_TREE_STATE: u8 = 42;

// LWW Register CRDT Events
pub const CRDT_REGISTER_WRITE: u8 = 43;
pub const CRDT_REGISTER_STATE: u8 = 44;

// Reserve 45-59 for future CRDT types

// Network Event Constants (Base 60+)
pub const NETWORK_BASE: u8 = 60;

// XSP (XaeroProtocol) Sync Events
pub const MMR_PEAKS_EXCHANGE: u8 = 60;
pub const MMR_DIFF: u8 = 61;
pub const EVENTS_REQUEST: u8 = 62;
pub const EVENTS_RESPONSE: u8 = 63;

// Additional Network Events
pub const PEER_CONNECTED: u8 = 64;
pub const PEER_DISCONNECTED: u8 = 65;
pub const FILE_TRANSFER_START: u8 = 66;
pub const FILE_TRANSFER_COMPLETE: u8 = 67;
pub const AUDIO_CALL_START: u8 = 68;
pub const AUDIO_CALL_END: u8 = 69;
pub const VIDEO_CALL_START: u8 = 70;
pub const VIDEO_CALL_END: u8 = 71;

// Reserve 72-127 for future network events

pub const PIN_FLAG: u32 = 0x8000_0000; // High bit = pinned

// Helper functions
pub fn is_pinned_event(event_type: u32) -> bool {
    (event_type & PIN_FLAG) != 0
}

pub fn make_pinned(event_type: u32) -> u32 {
    event_type | PIN_FLAG
}

pub fn get_base_event_type(event_type: u32) -> u32 {
    event_type & !PIN_FLAG // Remove pin flag to get original type
}

#[repr(C)]
/// Discriminant for different categories of events.
///
/// Encodes application, system, metadata, and network events.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum EventType {
    /// Application-level event carrying a user-defined subtype.
    ApplicationEvent(u8),
    /// Built-in system control events (start/stop/pause/etc.).
    SystemEvent(SystemEventKind),
    /// Metadata events offset by `META_BASE` for internal operations.
    MetaEvent(u8),
    /// Networking-level events, e.g., peer discovery or connection state.
    NetworkEvent(u8),
}

impl Default for EventType {
    fn default() -> Self {
        EventType::ApplicationEvent(0)
    }
}

#[repr(C)]
/// Specific kinds of system events controlling actor lifecycle and system operations.
///
/// Includes basic lifecycle events for actor control.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum SystemEventKind {
    /// Indicates the system or actor should start processing.
    Start,
    /// Indicates the system or actor should stop processing.
    Stop,
    /// Indicates the system or actor should pause processing temporarily.
    Pause,
    /// Indicates the system or actor should resume processing after a pause.
    Resume,
    /// Indicates the system or actor should shutdown gracefully.
    Shutdown,
    /// Indicates the system or actor should restart.
    Restart,
}

impl EventType {
    pub fn from_u8(value: u8) -> Self {
        // If it's ≥ META_BASE, interpret as MetaEvent(inner)
        if value >= META_BASE {
            return EventType::MetaEvent(value - META_BASE);
        }

        match value {
            0 => EventType::ApplicationEvent(0),

            // SystemEventKind variants (map numbers → enum)
            1 => EventType::SystemEvent(SystemEventKind::Start),
            2 => EventType::SystemEvent(SystemEventKind::Stop),
            3 => EventType::SystemEvent(SystemEventKind::Pause),
            4 => EventType::SystemEvent(SystemEventKind::Resume),
            5 => EventType::SystemEvent(SystemEventKind::Shutdown),
            6 => EventType::SystemEvent(SystemEventKind::Restart),

            // Reserve 7-29 for future system events

            // CRDT Application Events (30-44)
            CRDT_SET_ADD => EventType::ApplicationEvent(CRDT_SET_ADD),
            CRDT_SET_REMOVE => EventType::ApplicationEvent(CRDT_SET_REMOVE),
            CRDT_SET_STATE => EventType::ApplicationEvent(CRDT_SET_STATE),
            CRDT_COUNTER_INCREMENT => EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT),
            CRDT_COUNTER_DECREMENT => EventType::ApplicationEvent(CRDT_COUNTER_DECREMENT),
            CRDT_COUNTER_STATE => EventType::ApplicationEvent(CRDT_COUNTER_STATE),
            CRDT_TEXT_INSERT => EventType::ApplicationEvent(CRDT_TEXT_INSERT),
            CRDT_TEXT_DELETE => EventType::ApplicationEvent(CRDT_TEXT_DELETE),
            CRDT_TEXT_STATE => EventType::ApplicationEvent(CRDT_TEXT_STATE),
            CRDT_TREE_ADD_NODE => EventType::ApplicationEvent(CRDT_TREE_ADD_NODE),
            CRDT_TREE_REMOVE_NODE => EventType::ApplicationEvent(CRDT_TREE_REMOVE_NODE),
            CRDT_TREE_MOVE_NODE => EventType::ApplicationEvent(CRDT_TREE_MOVE_NODE),
            CRDT_TREE_STATE => EventType::ApplicationEvent(CRDT_TREE_STATE),
            CRDT_REGISTER_WRITE => EventType::ApplicationEvent(CRDT_REGISTER_WRITE),
            CRDT_REGISTER_STATE => EventType::ApplicationEvent(CRDT_REGISTER_STATE),

            // Reserve 45-59 for future CRDT types

            // Network events (60-127)
            v if (NETWORK_BASE..META_BASE).contains(&v) => EventType::NetworkEvent(v),

            _ => panic!("Invalid event type: {}", value),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            EventType::ApplicationEvent(v) => *v,

            // System events
            EventType::SystemEvent(SystemEventKind::Start) => 1,
            EventType::SystemEvent(SystemEventKind::Stop) => 2,
            EventType::SystemEvent(SystemEventKind::Pause) => 3,
            EventType::SystemEvent(SystemEventKind::Resume) => 4,
            EventType::SystemEvent(SystemEventKind::Shutdown) => 5,
            EventType::SystemEvent(SystemEventKind::Restart) => 6,

            EventType::NetworkEvent(v) => *v,
            EventType::MetaEvent(v) => META_BASE + *v,
        }
    }

    /// Check if this is an XSP sync event
    pub fn is_xsp_sync_event(&self) -> bool {
        matches!(self, EventType::NetworkEvent(v) if (MMR_PEAKS_EXCHANGE..=EVENTS_RESPONSE).contains(v))
    }

    /// Check if this is a media stream event
    pub fn is_media_event(&self) -> bool {
        matches!(self, EventType::NetworkEvent(v) if (AUDIO_CALL_START..=VIDEO_CALL_END).contains(v))
    }

    /// Check if this is a file transfer event
    pub fn is_file_transfer_event(&self) -> bool {
        matches!(self, EventType::NetworkEvent(v) if (FILE_TRANSFER_START..=FILE_TRANSFER_COMPLETE).contains(v))
    }
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemErrorCode {
    DbWrite = 1,
    SegmentRoll = 2,
    PageFlush = 3,
    MmrAppend = 4,
    SecondaryIndex = 5,
    Unknown = 0xFFFF,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ScanWindow {
    pub start: u64,
    pub end: u64,
}

unsafe impl Pod for ScanWindow {}
unsafe impl Zeroable for ScanWindow {}

// Helper functions for XSP events
impl EventType {
    /// Create MMR peaks exchange event
    pub fn mmr_peaks_exchange() -> Self {
        EventType::NetworkEvent(MMR_PEAKS_EXCHANGE)
    }

    /// Create MMR diff event
    pub fn mmr_diff() -> Self {
        EventType::NetworkEvent(MMR_DIFF)
    }

    /// Create events request event
    pub fn events_request() -> Self {
        EventType::NetworkEvent(EVENTS_REQUEST)
    }

    /// Create events response event
    pub fn events_response() -> Self {
        EventType::NetworkEvent(EVENTS_RESPONSE)
    }

    /// Create peer connected event
    pub fn peer_connected() -> Self {
        EventType::NetworkEvent(PEER_CONNECTED)
    }

    /// Create peer disconnected event
    pub fn peer_disconnected() -> Self {
        EventType::NetworkEvent(PEER_DISCONNECTED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xsp_event_constants() {
        assert_eq!(MMR_PEAKS_EXCHANGE, 60);
        assert_eq!(MMR_DIFF, 61);
        assert_eq!(EVENTS_REQUEST, 62);
        assert_eq!(EVENTS_RESPONSE, 63);
    }

    #[test]
    fn test_event_type_conversions() {
        let event = EventType::mmr_peaks_exchange();
        assert_eq!(event.to_u8(), MMR_PEAKS_EXCHANGE);
        assert_eq!(EventType::from_u8(MMR_PEAKS_EXCHANGE), event);
    }

    #[test]
    fn test_xsp_sync_detection() {
        assert!(EventType::mmr_peaks_exchange().is_xsp_sync_event());
        assert!(EventType::mmr_diff().is_xsp_sync_event());
        assert!(EventType::events_request().is_xsp_sync_event());
        assert!(EventType::events_response().is_xsp_sync_event());

        assert!(!EventType::peer_connected().is_xsp_sync_event());
        assert!(!EventType::ApplicationEvent(CRDT_SET_ADD).is_xsp_sync_event());
    }

    #[test]
    fn test_media_event_detection() {
        let audio_start = EventType::NetworkEvent(AUDIO_CALL_START);
        let video_end = EventType::NetworkEvent(VIDEO_CALL_END);

        assert!(audio_start.is_media_event());
        assert!(video_end.is_media_event());
        assert!(!EventType::mmr_peaks_exchange().is_media_event());
    }

    #[test]
    fn test_file_transfer_detection() {
        let file_start = EventType::NetworkEvent(FILE_TRANSFER_START);
        let file_complete = EventType::NetworkEvent(FILE_TRANSFER_COMPLETE);

        assert!(file_start.is_file_transfer_event());
        assert!(file_complete.is_file_transfer_event());
        assert!(!EventType::mmr_peaks_exchange().is_file_transfer_event());
    }
}
