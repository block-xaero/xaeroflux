//! Core event definitions for xaeroflux-core.
//!
//! This module provides:
//! - `Event<T>`: generic event envelope with type, version, timestamp, and payload.
//! - `EventType` and `SystemEventKind`: enums categorizing different event classes.
//! - Serialization support via `rkyv` for zero-copy archiving.
//! - `EVENT_HEADER` magic and `META_BASE` offset for metadata event encoding.

use std::{cmp::Ordering, fmt::Debug, sync::Arc, time::Duration};

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};
use xaeroid::XaeroID;

use crate::{CONF, XaeroData};

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
pub const NETWORK_BASE: u8 = 60;

#[repr(C)]
/// Discriminant for different categories of events.
///
/// Encodes application, system, metadata, network, and storage events.
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
    /// Storage subsystem events, e.g., segment rollover or compaction.
    StorageEvent(u8),
}

impl Default for EventType {
    fn default() -> Self {
        EventType::ApplicationEvent(0)
    }
}

#[repr(C)]
/// Specific kinds of system events controlling actor lifecycle and system operations.
///
/// Includes basic lifecycle events (start, stop, pause, resume, shutdown, restart, replay),
/// as well as events indicating payload writes, segment rollovers, page flushes,
/// MMR appends, and secondary index operations.
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
    /// Replay control events to rehydrate control bus state.
    ReplayControl,
    /// Replay data events to rehydrate application/data bus state.
    ReplayData,
    /// Indicates an event payload was written to a page.
    PayloadWritten,
    /// Indicates a segment rollover completed successfully.
    SegmentRolledOver,
    /// Indicates a segment rollover failed.
    SegmentRollOverFailed,
    /// Indicates a page was flushed successfully.
    PageFlushed,
    /// Indicates a page flush failed.
    PageFlushFailed,
    /// Indicates a leaf hash was appended to the MMR.
    MmrAppended,
    /// Indicates appending a leaf hash to the MMR failed.
    MmrAppendFailed,
    /// Indicates the secondary index entry was written.
    SecondaryIndexWritten,
    /// Indicates writing the secondary index entry failed.
    SecondaryIndexFailed,
    SubjectCreated,
    WorkspaceCreated,
    ObjectCreated,
}

impl EventType {
    pub fn from_u8(value: u8) -> Self {
        // If it's ≥ META_BASE, interpret as MetaEvent(inner)
        if value >= META_BASE {
            return EventType::MetaEvent(value - META_BASE);
        }

        match value {
            0 => EventType::ApplicationEvent(0),

            // SystemEventKind variants (map numbers → enum) - UNCHANGED
            1 => EventType::SystemEvent(SystemEventKind::Start),
            2 => EventType::SystemEvent(SystemEventKind::Stop),
            3 => EventType::SystemEvent(SystemEventKind::Pause),
            4 => EventType::SystemEvent(SystemEventKind::Resume),
            5 => EventType::SystemEvent(SystemEventKind::Shutdown),
            6 => EventType::SystemEvent(SystemEventKind::Restart),
            7 => EventType::SystemEvent(SystemEventKind::ReplayControl),
            8 => EventType::SystemEvent(SystemEventKind::ReplayData),
            9 => EventType::SystemEvent(SystemEventKind::PayloadWritten),
            10 => EventType::SystemEvent(SystemEventKind::SegmentRolledOver),
            11 => EventType::SystemEvent(SystemEventKind::SegmentRollOverFailed),
            12 => EventType::SystemEvent(SystemEventKind::PageFlushed),
            13 => EventType::SystemEvent(SystemEventKind::PageFlushFailed),
            14 => EventType::SystemEvent(SystemEventKind::MmrAppended),
            15 => EventType::SystemEvent(SystemEventKind::MmrAppendFailed),
            16 => EventType::SystemEvent(SystemEventKind::SecondaryIndexWritten),
            17 => EventType::SystemEvent(SystemEventKind::SecondaryIndexFailed),
            18 => EventType::SystemEvent(SystemEventKind::SubjectCreated),
            19 => EventType::SystemEvent(SystemEventKind::WorkspaceCreated),
            20 => EventType::SystemEvent(SystemEventKind::ObjectCreated),

            // CRDT Application Events (30-59)
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

            // Network/Storage events (60+) - Updated range
            v if (NETWORK_BASE..META_BASE).contains(&v) => EventType::NetworkEvent(v),
            v if (21..CRDT_BASE).contains(&v) => EventType::StorageEvent(v), // 21-29 for storage

            _ => panic!("Invalid event type: {}", value),
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            EventType::ApplicationEvent(v) => *v,

            // System events - UNCHANGED
            EventType::SystemEvent(SystemEventKind::Start) => 1,
            EventType::SystemEvent(SystemEventKind::Stop) => 2,
            EventType::SystemEvent(SystemEventKind::Pause) => 3,
            EventType::SystemEvent(SystemEventKind::Resume) => 4,
            EventType::SystemEvent(SystemEventKind::Shutdown) => 5,
            EventType::SystemEvent(SystemEventKind::Restart) => 6,
            EventType::SystemEvent(SystemEventKind::ReplayControl) => 7,
            EventType::SystemEvent(SystemEventKind::ReplayData) => 8,
            EventType::SystemEvent(SystemEventKind::PayloadWritten) => 9,
            EventType::SystemEvent(SystemEventKind::SegmentRolledOver) => 10,
            EventType::SystemEvent(SystemEventKind::SegmentRollOverFailed) => 11,
            EventType::SystemEvent(SystemEventKind::PageFlushed) => 12,
            EventType::SystemEvent(SystemEventKind::PageFlushFailed) => 13,
            EventType::SystemEvent(SystemEventKind::MmrAppended) => 14,
            EventType::SystemEvent(SystemEventKind::MmrAppendFailed) => 15,
            EventType::SystemEvent(SystemEventKind::SecondaryIndexWritten) => 16,
            EventType::SystemEvent(SystemEventKind::SecondaryIndexFailed) => 17,
            EventType::SystemEvent(SystemEventKind::SubjectCreated) => 18,
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated) => 19,
            EventType::SystemEvent(SystemEventKind::ObjectCreated) => 20,

            EventType::NetworkEvent(v) => *v,
            EventType::StorageEvent(v) => *v,
            EventType::MetaEvent(v) => META_BASE + *v,
        }
    }
}

// Rest of your code stays exactly the same...
#[repr(C)]
/// Generic event wrapper containing payload and metadata.
///
/// Fields:
/// - `event_type`: type/category of the event.
/// - `version`: configuration version when event was created.
/// - `data`: event payload implementing `XaeroData`.
/// - `ts`: UNIX epoch timestamp in milliseconds.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, Default)]
#[rkyv(
    derive(Debug),
    archive_bounds(T::Archived: Debug),
)]
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
    /// Construct a new application event.
    ///
    /// Assigns current configuration version and timestamp automatically.
    ///
    /// # Arguments
    /// - `data`: payload data for the event.
    /// - `e_type`: numeric event type code, converted via `EventType::from_u8`.
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

/// Envelope wrapping an application or system `Event` payload
/// along with an optional Merkle inclusion proof.
#[derive(Debug, Clone, Default)]
pub struct XaeroEvent {
    /// Core event data (e.g., domain event encoded as bytes).
    pub evt: Event<Vec<u8>>,
    /// Optional Merkle proof bytes (e.g., from MMR).
    pub merkle_proof: Option<Vec<u8>>,

    pub author_id: Option<XaeroID>,
    pub latest_ts: Option<u64>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ScanWindow {
    pub start: u64,
    pub end: u64,
}
unsafe impl Pod for ScanWindow {}
unsafe impl Zeroable for ScanWindow {}

#[allow(clippy::type_complexity)]
/// Defines per-event pipeline operations.
#[derive(Clone)]
pub enum Operator {
    // Mode operators
    StreamingMode, // ALWAYS ON, UNLESS IN BatchMode
    // Stream Operators
    Scan(Arc<ScanWindow>),
    /// Transform the event into another event.
    Map(Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>),
    /// Keep only events matching the predicate.
    Filter(Arc<dyn Fn(&XaeroEvent) -> bool + Send + Sync>),
    /// Drop events without a Merkle proof.
    FilterMerkleProofs,

    // Batch Mode operators
    /// Buffer operator (encodes a pipeline of Operators that is added sequentially)
    BatchMode(Duration, Option<usize>, Vec<Operator>),

    /// Causal, temporal ordering for events for example or anything in between.
    Sort(Arc<dyn Fn(&XaeroEvent, &XaeroEvent) -> Ordering + Send + Sync>),
    /// Folds vector of xaero events to a single xaero event.
    Fold(Arc<dyn Fn(XaeroEvent, Vec<XaeroEvent>) -> XaeroEvent + Send + Sync>),
    /// Reduces a vector of xaero events to any form you like (Vec<u8> is pretty much for laxity)
    Reduce(Arc<dyn Fn(Vec<XaeroEvent>) -> Vec<u8> + Send + Sync>),

    // Systemic operators
    /// Terminal op: drop all events.
    Blackhole,
}

/// Unit tests for event serialization and archiving via `rkyv`.
#[cfg(test)]
mod tests {
    use rkyv::{Archived, rancor::Failure};

    use crate::{event::Event, initialize};

    #[test]
    pub fn test_basic_serde() {
        initialize();
        let event = crate::event::Event::<String>::new("test".to_string(), 1);
        let bytes: rkyv::util::AlignedVec =
            rkyv::to_bytes::<Failure>(&Event::new("hello world".to_string(), 1))
                .expect("failed to serialize");
        let arch_event = rkyv::access::<Archived<Event<String>>, Failure>(&bytes)
            .expect("failed to access archived event");
        tracing::info!("non archived event: {:#?}", event);
        tracing::info!("archived event: {:#?}", arch_event);
    }
}
