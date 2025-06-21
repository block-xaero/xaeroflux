//! on-disk serialization format for xaeroflux-actors segments.
//!
//! Defines:
//! - Magic constants and sizing parameters for paging and segments.
//! - On-disk representations for MMR nodes and pages.
//! - On-disk event header and archival/unarchival functions for Arc<XaeroEvent>.
//! - On-disk Merkle node and page structures for proof storage.

use std::{mem, sync::Arc};

use bytemuck::{Pod, Zeroable};
use rusted_ring::AllocationError;
use xaeroflux_core::{XaeroPoolManager, event::XaeroEvent};

/// Magic prefix used at the start of all on-disk pages and events.
pub const XAERO_MAGIC: [u8; 4] = *b"XAER";
/// Size in bytes of the `XaeroOnDiskEventHeader`.
pub const EVENT_HEADER_SIZE: usize = mem::size_of::<XaeroOnDiskEventHeader>(); // 24 bytes
/// Total header size (magic, type, padding, and timestamp) in bytes.
pub const HEADER_SIZE: usize = 4 + 1 + 7 + 8 + 8 + 8; // = 36
/// Size in bytes of a single on-disk MMR node entry.
pub const NODE_SIZE: usize = 32 + 1 + 7; // = 40
/// Size of each on-disk page (16 KiB).
pub const PAGE_SIZE: usize = 16 * 1024; // 16 KiB
/// Number of MMR nodes that fit in one page after accounting for headers.
pub const NODES_PER_PAGE: usize = (PAGE_SIZE - HEADER_SIZE) / NODE_SIZE;
/// Number of pages per segment file.
pub const PAGES_PER_SEGMENT: usize = 1_024;

/// On-disk representation of a single Merkle Mountain Range node.
///
/// - `hash`: 32-byte node hash.
/// - `is_leaf`: 1 if this node is a leaf, 0 otherwise.
/// - `_pad`: padding to align to `NODE_SIZE`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MmrOnDiskNode {
    pub hash: [u8; 32],
    pub is_leaf: u8,
    _pad: [u8; 7],
}
unsafe impl Zeroable for MmrOnDiskNode {}
unsafe impl Pod for MmrOnDiskNode {}

/// On-disk layout of an MMR page.
///
/// Pages contain:
/// - `marker`: magic bytes to identify XAER format.
/// - `version`: format version.
/// - `leaf_start`: index of the first leaf hash in this page.
/// - `total_nodes`: total MMR nodes up to this point.
/// - `nodes`: fixed array of `MmrOnDiskNode` entries.
/// - `_pad`: padding to fill the rest of the 16 KiB page.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MmrOnDiskPage {
    pub marker: [u8; 4],  // b"XAER"
    pub version: u64,     // format version
    pub leaf_start: u64,  // offset in leaf_hashes of this page's first leaf
    pub total_nodes: u64, // total nodes in entire MMR at this point
    pub nodes: [MmrOnDiskNode; NODES_PER_PAGE],
    _pad: [u8; PAGE_SIZE - HEADER_SIZE - NODE_SIZE * NODES_PER_PAGE],
}
unsafe impl Zeroable for MmrOnDiskPage {}
unsafe impl Pod for MmrOnDiskPage {}

/// Header for an archived event stored on disk.
///
/// Structure:
/// - `marker`: 4-byte magic "XAER".
/// - `len`: length of the event data payload in bytes.
/// - `event_type`: type discriminator for the event.
/// - `_pad1`: padding to align timestamp to 8-byte boundary.
/// - `timestamp`: event timestamp for ordering.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct XaeroOnDiskEventHeader {
    pub marker: [u8; 4], // "XAER" - 4 bytes
    pub len: u32,        // payload length - 4 bytes
    pub event_type: u8,  // event type - 1 byte
    pub _pad1: [u8; 7],  // padding to align timestamp - 7 bytes
    pub timestamp: u64,  // latest_ts for ordering - 8 bytes
}
// Total: 4 + 4 + 1 + 7 + 8 = 24 bytes

unsafe impl Pod for XaeroOnDiskEventHeader {}
unsafe impl Zeroable for XaeroOnDiskEventHeader {}
const _: () = assert!(std::mem::size_of::<XaeroOnDiskEventHeader>() == 24);

/// Serialize an Arc<XaeroEvent> to a byte vector ready for paging.
///
/// This creates a 24-byte `XaeroOnDiskEventHeader` followed by the raw event data
/// accessed through the PooledEventPtr (zero-copy).
///
/// Only stores essential data:
/// - Event data (via zero-copy access)
/// - Event type
/// - Timestamp for ordering
///
/// Does NOT store:
/// - Vector clock (collaboration metadata)
/// - Author ID (identity information)
/// - Merkle proof (handled separately)
pub fn archive_xaero_event(xaero_event: &Arc<XaeroEvent>) -> Vec<u8> {
    // Zero-copy access to ring buffer data
    let event_data = xaero_event.data();
    let event_type = xaero_event.event_type();
    // FIX: latest_ts is u64, not Option<u64>
    let timestamp = xaero_event.latest_ts;

    // Create header with essential metadata
    let header = XaeroOnDiskEventHeader {
        marker: XAERO_MAGIC,
        len: event_data.len() as u32,
        event_type,
        _pad1: [0; 7], // 7 bytes of padding for alignment
        timestamp,
    };

    // Allocate buffer: header + raw event data
    let mut bytes = Vec::with_capacity(EVENT_HEADER_SIZE + event_data.len());
    bytes.extend_from_slice(bytemuck::bytes_of(&header));
    bytes.extend_from_slice(event_data);

    tracing::debug!(
        "Archived event: type={}, data_len={}, timestamp={}, total_size={}",
        event_type,
        event_data.len(),
        timestamp,
        bytes.len()
    );

    bytes
}

/// Parse a memory slice into an event header and raw event data.
///
/// Validates the magic prefix and payload length,
/// then returns the header values and raw event data.
///
/// Returns: (header_copy, raw_event_data)
///
/// # Panics
/// Panics if the magic prefix is invalid or the buffer is too small.
pub fn unarchive_to_raw_data(bytes: &[u8]) -> (XaeroOnDiskEventHeader, &[u8]) {
    if bytes.len() < EVENT_HEADER_SIZE {
        panic!(
            "Buffer too small for header: need {} bytes but got {}",
            EVENT_HEADER_SIZE,
            bytes.len()
        );
    }

    // FIX: Copy header bytes to aligned buffer to avoid alignment issues
    let mut header_bytes = [0u8; EVENT_HEADER_SIZE];
    header_bytes.copy_from_slice(&bytes[0..EVENT_HEADER_SIZE]);

    let header: XaeroOnDiskEventHeader = *bytemuck::from_bytes(&header_bytes);

    // Validate magic prefix
    if header.marker != XAERO_MAGIC {
        panic!(
            "Invalid magic number: expected {:?}, got {:?}",
            XAERO_MAGIC, header.marker
        );
    }

    // Extract event data
    let start = EVENT_HEADER_SIZE;
    let end = start + header.len as usize;

    if bytes.len() < end {
        panic!(
            "Buffer too small for event data: need {} bytes but got {}",
            end,
            bytes.len()
        );
    }

    let event_data = &bytes[start..end];

    tracing::debug!(
        "Unarchived raw data: type={}, data_len={}, timestamp={}",
        header.event_type,
        event_data.len(),
        header.timestamp
    );

    (header, event_data)
}

/// Reconstruct an Arc<XaeroEvent> from archived bytes using XaeroPoolManager.
///
/// Validates the header, extracts the event data, and creates a new XaeroEvent
/// in the ring buffer pools with the same data and metadata.
///
/// Returns an error if pool allocation fails.
pub fn unarchive_to_xaero_event(bytes: &[u8]) -> Result<Arc<XaeroEvent>, AllocationError> {
    let (header, event_data) = unarchive_to_raw_data(bytes);

    // Reconstruct XaeroEvent using XaeroPoolManager
    // FIX: Pass timestamp as u64 since that's what latest_ts expects
    XaeroPoolManager::create_xaero_event(
        event_data,
        header.event_type,
        None, // author_id not stored in archive
        None, // merkle_proof not stored in archive
        None, // vector_clock not stored in archive
        header.timestamp,
    ).map_err(|pool_error| {
        tracing::error!("Pool allocation failed during unarchive: {:?}", pool_error);
        AllocationError::EventCreation("pool allocation failed during unarchive")
    })
}

/// Helper function to peek at event metadata without full reconstruction
pub fn peek_event_metadata(bytes: &[u8]) -> Result<(u8, u64, usize), &'static str> {
    if bytes.len() < EVENT_HEADER_SIZE {
        return Err("Buffer too small for header");
    }

    // Copy header bytes to aligned buffer to avoid alignment issues
    let mut header_bytes = [0u8; EVENT_HEADER_SIZE];
    header_bytes.copy_from_slice(&bytes[0..EVENT_HEADER_SIZE]);

    let header: XaeroOnDiskEventHeader = *bytemuck::from_bytes(&header_bytes);

    if header.marker != XAERO_MAGIC {
        return Err("Invalid magic number");
    }

    Ok((header.event_type, header.timestamp, header.len as usize))
}

/// On-disk representation of a Merkle proof node.
///
/// - `hash`: 32-byte proof hash.
/// - `flags`: node flags encoding position or type.
/// - `_pad`: padding for `NODE_SIZE` alignment.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct XaeroOnDiskMerkleNode {
    pub hash: [u8; 32],
    pub flags: u8,
    pub _pad: [u8; 7], // 32+1+7 = 40
}
unsafe impl Zeroable for XaeroOnDiskMerkleNode {}
unsafe impl Pod for XaeroOnDiskMerkleNode {}

/// On-disk layout of a Merkle proof page.
///
/// Pages include:
/// - `marker`: magic bytes "XAER".
/// - `event_type`: type byte for proof section.
/// - `_pad1`: padding for alignment.
/// - `version`: format version.
/// - `leaf_start`: starting leaf index in this page.
/// - `total_nodes`: total proof nodes to date.
/// - `nodes`: fixed array of proof nodes.
/// - `_pad2`: padding to fill to `PAGE_SIZE`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct XaeroOnDiskMerklePage {
    pub marker: [u8; 4],                       // "XAER"
    pub event_type: u8,                        // 1 byte
    pub _pad1: [u8; 3],                        // align `version`
    pub version: u64,                          // 8
    pub leaf_start: u64,                       // 8
    pub total_nodes: u64,                      // 8
    pub nodes: [XaeroOnDiskMerkleNode; 16320], // NODES_PER_PAGE], // 408*40 = 16320
    pub _pad2: [u8; 32],                       // Adjusted padding to ensure correct size
}
unsafe impl Zeroable for XaeroOnDiskMerklePage {}
unsafe impl Pod for XaeroOnDiskMerklePage {}

#[cfg(test)]
mod tests {
    use xaeroflux_core::{date_time::emit_secs, event::EventType, initialize};
    use xaeroflux_core::event::SystemEventKind;
    use super::*;

    fn setup() {
        initialize();
        XaeroPoolManager::init();
    }

    #[test]
    fn test_archive_unarchive_roundtrip() {
        setup();

        let test_data = b"test event data for archival";
        let event_type = EventType::ApplicationEvent(42).to_u8();
        let timestamp = emit_secs();

        // Create original event
        let original_event = XaeroPoolManager::create_xaero_event(
            test_data,
            event_type,
            None, None, None,
            timestamp
        ).expect("Failed to create test event");

        // Archive the event
        let archived_bytes = archive_xaero_event(&original_event);

        // Unarchive back to Arc<XaeroEvent>
        let reconstructed_event = unarchive_to_xaero_event(&archived_bytes).expect("Failed to unarchive event");

        // Verify data integrity
        assert_eq!(reconstructed_event.data(), test_data);
        assert_eq!(reconstructed_event.event_type(), event_type);
        assert_eq!(reconstructed_event.latest_ts, timestamp);
    }

    #[test]
    fn test_unarchive_to_raw_data() {
        setup();

        let test_data = b"raw data test";
        let event_type = EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8();
        let timestamp = 12345678;

        let event = XaeroPoolManager::create_xaero_event(
            test_data,
            event_type,
            None, None, None,
            timestamp
        ).expect("Failed to create test event");

        let archived_bytes = archive_xaero_event(&event);
        let (header, raw_data) = unarchive_to_raw_data(&archived_bytes);

        assert_eq!(header.marker, XAERO_MAGIC);
        assert_eq!(header.len, test_data.len() as u32);
        assert_eq!(header.event_type, event_type);
        assert_eq!(header.timestamp, timestamp);
        assert_eq!(raw_data, test_data);
    }

    #[test]
    fn test_peek_event_metadata() {
        setup();

        let test_data = b"metadata peek test";
        let event_type = EventType::ApplicationEvent(123).to_u8();
        let timestamp = 987654321;

        let event = XaeroPoolManager::create_xaero_event(
            test_data,
            event_type,
            None, None, None,
            timestamp
        ).expect("Failed to create test event");

        let archived_bytes = archive_xaero_event(&event);
        let (peeked_type, peeked_timestamp, peeked_len) = peek_event_metadata(&archived_bytes)
            .expect("Failed to peek event metadata");

        assert_eq!(peeked_type, event_type);
        assert_eq!(peeked_timestamp, timestamp);
        assert_eq!(peeked_len, test_data.len());
    }

    #[test]
    fn test_zero_copy_access() {
        setup();

        let large_data = vec![0xAB; 4096]; // 4KB test data
        let event_type = EventType::ApplicationEvent(255).to_u8();

        let event = XaeroPoolManager::create_xaero_event(
            &large_data,
            event_type,
            None, None, None,
            emit_secs()
        ).expect("Failed to create large event");

        // Archive should use zero-copy access to ring buffer
        let archived_bytes = archive_xaero_event(&event);

        // Verify the archived data contains our test pattern
        let (header, raw_data) = unarchive_to_raw_data(&archived_bytes);
        assert_eq!(raw_data, large_data.as_slice());
        assert_eq!(header.len, large_data.len() as u32);
    }

    #[test]
    #[should_panic(expected = "Invalid magic number")]
    fn test_invalid_magic_number() {
        let bad_data = vec![0xFF; 100];
        let _ = unarchive_to_raw_data(&bad_data);
    }

    #[test]
    #[should_panic(expected = "Buffer too small")]
    fn test_buffer_too_small() {
        let small_data = vec![0; 10]; // Smaller than EVENT_HEADER_SIZE
        let _ = unarchive_to_raw_data(&small_data);
    }

    #[test]
    fn test_empty_event_data() {
        setup();

        let empty_data = b"";
        let event_type = EventType::ApplicationEvent(0).to_u8();

        let event = XaeroPoolManager::create_xaero_event(
            empty_data,
            event_type,
            None, None, None,
            emit_secs()
        ).expect("Failed to create empty event");

        let archived_bytes = archive_xaero_event(&event);
        let (header, raw_data) = unarchive_to_raw_data(&archived_bytes);

        assert_eq!(header.len, 0);
        assert_eq!(raw_data.len(), 0);
        assert_eq!(raw_data, empty_data);
    }

    #[test]
    fn test_timestamp_handling() {
        setup();

        let timestamp = 12345u64;
        let test_event = XaeroPoolManager::create_xaero_event(
            b"timestamp test",
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            timestamp,
        ).expect("Failed to create event with timestamp");

        let archived = archive_xaero_event(&test_event);
        let (header, _) = unarchive_to_raw_data(&archived);
        assert_eq!(header.timestamp, timestamp);

        // Verify reconstruction
        let reconstructed = unarchive_to_xaero_event(&archived).expect("Failed to reconstruct");
        assert_eq!(reconstructed.latest_ts, timestamp);
    }
}