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
use xaeroflux_core::{event::XaeroEvent, pool::XaeroInternalEvent};

/// Magic prefix used at the start of all on-disk pages and events.
pub const XAERO_MAGIC: [u8; 4] = *b"XAER";
/// Size in bytes of the `XaeroOnDiskEventHeader`.
pub const EVENT_HEADER_SIZE: usize = mem::size_of::<XaeroOnDiskEventHeader>(); // 24 bytes
/// Total header size (magic, type, padding, and timestamp) in bytes.
pub const HEADER_SIZE: u32 = 4 + 1 + 7 + 8 + 8 + 8; // = 36
/// Size in bytes of a single on-disk MMR node entry.
pub const NODE_SIZE: u32 = 32 + 1 + 7; // = 40
/// Size of each on-disk page (16 KiB).
pub const PAGE_SIZE: u32 = 16 * 1024; // 16 KiB
/// Number of MMR nodes that fit in one page after accounting for headers.
pub const NODES_PER_PAGE: u32 = (PAGE_SIZE - HEADER_SIZE) / NODE_SIZE;
/// Number of pages per segment file.
pub const PAGES_PER_SEGMENT: u32 = 1_024;

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
    pub nodes: [MmrOnDiskNode; NODES_PER_PAGE as usize],
    _pad: [u8; (PAGE_SIZE - HEADER_SIZE - NODE_SIZE * NODES_PER_PAGE) as usize],
}
unsafe impl Zeroable for MmrOnDiskPage {}
unsafe impl Pod for MmrOnDiskPage {}

const fn archive_buffer_size<const SIZE: usize>() -> usize {
    EVENT_HEADER_SIZE + SIZE
}

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

pub use paste::paste;
macro_rules! impl_archive_for_size {
    ($size:expr) => {
        paste::paste! {
            pub fn [<archive_xaero_internal_event_ $size>](event: XaeroInternalEvent<$size>) -> [u8; EVENT_HEADER_SIZE + $size] {
                let mut result = [0u8; EVENT_HEADER_SIZE + $size];

                // Header section (32 bytes)
                result[0..32].copy_from_slice(&event.xaero_id_hash);
                result[32..64].copy_from_slice(&event.vector_clock_hash);

                // Event data section - handle size correctly
                let event_data = &event.evt.data;
                let copy_len = std::cmp::min(event_data.len(), $size);
                result[EVENT_HEADER_SIZE..EVENT_HEADER_SIZE + copy_len].copy_from_slice(&event_data[..copy_len]);

                // Set remaining bytes to zero if event_data is smaller than $size
                if copy_len < $size {
                    result[EVENT_HEADER_SIZE + copy_len..].fill(0);
                }

                result
            }
        }
    };
}

// Generate functions for your T-shirt sizes
impl_archive_for_size!(64); // XS
impl_archive_for_size!(256); // S
impl_archive_for_size!(1024); // M
impl_archive_for_size!(4096); // L
impl_archive_for_size!(16384); // XL

// What your macro should generate:
pub const ARCHIVE_SIZE_64: usize = EVENT_HEADER_SIZE + std::mem::size_of::<XaeroInternalEvent<64>>();
pub const ARCHIVE_SIZE_256: usize = EVENT_HEADER_SIZE + std::mem::size_of::<XaeroInternalEvent<256>>();
pub const ARCHIVE_SIZE_1024: usize = EVENT_HEADER_SIZE + std::mem::size_of::<XaeroInternalEvent<1024>>();
pub const ARCHIVE_SIZE_4096: usize = EVENT_HEADER_SIZE + std::mem::size_of::<XaeroInternalEvent<4096>>();
pub const ARCHIVE_SIZE_16384: usize = EVENT_HEADER_SIZE + std::mem::size_of::<XaeroInternalEvent<16384>>();
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
        panic!("Buffer too small for header: need {} bytes but got {}", EVENT_HEADER_SIZE, bytes.len());
    }

    // FIX: Copy header bytes to aligned buffer to avoid alignment issues
    let mut header_bytes = [0u8; EVENT_HEADER_SIZE];
    header_bytes.copy_from_slice(&bytes[0..EVENT_HEADER_SIZE]);

    let header: XaeroOnDiskEventHeader = *bytemuck::from_bytes(&header_bytes);

    // Validate magic prefix
    if header.marker != XAERO_MAGIC {
        panic!("Invalid magic number: expected {:?}, got {:?}", XAERO_MAGIC, header.marker);
    }

    // Extract event data
    let start = EVENT_HEADER_SIZE;
    let end = start + header.len as usize;

    if bytes.len() < end {
        panic!("Buffer too small for event data: need {} bytes but got {}", end, bytes.len());
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
