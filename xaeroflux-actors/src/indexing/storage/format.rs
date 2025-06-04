//! on-disk serialization format for xaeroflux-actors segments.
//!
//! Defines:
//! - Magic constants and sizing parameters for paging and segments.
//! - On-disk representations for MMR nodes and pages.
//! - On-disk event header and archival/unarchival functions.
//! - On-disk Merkle node and page structures for proof storage.

use std::mem;

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, rancor::Failure};
use xaeroflux_core::event::{ArchivedEvent, Event};

/// Magic prefix used at the start of all on-disk pages and events.
pub const XAERO_MAGIC: [u8; 4] = *b"XAER";
/// Size in bytes of the `XaeroOnDiskEventHeader` (always 16).
pub const EVENT_HEADER_SIZE: usize = mem::size_of::<XaeroOnDiskEventHeader>(); // 12
/// Total header size (magic, type, padding, and three u64 fields) in bytes.
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
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
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
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
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
/// - `len`: length of the archived payload in bytes.
/// - `event_type`: type discriminator for the event.
/// - `_pad1`: padding to align the following version field.
#[repr(C)]
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct XaeroOnDiskEventHeader {
    pub marker: [u8; 4], // "XAER"
    pub len: u32,        // payload length
    pub event_type: u8,  // 1 byte
    pub _pad1: [u8; 7],  // align `version`
}

unsafe impl Pod for XaeroOnDiskEventHeader {}
unsafe impl Zeroable for XaeroOnDiskEventHeader {}
const _: () = assert!(std::mem::size_of::<XaeroOnDiskEventHeader>() == 16);

/// Serialize an event to a byte vector ready for paging.
///
/// This prepends a 16-byte `XaeroOnDiskEventHeader` to the
/// Rkyv-serialized event payload.
///
/// # Panics
/// Panics if the Rkyv serialization fails.
pub fn archive(e: &Event<Vec<u8>>) -> Vec<u8> {
    let archived_b = rkyv::to_bytes::<Failure>(e).expect("failed_to_archive_event");
    let mut bytes = Vec::with_capacity(EVENT_HEADER_SIZE + archived_b.len());
    let header = XaeroOnDiskEventHeader {
        marker: XAERO_MAGIC,
        len: archived_b.len() as u32,
        event_type: e.event_type.to_u8(),
        _pad1: [0; 7],
    };
    bytes.extend_from_slice(bytemuck::bytes_of(&header));
    bytes.extend_from_slice(archived_b.as_slice());
    bytes
}
/// Parse a memory slice into an event header and archived event.
///
/// Validates the magic prefix and payload length,
/// then returns references to the header and the Rkyv-backed payload.
///
/// # Panics
/// Panics if the magic prefix is invalid or the buffer is too small,
/// or if Rkyv access fails.
pub fn unarchive<'a>(bytes: &'a [u8]) -> (&'a XaeroOnDiskEventHeader, &'a ArchivedEvent<Vec<u8>>) {
    let header: &'a XaeroOnDiskEventHeader = bytemuck::from_bytes(&bytes[0..EVENT_HEADER_SIZE]);
    // removing header
    if header.marker != XAERO_MAGIC {
        panic!("invalid magic number"); // FIXME: corrupt data needs to be handled!
    }

    let start = EVENT_HEADER_SIZE;
    let end = start + header.len as usize;

    if bytes.len() < end {
        panic!(
            "buffer too small: need {} bytes but got {}",
            end,
            bytes.len()
        );
    }
    let payload = &bytes[start..end];
    match rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, Failure>(payload) {
        Ok(archived) => {
            tracing::debug!(
                "unarchived successfully: header.len={} payload.len={}",
                header.len,
                payload.len()
            );
            (header, archived)
        }
        Err(e) => {
            // dump diagnostics
            tracing::error!("=== rkyv access Failure: {:?} ===", e);
            tracing::error!(
                "header: {:?}, EVENT_HEADER_SIZE={}, payload.len={}",
                header,
                EVENT_HEADER_SIZE,
                payload.len()
            );
            let snippet = &payload[..payload.len().min(64)];
            tracing::error!("payload[0..min(64)]: {}", hex::encode(snippet));
            panic!("failed to unarchive event: {:?}", e);
        }
    }
}

/// On-disk representation of a Merkle proof node.
///
/// - `hash`: 32-byte proof hash.
/// - `flags`: node flags encoding position or type.
/// - `_pad`: padding for `NODE_SIZE` alignment.
#[repr(C)]
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
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
#[derive(Clone, Copy, Archive, Debug)]
#[rkyv(derive(Debug))]
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
