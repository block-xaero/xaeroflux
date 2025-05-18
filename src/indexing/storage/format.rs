use std::mem;

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, rancor::Failure};

use crate::core::event::{ArchivedEvent, Event};

pub const XAERO_MAGIC: [u8; 4] = *b"XAER";
pub const EVENT_HEADER_SIZE: usize = mem::size_of::<XaeroOnDiskEventHeader>(); // 12
pub const HEADER_SIZE: usize = 4 + 1 + 7 + 8 + 8 + 8; // = 36
pub const NODE_SIZE: usize = 32 + 1 + 7; // = 40
pub const PAGE_SIZE: usize = 16 * 1024; // 16 KiB
pub const NODES_PER_PAGE: usize = (PAGE_SIZE - HEADER_SIZE) / NODE_SIZE;
pub const PAGES_PER_SEGMENT: usize = 1_024;

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

/// Header for archived events on disk.
/// Starts with a magic number, followed by the length of the payload,
/// the event type, and some padding to align the version field.
/// Magic number is "XAER" (4 bytes).
/// Length is 4 bytes (u32).
/// Event type is 1 byte (u8).
/// Padding is 3 bytes (u8).
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

/// Archives an event to be shoved to pages on disk.
/// serializes the event to rkyv bytes and prepends a `XaeroOnDiskEventHeader`  to it.
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
/// Unarchives an event from a byte slice.
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
