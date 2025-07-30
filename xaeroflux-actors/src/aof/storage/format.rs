//! Core metadata types for xaeroflux-actors.
//!
//! This module defines:
//! - `SegmentMeta`: metadata about a single log segment and its pages.
//! - `ReaderCursor`: tracks a consumer's position within the segment log.
//! - `MMRMeta`: Merkle Mountain Range index metadata per segment.

use bytemuck::{Pod, Zeroable};

/// Metadata for a persisted segment page in the append-only log.
///
/// Fields:
/// - `page_index`: zero-based index of the page within its segment.
/// - `segment_index`: index of the segment in the overall log.
/// - `write_pos`: number of pages written before this one rolled over.
/// - `byte_offset`: starting byte within the underlying file.
/// - `latest_segment_id`: ID of the most recently completed segment at write time.
/// - `ts_start`: timestamp (ms since epoch) of the first event in this segment.
/// - `ts_end`: timestamp (ms since epoch) of the last event in this segment.
#[repr(C, packed)]
#[derive(Debug, Clone, Default, Copy)]
pub struct SegmentMeta {
    pub page_index: u32,
    pub segment_index: u32,
    pub write_pos: u32,
    pub byte_offset: u32,
    pub latest_segment_id: u32,
    pub ts_start: u64, // Timestamp of the first event in this segment
    pub ts_end: u64,   // Timestamp of the last event in this segment
}

unsafe impl Zeroable for SegmentMeta {}
unsafe impl Pod for SegmentMeta {}

/// Cursor tracking a subscriber's read position in the segment log.
///
/// Fields:
/// - `subscriber_name`: fixed-size identifier of the subscriber.
/// - `subscriber_id`: numeric ID of the subscriber instance.
/// - `page_index`: current page index within the segment to read next.
/// - `segment_index`: current segment index to read next.
/// - `read_pos`: number of pages already consumed in this segment.
/// - `byte_offset`: byte offset within the current page.
/// - `latest_segment_id`: most recent segment ID observed by this reader.
#[repr(C)]
#[derive(Debug, Clone, Default, Copy)]
pub struct ReaderCursor {
    pub subscriber_name: [u8; 32], // Fixed-size array instead of String
    pub subscriber_id: usize,
    pub page_index: usize,
    pub segment_index: usize,
    pub read_pos: usize,
    pub byte_offset: usize,
    pub latest_segment_id: usize,
}

unsafe impl Zeroable for ReaderCursor {}
unsafe impl Pod for ReaderCursor {}

/// Metadata for the Merkle Mountain Range (MMR).
///
/// Fields:
/// - `root_hash`: 32-byte hash of the MMR root for integrity proofs.
/// - `peaks_count`: number of peaks (sub-roots) in the MMR structure.
/// - `leaf_count`: total number of leaves (events) in the MMR.
#[repr(C, packed)]
#[derive(Debug, Clone, Default, Copy)]
pub struct MmrMeta {
    pub root_hash: [u8; 32], // Fixed-size array instead of String
    pub peaks_count: usize,
    pub leaf_count: usize,
}

unsafe impl Zeroable for MmrMeta {}
unsafe impl Pod for MmrMeta {}

#[repr(C)]
#[derive(Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
pub struct EventKey {
    pub xaero_id_hash: [u8; 32],
    pub vector_clock_hash: [u8; 32],
    pub ts: u64,        // 8 bytes, big-endian
    pub kind: u8,       // 1 byte
    pub hash: [u8; 32], // 32 bytes
}
unsafe impl Pod for EventKey {}
unsafe impl Zeroable for EventKey {}

use std::fmt::Debug;

impl Debug for EventKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventKey {{ ts: {}, kind: {}, hash: {} }}", self.ts, self.kind, hex::encode(self.hash))
    }
}
