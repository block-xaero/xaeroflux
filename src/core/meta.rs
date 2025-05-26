//! Core metadata types for xaeroflux.
//!
//! This module defines:
//! - `SegmentMeta`: metadata about a single log segment and its pages.
//! - `ReaderCursor`: tracks a consumer’s position within the segment log.
//! - `MMRMeta`: Merkle Mountain Range index metadata per segment.

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};

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
    pub page_index: usize,
    pub segment_index: usize,
    pub write_pos: usize,
    pub byte_offset: usize,
    pub latest_segment_id: usize,
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
#[derive(Debug, Clone, Archive, Serialize, Deserialize, Default)]
#[rkyv(derive(Debug))]
#[derive(Copy)]
pub struct ReaderCursor {
    pub suscriber_name: [u8; 32], // Replace String with a fixed-size array to make it Copy
    pub subscriber_id: usize,
    pub page_index: usize,
    pub segment_index: usize,
    pub read_pos: usize,
    pub byte_offset: usize,
    pub latest_segment_id: usize,
}

unsafe impl Zeroable for ReaderCursor {}
unsafe impl Pod for ReaderCursor {}

/// Metadata for the Merkle Mountain Range (MMR) index on a segment.
///
/// Fields:
/// - `root_hash`: 32-byte hash of the MMR root for integrity proofs.
/// - `peaks_count`: number of peaks (sub-roots) in the MMR structure.
/// - `leaf_count`: total number of leaves (events) in the MMR.
/// - `segment_meta`: nested `SegmentMeta` for the segment containing this MMR.
#[repr(C, packed)]
#[derive(Debug, Clone, Default, Copy)]
pub struct MMRMeta {
    pub root_hash: [u8; 32], // Replace String with a fixed-size array to make it Copy
    pub peaks_count: usize,
    pub leaf_count: usize,
    pub segment_meta: SegmentMeta,
}

unsafe impl Zeroable for MMRMeta {}
unsafe impl Pod for MMRMeta {}
