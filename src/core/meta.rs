use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};

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

/// A little cursor for each consumer so they know  
/// "which segment → which page → byte‐offset within that page"
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
