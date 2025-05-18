use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};

#[repr(C)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, Default)]
#[rkyv(derive(Debug))]
#[derive(Copy)]
pub struct SegmentMeta {
    pub page_index: usize,
    pub segment_index: usize,
    pub write_pos: usize,
    pub byte_offset: usize,
    pub latest_segment_id: usize,
}

unsafe impl Zeroable for SegmentMeta {}
unsafe impl Pod for SegmentMeta {}

#[repr(C)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize, Default)]
#[rkyv(derive(Debug))]
#[derive(Copy)]
pub struct MMRMeta {
    pub root_hash: [u8; 32], // Replace String with a fixed-size array to make it Copy
    pub peaks_count: usize,
    pub leaf_count: usize,
    pub segment_meta: SegmentMeta,
}

unsafe impl Zeroable for MMRMeta {}
unsafe impl Pod for MMRMeta {}

/// tells subscribers where to start reading from
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ReadMeta {
    pub page_index: usize,
    pub segment_index: usize,
    pub read_pos: usize,
    pub byte_offset: usize,
    pub latest_read_segment_id: usize,
}

unsafe impl Zeroable for ReadMeta {}
unsafe impl Pod for ReadMeta {}
