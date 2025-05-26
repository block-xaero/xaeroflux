use bytemuck::{Pod, Zeroable};
use crate::core::event::{LeafLocation, SystemErrorCode};

/// The data carried by each system event.
/// All fields are fixed-size so this enum is Pod-friendly.
#[repr(C)]
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, Pod, Zeroable)]
#[archive_bounds(::core::fmt::Debug)]
pub enum SystemPayload {
    PayloadWritten { leaf_index: u64, loc: LeafLocation },
    SegmentRolledOver { ts_start: u64, seg_idx: u32 },
    SegmentRollOverFailed { ts_start: u64, seg_idx: u32, error_code: u16 },
    PageFlushed { page_index: u32 },
    PageFlushFailed { page_index: u32, error_code: u16 },
    MmrAppended { leaf_index: u64, leaf_hash: [u8;32] },
    MmrAppendFailed { leaf_index: u64, error_code: u16 },
    SecondaryIndexWritten { leaf_hash: [u8;32] },
    SecondaryIndexFailed { leaf_hash: [u8;32], error_code: u16 },
}