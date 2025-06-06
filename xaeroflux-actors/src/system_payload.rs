use bytemuck::{Pod, Zeroable};

use crate::{aof::storage::format::SegmentMeta, networking::p2p::ADDRESS_MAX};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum SystemPayload {
    SubjectCreated {
        subject_id: [u8; 32],
    },
    WorkspaceCreated {
        workspace_id: [u8; 32],
    },
    ObjectCreated {
        object_id: [u8; 32],
    },
    /// Indicates an event payload was written to a segment page.
    PayloadWritten {
        leaf_hash: [u8; 32],
        meta: SegmentMeta,
    },

    PeerDiscovered {
        peer_id: [u8; 32],
        address: [u8; ADDRESS_MAX],
    },
    PeerLost {
        peer_id: [u8; 32],
        address: [u8; ADDRESS_MAX],
    },
    SegmentRolledOver {
        meta: SegmentMeta,
    },
    SegmentRollOverFailed {
        meta: SegmentMeta,
        error_code: u16,
    },
    PageFlushed {
        meta: SegmentMeta,
    },
    PageFlushFailed {
        meta: SegmentMeta,
        error_code: u16,
    },
    MmrAppended {
        leaf_hash: [u8; 32],
    },
    MmrAppendFailed {
        leaf_hash: [u8; 32],
        error_code: u16,
    },
    SecondaryIndexWritten {
        leaf_hash: [u8; 32],
    },
    SecondaryIndexFailed {
        leaf_hash: [u8; 32],
        error_code: u16,
    },
    PeaksWritten {
        peak_count: u64,
        root_hash: [u8; 32],
    },
    SegmentMetaChanged {
        meta: SegmentMeta,
    },
    JoinedTopic {
        topic_id: u32,
    },
    LeftTopic {
        topic_id: u32,
    },
    MMRLeafAppended {
        leaf_hash: [u8; 32],
    },
}

unsafe impl Pod for SystemPayload {}
unsafe impl Zeroable for SystemPayload {}
