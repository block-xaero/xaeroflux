use bytemuck::{Pod, Zeroable};
use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::core::aof::storage::format::SegmentMeta;

/// The data carried by each system event.
/// All fields are fixed-size so this enum is Pod-friendly.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum SystemPayload {
    /// Indicates an event payload was written to a segment page.
    PayloadWritten {
        leaf_hash: [u8; 32],
        meta: SegmentMeta,
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
        leaf_index: u64,
        error_code: u16,
    },
    SecondaryIndexWritten {
        leaf_hash: [u8; 32],
    },
    SecondaryIndexFailed {
        leaf_hash: [u8; 32],
        error_code: u16,
    },
}

unsafe impl Zeroable for SystemPayload {}
unsafe impl Pod for SystemPayload {}

pub struct ControlBus {
    pub(crate) tx: Sender<SystemPayload>,
    pub(crate) rx: Receiver<SystemPayload>,
}

impl Default for ControlBus {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlBus {
    /// Create a new ControlBus with an unbounded channel.
    pub fn new() -> Self {
        let (tx, rx) = unbounded();
        Self { tx, rx }
    }

    /// Get a sender handle for publishing system payloads.
    pub fn sender(&self) -> Sender<SystemPayload> {
        self.tx.clone()
    }

    /// Subscribe to receive all system payloads.
    pub fn subscribe(&self) -> Receiver<SystemPayload> {
        self.rx.clone()
    }
}
