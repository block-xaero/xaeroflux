use std::sync::Arc;

use bytemuck::{Pod, Zeroable};
use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::core::{DISPATCHER_POOL, meta::SegmentMeta};

/// The data carried by each system event.
/// All fields are fixed-size so this enum is Pod-friendly.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum SystemPayload {
    /// Indicates an event payload was written to a segment page.
    PayloadWritten {
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
        leaf_index: u64,
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

    /// Register a handler that will be called for every system payload.
    pub fn register_handler<H: ControlHandler + Send + Sync + 'static>(&self, handler: Arc<H>) {
        let rx = self.rx.clone();
        DISPATCHER_POOL
            .get()
            .expect("DISPATCH POOL not initialized")
            .execute(move || {
                while let Ok(payload) = rx.recv() {
                    handler.handle(payload);
                }
            });
    }
}

/// Trait that actors implement to handle incoming system payloads.
pub trait ControlHandler {
    /// Called for each system payload received on the control bus.
    fn handle(&self, payload: SystemPayload);
}
