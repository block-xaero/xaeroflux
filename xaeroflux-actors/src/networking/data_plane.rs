use std::sync::Arc;

use xaeroflux_core::{
    event::XaeroEvent,
    pool::{FixedMerkleProof, FixedVectorClock},
};

use crate::{
    aof::storage::format::SegmentMeta,
    networking::SyncContext,
};

// Topic: "xaeroflux/group/{group_id}/workspace/{workspace_id}/object/{object_id}/data"
pub enum DataMessage {
    // Live CRDT operations
    LiveEvent {
        event: Vec<u8>,
        merkle_proof: Option<FixedMerkleProof>,
        vector_clock: Option<FixedVectorClock>,
    },

    // Bulk sync responses
    SegmentData {
        segment_id: SegmentMeta,
        events: Vec<Arc<XaeroEvent>>, // Batch of events
        is_final: bool,               // Last chunk of segment
    },

    // Individual event responses
    EventData {
        event_hash: [u8; 32],
        event: Arc<XaeroEvent>,
        context: SyncContext, // Why this was sent
    },
}
