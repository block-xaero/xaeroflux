use xaeroid::XaeroID;

use crate::{aof::storage::format::SegmentMeta, networking::BufferStatus};

// Topic: "xaeroflux/group/{group_id}/control/discovery"
pub enum ControlMessage {
    // Initial announcement
    PeerAnnouncement {
        xaero_id: XaeroID,
        merkle_peaks: Vec<[u8; 32]>, // Current MMR peaks
        last_event_hash: [u8; 32],   // Latest event seen
    },

    // Response to announcement
    PeerHandshake {
        xaero_id: XaeroID,
        merkle_peaks: Vec<[u8; 32]>,
        missing_ranges: Vec<[u8; 32]>, // What I'm missing
        have_ranges: Vec<[u8; 32]>,    // What I can provide
    },

    // Diff negotiation
    SyncRequest {
        missing_hashes: Vec<[u8; 32]>,      // Specific events I need
        segment_requests: Vec<SegmentMeta>, // Segments I want
    },

    // Metadata updates
    PeakUpdate {
        new_peaks: Vec<[u8; 32]>,
        event_count: u64,
    },

    // Backpressure/flow control
    FlowControl {
        peer_id: XaeroID,
        buffer_status: BufferStatus, // How full are my buffers
    },
}
