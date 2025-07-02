use std::sync::Arc;

pub struct XaeroPeerState {
    pub current_phase: XaeroProtocolPhase,
}
pub struct XaeroPeer {
    pub xaero_id: [u8; 32],
    pub state: XaeroPeerState,
}

pub enum XaeroProtocolPhase {
    Init,            // Initialization phase
    DiscoverPeers,   // Initial discovery
    Handshake,       // Connection establishment
    SyncRequest,     // Sync negotiation
    SyncResponse,    // Response to SyncRequest with actual data
    LiveEventStream, // Real-time CRDT operations during collaboration
    PeakUpdate,      // Incremental MMR peak updates
    FlowControl,     // Backpressure management
    SegmentTransfer, // Bulk segment data transfer
    EventQuery,      // Individual event requests by hash
    Disconnect,      // Graceful peer disconnection
}
