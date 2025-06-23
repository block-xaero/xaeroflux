use std::sync::{Arc, OnceLock};

use iroh::Endpoint;
use rusted_ring::RingPtr;
use xaeroid::XaeroID;

use crate::networking::network_conduit::NetworkPipe;
use crate::networking::BufferStatus;

pub struct XaeroPeerState {
    pub endpoint: Arc<Endpoint>,
    pub current_phase: XaeroProtocolPhase,
    pub buffer_status: BufferStatus,
    tokio_rt: &'static OnceLock<tokio::runtime::Runtime>,
}
pub struct XaeroPeer {
    pub xaero_id: RingPtr<XaeroID>,
    pub pipe: NetworkPipe,
}

pub enum XaeroProtocolPhase {
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
