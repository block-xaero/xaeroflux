use std::sync::{Arc, OnceLock};

use iroh::Endpoint;
use rusted_ring::RingPtr;
use xaeroflux_core::{XaeroPoolManager, P2P_RUNTIME};
use xaeroid::{XaeroCredential, XaeroID, XaeroProof};

use crate::networking::{BufferStatus, network_conduit::NetworkPipe, xaero_id_zero};

pub struct XaeroPeerState {
    pub endpoint: Arc<Endpoint>,
    pub current_phase: XaeroProtocolPhase,
    pub buffer_status: BufferStatus,
}
pub struct XaeroPeer {
    pub xaero_id: RingPtr<XaeroID>,
    pub state: XaeroPeerState,
    pub pipe: NetworkPipe,
}

impl XaeroPeer {
    pub fn new(endpoint: Arc<Endpoint>, pipe: NetworkPipe) -> Self {
        // assert p2p runtime
        // FIXME: XaeroID needed here - which is legitimate.
        XaeroPeer {
            xaero_id: XaeroPoolManager::allocate_xaero_id(xaero_id_zero()).expect("Failed to allocate Xaero ID"),
            state: XaeroPeerState {
                endpoint,
                current_phase: XaeroProtocolPhase::Init,
                buffer_status: BufferStatus::Init,
            },
            pipe,
        }
    }
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
