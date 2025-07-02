use xaeroid::{XaeroCredential, XaeroProof};

pub mod discovery;
pub mod iroh;
mod p2p;
mod pool;
mod state;

pub fn xaero_id_zero() -> XaeroID {
    let xaero_proofs = [
        XaeroProof { zk_proof: [0u8; 32] },
        XaeroProof { zk_proof: [0u8; 32] },
        XaeroProof { zk_proof: [0u8; 32] },
        XaeroProof { zk_proof: [0u8; 32] },
    ];
    XaeroID {
        did_peer: [0u8; 897],
        did_peer_len: 0,
        secret_key: [0u8; 1281],
        _pad: [0u8; 3],
        credential: XaeroCredential {
            vc: [0u8; 256],
            vc_len: 256,
            proofs: xaero_proofs,
            proof_count: 0,
            _pad: [0u8; 1],
        },
    }
}

// Core traits and structs for XaeroFlux P2P networking
use std::{collections::HashMap, time::SystemTime};

use ::iroh::{Endpoint, NodeId, endpoint::Connection};
use iroh_gossip::{api::GossipTopic, net::Gossip};
use rkyv::{Archive, Deserialize, Serialize};
use rusted_ring_new::PooledEvent;
use xaeroflux_core::pool::XaeroPeerEvent;
use xaeroid::XaeroID;

use crate::aof::storage::format::SegmentMeta;

// =============================================================================
// GOSSIPSUB COLLABORATION LAYER
// =============================================================================

/// Gossipsub topic management for workspace collaboration
#[async_trait::async_trait]
pub trait GossipCollaboration {
    /// Join gossip topic for workspace collaboration
    async fn join_workspace_topic(
        &self,
        workspace_id: [u8; 32],
        bootstrap_peers: Vec<NodeId>,
    ) -> anyhow::Result<GossipTopic>;

    /// Leave workspace topic
    async fn leave_workspace_topic(&self, workspace_id: [u8; 32]) -> anyhow::Result<()>;

    /// Broadcast control message to workspace
    async fn broadcast_control(&self, workspace_id: [u8; 32], message: XaeroPeerEvent<64>) -> anyhow::Result<()>;

    /// Broadcast data message to workspace
    async fn broadcast_data(&self, workspace_id: [u8; 32], message: XaeroPeerEvent<1024>) -> anyhow::Result<()>;
}

// =============================================================================
// DIRECT CONNECTION LAYER
// =============================================================================

/// ALPN protocol identifiers
pub const XAERO_HANDSHAKE_ALPN: &[u8] = b"xaeroflux/handshake/1.0";
pub const XAERO_COLLABORATION_ALPN: &[u8] = b"xaeroflux/collaboration/1.0";
pub const XAERO_SYNC_ALPN: &[u8] = b"xaeroflux/sync/1.0";
pub const XAERO_FILE_TRANSFER_ALPN: &[u8] = b"xaeroflux/file-transfer/1.0";

/// Direct connection management for peer-to-peer communication
#[async_trait::async_trait]
pub trait DirectConnection {
    /// Establish direct connection with peer
    async fn connect_to_peer(&self, peer_id: NodeId, protocol: &[u8]) -> anyhow::Result<Connection>;

    async fn send_crdt_event(&self, peer_id: NodeId, object_id: [u8; 32], event: PooledEvent<64>)
    -> anyhow::Result<()>;

    /// Request file/segment transfer from peer
    async fn request_file_transfer(&self, peer_id: NodeId, file_hash: [u8; 32]) -> anyhow::Result<Vec<u8>>;

    /// Sync segments directly with peer
    async fn sync_segments(&self, peer_id: NodeId, missing_segments: Vec<SegmentMeta>) -> anyhow::Result<Vec<u8>>;
}

// =============================================================================
// CORE NETWORKING STATE
// =============================================================================

/// Main networking state combining all layers
pub struct XaeroNetworking {
    pub endpoint: Endpoint,
    pub gossip: Gossip,
    pub active_connections: HashMap<NodeId, ActivePeer>,
    pub workspace_topics: HashMap<[u8; 32], GossipTopic>,
    pub my_xaero_id: XaeroID,
    pub my_node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct ActivePeer {
    pub xaero_id: XaeroID,
    pub node_id: NodeId,
    pub last_seen: SystemTime,
    pub capabilities: Vec<String>,
    pub connection_status: ConnectionStatus,
}

#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    Discovered,      // Found via DHT
    GossipConnected, // Connected via gossipsub
    DirectConnected, // Direct connection established
    Syncing,         // Currently syncing data
    Offline,         // Known but unreachable
}

// =============================================================================
// EVENT TYPES FOR COLLABORATION
// =============================================================================

/// File transfer request/response
#[derive(Debug, Clone)]
pub struct FileTransferRequest {
    pub file_hash: [u8; 32],
    pub chunk_size: u32,
    pub start_offset: u64,
    pub end_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct FileTransferResponse {
    pub file_hash: [u8; 32],
    pub chunk_data: Vec<u8>,
    pub chunk_index: u32,
    pub is_final: bool,
}

// =============================================================================
// PROTOCOL HANDLERS
// =============================================================================

/// Protocol handler trait for different ALPN protocols
#[async_trait::async_trait]
pub trait ProtocolHandler: Send + Sync {
    async fn handle_connection(&self, conn: Connection) -> anyhow::Result<()>;
}

/// Handshake protocol for peer verification
pub struct HandshakeProtocol {
    pub xaero_id: XaeroID,
    pub capabilities: Vec<String>,
}
