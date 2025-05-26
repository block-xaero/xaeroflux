use std::sync::Arc;

use bytemuck::{Pod, Zeroable};
use rkyv::Archive;

/// Unified error type for P2P operations.
#[derive(Debug)]
pub enum XaeroP2PError {
    ControlPlaneError,
    DataPlaneError,
    DiscoveryError,
}

use crate::{XaeroEvent, core::meta::SegmentMeta, indexing::storage::mmr::Peak};

/// Unique identifier for a peer or node
pub type NodeId = [u8; 32];

/// Represents a peer in the network
#[derive(Debug, Clone)]
pub struct Peer {
    /// Unique ID for the peer (DID:peer public key hash)
    pub id: NodeId,
    /// Network address (multiaddr, mDNS name, etc.)
    pub address: String,
}

/// Messaging payload container
#[derive(Clone)]
pub struct XaeroGossipEvent {
    pub ts_micros: u64,
    pub kind: u8,
    pub event: XaeroEvent,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub struct PageRequest {
    pub segment_index: usize,
    pub page_index: usize,
}
unsafe impl Pod for PageRequest {}
unsafe impl Zeroable for PageRequest {}

#[repr(C)]
#[derive(Clone, Archive, Debug)]
#[rkyv(derive(Debug))]
pub struct PageResponse {
    pub segment_index: usize,
    pub page_index: usize,
    pub page: Vec<u8>, // Serialized page data
}

#[repr(C)]
#[derive(Copy, Clone)]
/// Request the Merkle proof for a specific leaf index.
pub struct ProofRequest {
    /// Global leaf index to prove
    pub leaf_index: u64,
}
unsafe impl Pod for ProofRequest {}
unsafe impl Zeroable for ProofRequest {}

#[repr(C)]
#[derive(Clone, Archive, Debug)]
/// Response carrying the Merkle proof for a requested leaf.
pub struct ProofResponse {
    /// Global leaf index this proof corresponds to
    pub leaf_index: u64,
    /// Serialized proof nodes; caller can verify against known MMR root
    pub proof: Vec<[u8; 32]>,
}

// 1) gossip.publish(Peaks(pub_peaks));
// 2) on DIFF -> gossip.publish(PageRequest(...));
// 3) on PageRequest -> gossip.publish(PageResponse(...));
// 4) on PageResponse -> apply pages;
// 5) when done, optionally gossip.publish(SyncDone);

pub enum XaeroSyncPhase {
    /// Initial phase where peers exchange peaks
    PeaksExchange,
    /// Phase where specific pages are requested
    PageRequest,
    /// Phase where pages are sent in response to requests
    PageResponse,
    /// Final phase indicating synchronization is complete
    SyncDone,
    /// From here on, push each new event as it arrives
    LiveStream,
    /// Proof request for event verification
    ProofRequest,
    /// Response containing the Merkle proof for a requested event
    ProofResponse,
}
/// Marker trait for Xaero Behaviors that implement data and control planes.
pub trait XaeroPlane {
    fn join_topic(&self, topic: &str) -> Result<(), XaeroP2PError>;
    fn leave_topic(&self, topic: &str) -> Result<(), XaeroP2PError>;
    /// Register a callback for asynchronous errors.
    fn on_error<F>(&self, handler: Arc<dyn Fn(XaeroP2PError) + Send + Sync + 'static>);
    /// Register a callback to be invoked when a topic is joined.
    fn on_join_topic<F>(&self, handler: Arc<dyn Fn(&str) + Send + Sync + 'static>);
    /// Register a callback to be invoked when a topic is left.
    fn on_leave_topic<F>(&self, handler: Arc<dyn Fn(&str) + Send + Sync + 'static>);
}
pub trait XaeroControlPlane {
    /// Announce updated peaks to the control plane.
    fn announce_peaks(&self, peaks: Vec<Peak>) -> Result<(), XaeroP2PError>;
    /// Store segment metadata in the control plane.
    fn publish_segment_meta(&self, seg_meta: SegmentMeta) -> Result<(), XaeroP2PError>;

    /// Register a callback to handle incoming peak announcements.
    fn on_peaks<F>(&self, handler: Arc<dyn Fn(&Peer, Vec<Peak>) + Send + Sync + 'static>);
    /// Register a callback to handle incoming segment metadata announcements.
    fn on_segment_meta<F>(&self, handler: Arc<dyn Fn(&Peer, SegmentMeta) + Send + Sync + 'static>);
}

pub trait XaeroDataPlane {
    fn send_event(&self, peer: &Peer, event: XaeroGossipEvent) -> Result<(), XaeroP2PError>;
    fn request_page(&self, peer: &Peer, page_request: PageRequest) -> Result<(), XaeroP2PError>;
    fn send_page(&self, peer: &Peer, page_response: PageResponse) -> Result<(), XaeroP2PError>;
    fn mark_done(&self, peer: &Peer) -> Result<(), XaeroP2PError>;

    /// Ask a peer to send the Merkle proof for a given leaf.
    fn request_proof(&self, peer: &Peer, req: ProofRequest) -> Result<(), XaeroP2PError>;
    /// Send a Merkle proof in response to a request.
    fn send_proof(&self, peer: &Peer, resp: ProofResponse) -> Result<(), XaeroP2PError>;

    /// Register a callback to handle incoming live events.
    fn on_event<F>(&self, handler: Arc<dyn Fn(&Peer, XaeroGossipEvent) + Send + Sync + 'static>);
    /// Register a callback to handle incoming page requests.
    fn on_page_request<F>(&self, handler: Arc<dyn Fn(&Peer, PageRequest) + Send + Sync + 'static>);
    /// Register a callback to handle incoming page responses.
    fn on_page_response<F>(
        &self,
        handler: Arc<dyn Fn(&Peer, PageResponse) + Send + Sync + 'static>,
    );
    /// Register a callback to handle incoming proof requests.
    fn on_proof_request<F>(
        &self,
        handler: Arc<dyn Fn(&Peer, ProofRequest) + Send + Sync + 'static>,
    );
    /// Register a callback to handle incoming proof responses.
    fn on_proof_response<F>(
        &self,
        handler: Arc<dyn Fn(&Peer, ProofResponse) + Send + Sync + 'static>,
    );
    /// Register a callback to handle sync completion notifications.
    fn on_sync_done<F>(&self, handler: Arc<dyn Fn(&Peer) + Send + Sync + 'static>);
}

/// XaeroTopic represents a topic in the Xaero P2P network.
pub struct XaeroTopic<T>
where
    T: XaeroPlane + Send + Sync + 'static,
{
    pub name: String,
    pub plane: T,
}

pub trait XaeroDiscoveryBehavior<T>
where
    T: XaeroPlane + Send + Sync + 'static,
{
    fn discover_peers(&self, topic: XaeroTopic<T>) -> Vec<Peer>;
    fn on_peer_discovered<F>(&self, handler: Arc<dyn Fn(&Peer) + Send + Sync + 'static>);
    fn on_peer_lost<F>(&self, handler: Arc<dyn Fn(&Peer) + Send + Sync + 'static>);
}
