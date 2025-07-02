// discovery.rs
use bytemuck::{Pod, Zeroable};
use xaeroid::XaeroProof;
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct XaeroDHTId {
    pub xaero_id_hash: [u8; 32],
    pub node_id_hash: [u8; 32],
}
impl XaeroDHTId {
    pub fn to_bytes(&self) -> &[u8] {
        bytemuck::bytes_of(self) // Zero-copy, respects your repr(C)
    }

    pub fn from_bytes(data: &[u8]) -> anyhow::Result<&Self> {
        Ok(bytemuck::from_bytes(data))
    }

    pub fn to_discovery_string(&self) -> String {
        hex::encode(self.to_bytes())
    }

    pub fn from_discovery_string(s: &str) -> anyhow::Result<Self> {
        let bytes = hex::decode(s)?;
        let record_ref = Self::from_bytes(&bytes)?;
        Ok(*record_ref) // Copy the record
    }
}
unsafe impl Pod for XaeroDHTId {}
unsafe impl Zeroable for XaeroDHTId {}

/// DHT record for XaeroID discovery and capability advertisement
#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct XaeroDHTRecord {
    pub id: XaeroDHTId,
    pub zk_proofs: [XaeroProof; 4],
    pub groups: [[u8; 32]; 10], // evict the oldest record.
    pub last_seen: u64,
    pub sync_state: XaeroSyncState,
    pub bootstrap_priority: u8, // Who to sync from first (0-255)
    pub sync_reliability_score: u8,
    pub _padding: [u8; 6], // Padding for alignment
}
unsafe impl Pod for XaeroDHTRecord {}
unsafe impl Zeroable for XaeroDHTRecord {}

impl XaeroDHTRecord {
    pub fn to_bytes(&self) -> &[u8] {
        bytemuck::bytes_of(self) // Zero-copy, respects your repr(C)
    }

    pub fn from_bytes(data: &[u8]) -> anyhow::Result<&Self> {
        Ok(bytemuck::from_bytes(data))
    }

    pub fn to_discovery_string(&self) -> String {
        hex::encode(self.to_bytes())
    }

    pub fn from_discovery_string(s: &str) -> anyhow::Result<Self> {
        let bytes = hex::decode(s)?;
        let record_ref = Self::from_bytes(&bytes)?;
        Ok(*record_ref) // Copy the record
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(64))]
pub struct XaeroSyncState {
    pub merkle_peaks: [[u8; 32]; 16],
    pub vector_clock: xaeroflux_core::vector_clock_actor::XaeroVectorClock,
    pub last_event_hash: [u8; 32],
    pub event_count: u64,
}
unsafe impl Pod for XaeroSyncState {}
unsafe impl Zeroable for XaeroSyncState {}

pub struct IrohDHTKeys;

impl IrohDHTKeys {
    /// Group membership key
    pub fn group_key(group_id: [u8; 32]) -> Vec<u8> {
        let mut key = b"xaeroflux/group/".to_vec();
        key.extend_from_slice(&group_id);
        key
    }

    /// ZK proof capability key
    pub fn zk_proof_key(xaero_id: [u8; 32], proof_hash: [u8; 32]) -> Vec<u8> {
        let mut key = b"xaeroflux/proof/".to_vec();
        key.extend_from_slice(&proof_hash);
        key
    }

    /// Local network key (same WiFi/VPN)
    pub fn local_network_key(network_fingerprint: [u8; 32]) -> Vec<u8> {
        let mut key = b"xaeroflux/local/".to_vec();
        key.extend_from_slice(&network_fingerprint);
        key
    }
}

/// DHT discovery operations
#[async_trait::async_trait]
pub trait DHTDiscovery {
    /// Publish your XaeroID and capabilities to DHT
    async fn publish_xaero_id(&self, groups: Vec<[u8; 32]>) -> anyhow::Result<()>;

    /// Discover XaeroIDs for a specific group
    async fn discover_group_members(&self, group_id: [u8; 32]) -> anyhow::Result<Vec<XaeroDHTRecord>>;

    /// Query DHT for specific capabilities (expressed via `XaeroProof`)
    async fn find_peers_with_zk_proof(&self, capability: XaeroProof) -> anyhow::Result<Vec<XaeroDHTRecord>>;

    /// Network-aware discovery for same WiFi/VPN optimization
    async fn discover_local_network_peers(&self) -> anyhow::Result<Vec<XaeroDHTId>>;

    /// Discover peers within network latency threshold (for relay selection)
    async fn discover_nearby_peers(&self, max_latency_ms: u32) -> anyhow::Result<Vec<XaeroDHTRecord>>;

    /// Update your sync state in DHT
    async fn update_sync_state(&self, sync_state: XaeroSyncState) -> anyhow::Result<()>;

    /// Check if peer can provide missing data (before attempting sync)
    async fn assess_sync_capability(&self, peer: &XaeroDHTRecord, my_sync_state: &XaeroSyncState) -> SyncAssessment;
}

#[derive(Debug)]
pub enum SyncAssessment {
    CanProvideAll,          // Peer has everything I'm missing
    CanProvidePartial(f32), // Peer has X% of what I need
    CannotProvide,          // Peer is behind me
    Concurrent,             // Vector clock conflict
}
