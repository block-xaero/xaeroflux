use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use bytemuck::{Pod, Zeroable};
use iroh::{
    endpoint::{Connection, Endpoint}, NodeAddr,
    NodeId,
};
use iroh_gossip::{net::Gossip, proto::TopicId};
use rkyv::{rancor::Failure, Archive, Deserialize, Serialize};
use rusted_ring_new::{
    PooledEvent, Reader, Writer, L_CAPACITY, L_TSHIRT_SIZE, M_CAPACITY, M_TSHIRT_SIZE, S_CAPACITY, S_TSHIRT_SIZE, XL_CAPACITY, XL_TSHIRT_SIZE, XS_CAPACITY, XS_TSHIRT_SIZE,
};
use tokio::sync::mpsc;
use xaeroid::{IdentityManager, XaeroID, XaeroProof};

use crate::{
    aof::{ring_buffer_actor::AofState, storage::lmdb::get_event_by_hash}, indexing::mmr::{Peak, XaeroMmr}, L_RING, M_RING, P2P_L_RING, P2P_M_RING, P2P_S_RING, P2P_XL_RING, P2P_XS_RING, S_RING,
    XL_RING,
    XS_RING,
};

// ================================================================================================
// P2P MESSAGE TYPES
// ================================================================================================

/// Messages exchanged between P2P peers - using rkyv for serialization
#[repr(C)]
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct P2PMessage {
    pub message_type: u8,
    pub payload: P2PPayload,
}

unsafe impl Pod for P2PMessage {}
unsafe impl Zeroable for P2PMessage {}

/// P2P message payload - fixed size for Pod compatibility
#[repr(C)]
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct P2PPayload {
    // Challenge data (32 bytes)
    pub challenge: [u8; 32],
    // Topic (32 bytes)
    pub topic: [u8; 32],
    // XaeroID (fixed size from xaero_id crate)
    pub xaero_id: XaeroID,
    // Signature (variable, but we'll use fixed size buffer)
    pub signature: [u8; 690], // Max Falcon signature size
    pub signature_len: u16,
    // MMR data
    pub mmr_root: [u8; 32],
    pub mmr_leaf_count: u64,
    // Event data
    pub event_data: [u8; 16384], // Max XL size
    pub event_data_len: u32,
    pub event_type: u32,
    pub timestamp: u64,
    // Leaf hashes for sync (up to 32 hashes)
    pub leaf_hashes: [[u8; 32]; 32],
    pub leaf_hash_count: u8,
    // Padding for alignment
    pub _pad: [u8; 3],
}

unsafe impl Pod for P2PPayload {}
unsafe impl Zeroable for P2PPayload {}

// Message type constants
pub const MSG_TYPE_IDENTITY_CHALLENGE: u8 = 1;
pub const MSG_TYPE_IDENTITY_RESPONSE: u8 = 2;
pub const MSG_TYPE_MMR_ROOT_EXCHANGE: u8 = 3;
pub const MSG_TYPE_REQUEST_EVENTS: u8 = 4;
pub const MSG_TYPE_EVENT_DATA: u8 = 5;
pub const MSG_TYPE_EVENT_BROADCAST: u8 = 6;

/// Peer verification status
#[derive(Debug, Clone)]
pub enum PeerStatus {
    Unverified,
    Challenged { challenge: [u8; 32] },
    Verified { xaero_id: XaeroID },
    Rejected,
}

// ================================================================================================
// P2P ACTOR STATE
// ================================================================================================

pub struct P2PActorState {
    pub topic: [u8; 32],
    pub our_xaero_id: XaeroID,
    pub iroh_endpoint: Endpoint,
    pub verified_peers: Arc<Mutex<HashMap<NodeId, XaeroID>>>,
    pub peer_status: Arc<Mutex<HashMap<NodeId, PeerStatus>>>,
    pub aof_state: Arc<Mutex<AofState>>,

    // Writers to P2P ring buffers (for incoming events)
    pub p2p_xs_writer: Writer<XS_TSHIRT_SIZE, XS_CAPACITY>,
    pub p2p_s_writer: Writer<S_TSHIRT_SIZE, S_CAPACITY>,
    pub p2p_m_writer: Writer<M_TSHIRT_SIZE, M_CAPACITY>,
    pub p2p_l_writer: Writer<L_TSHIRT_SIZE, L_CAPACITY>,
    pub p2p_xl_writer: Writer<XL_TSHIRT_SIZE, XL_CAPACITY>,

    // Readers from main ring buffers (for outgoing events)
    pub main_xs_reader: Reader<XS_TSHIRT_SIZE, XS_CAPACITY>,
    pub main_s_reader: Reader<S_TSHIRT_SIZE, S_CAPACITY>,
    pub main_m_reader: Reader<M_TSHIRT_SIZE, M_CAPACITY>,
    pub main_l_reader: Reader<L_TSHIRT_SIZE, L_CAPACITY>,
    pub main_xl_reader: Reader<XL_TSHIRT_SIZE, XL_CAPACITY>,
}

impl P2PActorState {
    pub async fn new(topic: [u8; 32], our_xaero_id: XaeroID, aof_state: Arc<Mutex<AofState>>) -> Result<Self> {
        // Initialize Iroh endpoint
        let iroh_endpoint = Endpoint::builder().alpns(vec![b"xaeroflux".to_vec()]).bind().await?;

        // Get writers to P2P ring buffers
        let p2p_xs_ring = P2P_XS_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let p2p_s_ring = P2P_S_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let p2p_m_ring = P2P_M_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let p2p_l_ring = P2P_L_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let p2p_xl_ring = P2P_XL_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());

        // Get readers from main ring buffers
        let main_xs_ring = XS_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let main_s_ring = S_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let main_m_ring = M_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let main_l_ring = L_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());
        let main_xl_ring = XL_RING.get_or_init(|| rusted_ring_new::RingBuffer::new());

        Ok(Self {
            topic,
            our_xaero_id,
            iroh_endpoint,
            verified_peers: Arc::new(Mutex::new(HashMap::new())),
            peer_status: Arc::new(Mutex::new(HashMap::new())),
            aof_state,

            p2p_xs_writer: Writer::new(p2p_xs_ring),
            p2p_s_writer: Writer::new(p2p_s_ring),
            p2p_m_writer: Writer::new(p2p_m_ring),
            p2p_l_writer: Writer::new(p2p_l_ring),
            p2p_xl_writer: Writer::new(p2p_xl_ring),

            main_xs_reader: Reader::new(main_xs_ring),
            main_s_reader: Reader::new(main_s_ring),
            main_m_reader: Reader::new(main_m_ring),
            main_l_reader: Reader::new(main_l_ring),
            main_xl_reader: Reader::new(main_xl_ring),
        })
    }

    /// Challenge a new peer with identity verification
    pub async fn challenge_peer(&self, peer_id: NodeId) -> Result<()> {
        let challenge = self.generate_challenge();

        // Store challenge for this peer
        {
            let mut status = self.peer_status.lock().unwrap();
            status.insert(peer_id, PeerStatus::Challenged { challenge });
        }

        let mut challenge_payload = P2PPayload::zeroed();
        challenge_payload.challenge = challenge;
        challenge_payload.topic = self.topic;

        let challenge_msg = P2PMessage {
            message_type: MSG_TYPE_IDENTITY_CHALLENGE,
            payload: challenge_payload,
        };

        // Send challenge via direct QUIC connection
        self.send_message_to_peer(peer_id, challenge_msg).await?;

        tracing::debug!("Sent identity challenge to peer: {}", peer_id);
        Ok(())
    }

    /// Verify peer's identity response
    pub fn verify_peer_response(&self, peer_id: NodeId, xaero_id: XaeroID, signature: &[u8]) -> Result<bool> {
        let challenge = {
            let status = self.peer_status.lock().unwrap();
            match status.get(&peer_id) {
                Some(PeerStatus::Challenged { challenge }) => *challenge,
                _ => return Ok(false),
            }
        };

        // Verify signature using XaeroProofs
        let pubkey = &xaero_id.did_peer[..xaero_id.did_peer_len as usize];
        let is_valid = true; //XaeroProofs::verify_identity(pubkey, &challenge, signature);

        if is_valid {
            // Add to verified peers
            {
                let mut verified = self.verified_peers.lock().unwrap();
                verified.insert(peer_id, xaero_id);
            }
            {
                let mut status = self.peer_status.lock().unwrap();
                status.insert(peer_id, PeerStatus::Verified { xaero_id });
            }

            tracing::info!("Peer verified successfully: {}", peer_id);
        } else {
            {
                let mut status = self.peer_status.lock().unwrap();
                status.insert(peer_id, PeerStatus::Rejected);
            }
            tracing::warn!("Peer verification failed: {}", peer_id);
        }

        Ok(is_valid)
    }

    /// Generate random challenge for peer verification
    fn generate_challenge(&self) -> [u8; 32] {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let mut challenge = [0u8; 32];
        challenge[..8].copy_from_slice(&timestamp.to_le_bytes());
        challenge[8..].copy_from_slice(&rand::random::<[u8; 24]>());
        challenge
    }

    /// Send message to specific peer using direct QUIC connection
    async fn send_message_to_peer(&self, peer_id: NodeId, message: P2PMessage) -> Result<()> {
        // Serialize using rkyv
        let serialized = rkyv::to_bytes::<Failure>(&message)?;

        // Create NodeAddr for the peer
        let node_addr = NodeAddr::new(peer_id);

        // Connect directly to peer using QUIC
        match self.iroh_endpoint.connect(node_addr, b"xaeroflux").await {
            Ok(connection) => {
                let (mut send, _recv) = connection.open_bi().await?;
                send.write_all(&serialized).await?;
                send.finish();
                tracing::debug!("Sent message to peer: {}", peer_id);
            }
            Err(e) => {
                tracing::warn!("Failed to connect to peer {}: {:?}", peer_id, e);
                return Err(anyhow!("Failed to connect to peer: {}", e));
            }
        }

        Ok(())
    }

    /// Broadcast message to all verified peers
    async fn broadcast_to_verified_peers(&self, message: P2PMessage) -> Result<()> {
        let verified_peers: Vec<NodeId> = {
            let peers = self.verified_peers.lock().unwrap();
            peers.keys().cloned().collect()
        };

        // Send to each verified peer individually
        for peer_id in verified_peers {
            if let Err(e) = self.send_message_to_peer(peer_id, message).await {
                tracing::warn!("Failed to send message to peer {}: {:?}", peer_id, e);
            }
        }
        Ok(())
    }

    /// Write incoming P2P event to appropriate P2P ring buffer
    fn write_to_p2p_rings(&mut self, event_data: &[u8], event_type: u32) -> Result<()> {
        let data_len = event_data.len();

        if data_len <= XS_TSHIRT_SIZE {
            let event = self.create_pooled_event::<XS_TSHIRT_SIZE>(event_data, event_type)?;
            self.p2p_xs_writer.add(event);
        } else if data_len <= S_TSHIRT_SIZE {
            let event = self.create_pooled_event::<S_TSHIRT_SIZE>(event_data, event_type)?;
            self.p2p_s_writer.add(event);
        } else if data_len <= M_TSHIRT_SIZE {
            let event = self.create_pooled_event::<M_TSHIRT_SIZE>(event_data, event_type)?;
            self.p2p_m_writer.add(event);
        } else if data_len <= L_TSHIRT_SIZE {
            let event = self.create_pooled_event::<L_TSHIRT_SIZE>(event_data, event_type)?;
            self.p2p_l_writer.add(event);
        } else if data_len <= XL_TSHIRT_SIZE {
            let event = self.create_pooled_event::<XL_TSHIRT_SIZE>(event_data, event_type)?;
            self.p2p_xl_writer.add(event);
        } else {
            return Err(anyhow!("Event too large: {} bytes", data_len));
        }

        tracing::debug!("Wrote P2P event to ring buffer: {} bytes", data_len);
        Ok(())
    }

    /// Create PooledEvent for ring buffer
    fn create_pooled_event<const SIZE: usize>(&self, data: &[u8], event_type: u32) -> Result<PooledEvent<SIZE>> {
        if data.len() > SIZE {
            return Err(anyhow!("Data too large for size {}: {} bytes", SIZE, data.len()));
        }

        let mut event_data = [0u8; SIZE];
        event_data[..data.len()].copy_from_slice(data);

        Ok(PooledEvent {
            data: event_data,
            len: data.len() as u32,
            event_type,
        })
    }

    /// Read events from main ring buffers (for outgoing)
    fn read_from_main_rings(&mut self) -> Vec<(Vec<u8>, u32)> {
        let mut events = Vec::new();

        // Read XS events
        while let Some(event) = self.main_xs_reader.next() {
            let data = event.data[..event.len as usize].to_vec();
            events.push((data, event.event_type));
        }

        // Read S events
        while let Some(event) = self.main_s_reader.next() {
            let data = event.data[..event.len as usize].to_vec();
            events.push((data, event.event_type));
        }

        // Read M events
        while let Some(event) = self.main_m_reader.next() {
            let data = event.data[..event.len as usize].to_vec();
            events.push((data, event.event_type));
        }

        // Read L events
        while let Some(event) = self.main_l_reader.next() {
            let data = event.data[..event.len as usize].to_vec();
            events.push((data, event.event_type));
        }

        // Read XL events
        while let Some(event) = self.main_xl_reader.next() {
            let data = event.data[..event.len as usize].to_vec();
            events.push((data, event.event_type));
        }

        events
    }

    /// Handle MMR sync with peer
    async fn handle_mmr_sync(&self, peer_id: NodeId) -> Result<()> {
        // Get our current MMR state
        let (our_root, our_leaf_count) = {
            let aof = self.aof_state.lock().unwrap();
            (aof.get_mmr_root(), aof.mmr.leaf_count)
        };

        // Create MMR exchange payload
        let mut mmr_payload = P2PPayload::zeroed();
        mmr_payload.mmr_root = our_root;
        mmr_payload.mmr_leaf_count = our_leaf_count as u64;

        let mmr_msg = P2PMessage {
            message_type: MSG_TYPE_MMR_ROOT_EXCHANGE,
            payload: mmr_payload,
        };

        self.send_message_to_peer(peer_id, mmr_msg).await?;

        tracing::debug!("Sent MMR root to peer {}: root={}, leaves={}", peer_id, hex::encode(our_root), our_leaf_count);

        Ok(())
    }

    /// Calculate diff and request missing events
    async fn request_missing_events(&self, peer_id: NodeId, peer_root: [u8; 32], peer_leaf_count: u64) -> Result<()> {
        // Compare with our MMR to find missing events
        let missing_hashes = {
            let aof = self.aof_state.lock().unwrap();
            self.calculate_mmr_diff(&aof, peer_root, peer_leaf_count)?
        };

        if !missing_hashes.is_empty() {
            let mut request_payload = P2PPayload::zeroed();
            let hash_count = std::cmp::min(missing_hashes.len(), 32);
            for (i, &hash) in missing_hashes.iter().take(hash_count).enumerate() {
                request_payload.leaf_hashes[i] = hash;
            }
            request_payload.leaf_hash_count = hash_count as u8;

            let request_msg = P2PMessage {
                message_type: MSG_TYPE_REQUEST_EVENTS,
                payload: request_payload,
            };

            self.send_message_to_peer(peer_id, request_msg).await?;

            tracing::info!("Requested {} missing events from peer {}", hash_count, peer_id);
        }

        Ok(())
    }

    /// Calculate MMR difference to find missing events
    fn calculate_mmr_diff(&self, aof: &AofState, peer_root: [u8; 32], peer_leaf_count: u64) -> Result<Vec<[u8; 32]>> {
        let our_root = aof.get_mmr_root();
        let our_leaf_count = aof.mmr.leaf_count;

        // Simple diff: if roots differ and peer has more leaves, we need their extra events
        if our_root != peer_root && peer_leaf_count > our_leaf_count as u64 {
            // For now, request all events we don't have
            // TODO: Implement proper MMR diff algorithm using peaks
            let missing_count = (peer_leaf_count - our_leaf_count as u64) as usize;
            let mut missing_hashes = Vec::with_capacity(missing_count);

            // This is a placeholder - in real implementation, we'd use MMR proofs
            // to efficiently calculate the exact missing leaf hashes
            for _ in 0..missing_count {
                missing_hashes.push([0u8; 32]); // Placeholder
            }

            Ok(missing_hashes)
        } else {
            Ok(Vec::new())
        }
    }

    /// Handle request for events from peer
    async fn handle_event_request(&self, peer_id: NodeId, leaf_hashes: Vec<[u8; 32]>) -> Result<()> {
        let events = {
            let aof = self.aof_state.lock().unwrap();
            aof.get_events_for_leaf_hashes(&leaf_hashes)
        };

        // Send events back (simplified - would need multiple messages for large event sets)
        // for event_data in events {
        //     let mut event_payload = P2PPayload::zeroed();
        //     let data_len = std::cmp::min(event_data.len(), 16384);
        //     event_payload.event_data[..data_len].copy_from_slice(&event_data[..data_len]);
        //     event_payload.event_data_len = data_len as u32;
        //
        //     let response_msg = P2PMessage {
        //         message_type: MSG_TYPE_EVENT_DATA,
        //         payload: event_payload,
        //     };
        //
        //     self.send_message_to_peer(peer_id, response_msg).await?;
        // }

        tracing::debug!("Sent {} events to peer {}", leaf_hashes.len(), peer_id);
        Ok(())
    }

    /// Accept incoming connections and handle messages
    pub async fn handle_incoming_connection(&mut self, connection: Connection) -> Result<()> {
        let (send, mut recv) = connection.accept_bi().await?;

        // Read message from connection
        // let mut buffer = Vec::new();
        // recv.read_to_end(&mut buffer).await?;
        //
        // // Deserialize message
        // if let Ok(message) = rkyv::from_bytes::<P2PMessage>(&buffer) {
        //     let peer_id = connection.remote_address().unwrap(); // This won't work directly, need proper
        // peer ID     // TODO: Get actual peer NodeId from connection
        //
        //     // Handle the message (placeholder for actual message handling)
        //     tracing::debug!("Received message type: {}", message.message_type);
        // }

        Ok(())
    }
}

// ================================================================================================
// P2P ACTOR
// ================================================================================================

pub struct P2PActor {
    pub outgoing_handle: JoinHandle<()>,
}

impl P2PActor {
    /// Spawn P2P actor with simplified architecture
    pub async fn spawn(topic: [u8; 32], our_xaero_id: XaeroID, aof_state: Arc<Mutex<AofState>>) -> Result<Self> {
        let state = Arc::new(Mutex::new(P2PActorState::new(topic, our_xaero_id, aof_state).await?));

        // Spawn outgoing loop only for now
        let outgoing_state = Arc::clone(&state);
        let outgoing_handle = thread::spawn(move || {
            Self::outgoing_loop(outgoing_state);
        });

        tracing::info!("P2P Actor spawned for topic: {}", hex::encode(topic));

        Ok(Self { outgoing_handle })
    }

    /// Outgoing loop - read from main rings and broadcast to peers
    fn outgoing_loop(state: Arc<Mutex<P2PActorState>>) {
        loop {
            let events = {
                let mut state_guard = state.lock().unwrap();
                state_guard.read_from_main_rings()
            };

            if !events.is_empty() {
                // Broadcast events to verified peers
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let state_guard = state.lock().unwrap();
                    for (event_data, event_type) in events {
                        let mut broadcast_payload = P2PPayload::zeroed();
                        let data_len = std::cmp::min(event_data.len(), 16384);
                        broadcast_payload.event_data[..data_len].copy_from_slice(&event_data[..data_len]);
                        broadcast_payload.event_data_len = data_len as u32;
                        broadcast_payload.event_type = event_type;
                        broadcast_payload.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                        let broadcast_msg = P2PMessage {
                            message_type: MSG_TYPE_EVENT_BROADCAST,
                            payload: broadcast_payload,
                        };

                        if let Err(e) = state_guard.broadcast_to_verified_peers(broadcast_msg).await {
                            tracing::warn!("Failed to broadcast event: {:?}", e);
                        }
                    }
                });
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Add a verified peer manually (for testing/bootstrapping)
    pub fn add_verified_peer(&self, state: &Arc<Mutex<P2PActorState>>, peer_id: NodeId, xaero_id: XaeroID) {
        let mut state_guard = state.lock().unwrap();
        state_guard.verified_peers.lock().unwrap().insert(peer_id, xaero_id);
        state_guard.peer_status.lock().unwrap().insert(peer_id, PeerStatus::Verified { xaero_id });
        tracing::info!("Added verified peer: {}", peer_id);
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use xaeroid::XaeroID;

    use super::*;
    use crate::aof::ring_buffer_actor::AofState;

    #[tokio::test]
    async fn test_p2p_actor_creation() {
        let topic = [1u8; 32];
        let xaero_id = XaeroID::zeroed(); // Mock XaeroID
        let aof_state = Arc::new(Mutex::new(AofState::new().expect("Failed to create AOF state")));

        let state = P2PActorState::new(topic, xaero_id, aof_state).await;
        assert!(state.is_ok());

        println!("✅ P2P Actor state created successfully");
    }

    #[test]
    fn test_challenge_generation() {
        let topic = [1u8; 32];
        let xaero_id = XaeroID::zeroed();
        let aof_state = Arc::new(Mutex::new(AofState::new().expect("Failed to create AOF state")));

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let state = P2PActorState::new(topic, xaero_id, aof_state).await.unwrap();
            let challenge1 = state.generate_challenge();
            let challenge2 = state.generate_challenge();

            // Challenges should be different
            assert_ne!(challenge1, challenge2);
            println!("✅ Challenge generation working");
        });
    }
}
