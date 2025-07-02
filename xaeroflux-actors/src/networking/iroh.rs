// iroh.rs
use std::collections::HashSet;

use bytemuck::Zeroable;
use futures::{FutureExt, StreamExt, TryFutureExt};
use hex::FromHexError;
use iroh::{
    Endpoint, SecretKey,
    discovery::{Discovery, UserData},
    endpoint::BindError,
};
use iroh_gossip::net::Gossip;
use rkyv::rancor::Failure;
use xaeroflux_core::{
    P2P_RUNTIME,
    date_time::emit_secs,
    hash::{blake_hash, blake_hash_slice},
    vector_clock_actor::XaeroVectorClock,
};
use xaeroid::{XaeroID, XaeroProof};

use crate::networking::discovery::{DHTDiscovery, SyncAssessment, XaeroDHTId, XaeroDHTRecord, XaeroSyncState};

#[derive(Debug)]
pub struct IrohState {
    pub xaero_id: XaeroID,
    pub endpoint: Endpoint,
    pub gossiper: Gossip,
}

impl IrohState {
    pub async fn new(xid: &XaeroID) -> Self {
        let falcon_secret = &xid.secret_key[..1280]; // Falcon-512 secret key is 1280 bytes
        let ed25519_seed = blake_hash_slice(&falcon_secret);
        let secret_key = SecretKey::from_bytes(&ed25519_seed);
        let public_key = secret_key.public();
        let res_endpoint = iroh::Endpoint::builder()
            .alpns(vec![b"xaeroflux".to_vec()])
            .secret_key(secret_key)
            .discovery_n0()
            .discovery_dht()
            .discovery_local_network()
            .bind()
            .await;
        let endpoint = match res_endpoint {
            Ok(endpoint) => endpoint,
            Err(e) => {
                panic!("failed due to {e:?}")
            }
        };
        let xaero_id_hash = blake_hash_slice(bytemuck::bytes_of(xid));
        let node_id_hash = *endpoint.node_id().as_bytes(); // de-ref copy
        let x_dht_record = XaeroDHTRecord {
            id: XaeroDHTId {
                xaero_id_hash,
                node_id_hash,
            },
            zk_proofs: xid.credential.proofs,
            groups: [[0u8; 32]; 10], // TODO: at most and atleast median
            last_seen: emit_secs(),
            sync_state: XaeroSyncState {
                merkle_peaks: [[0u8; 32]; 16],            // TODO: look at current mmr peaks
                vector_clock: XaeroVectorClock::zeroed(), // TODO: look at clock for root?
                last_event_hash: [0u8; 32],               // todo: fill in later
                event_count: 0,                           // todo:: ? pick from segment meta?
            },
            bootstrap_priority: 0,
            sync_reliability_score: 0,
            _padding: [0u8; 6],
        };

        let x_dht_record_bytes = bytemuck::bytes_of(&x_dht_record);
        let user_data = hex::encode(x_dht_record_bytes[..64].to_vec());
        endpoint.set_user_data_for_discovery(Some(user_data.parse().expect("cannot parse")));

        let gossip = Gossip::builder().spawn(endpoint.clone());
        Self {
            xaero_id: *xid,
            endpoint,
            gossiper: gossip,
        }
    }

    /// Update discovery data with new sync state
    pub fn update_discovery_data(&self, sync_state: &XaeroSyncState) {
        let xaero_id_hash = blake_hash_slice(bytemuck::bytes_of(&self.xaero_id));
        let node_id_hash = *self.endpoint.node_id().as_bytes();

        let updated_record = XaeroDHTRecord {
            id: XaeroDHTId {
                xaero_id_hash,
                node_id_hash,
            },
            zk_proofs: self.xaero_id.credential.proofs,
            groups: [[0u8; 32]; 10], // TODO: update with actual groups
            last_seen: emit_secs(),
            sync_state: *sync_state,
            bootstrap_priority: 0,
            sync_reliability_score: 0,
            _padding: [0u8; 6],
        };

        let record_bytes = bytemuck::bytes_of(&updated_record);
        let user_data = hex::encode(record_bytes);
        self.endpoint
            .set_user_data_for_discovery(Some(user_data.parse().expect("Cannot parse")));

        tracing::debug!("Updated discovery data with new sync state");
    }
}

pub struct XaeroDHTDiscovery {
    pub dht_record: XaeroDHTRecord,
    pub iroh_state: IrohState,
}

impl XaeroDHTDiscovery {
    pub fn new(iroh_state: IrohState) -> Self {
        let xaero_id_hash = blake_hash_slice(bytemuck::bytes_of(&iroh_state.xaero_id));
        let node_id_hash = *iroh_state.endpoint.node_id().as_bytes();

        let dht_record = XaeroDHTRecord {
            id: XaeroDHTId {
                xaero_id_hash,
                node_id_hash,
            },
            zk_proofs: iroh_state.xaero_id.credential.proofs,
            groups: [[0u8; 32]; 10],
            last_seen: emit_secs(),
            sync_state: XaeroSyncState {
                merkle_peaks: [[0u8; 32]; 16],
                vector_clock: XaeroVectorClock::zeroed(),
                last_event_hash: [0u8; 32],
                event_count: 0,
            },
            bootstrap_priority: 0,
            sync_reliability_score: 0,
            _padding: [0u8; 6],
        };

        Self { dht_record, iroh_state }
    }
}

#[async_trait::async_trait]
impl DHTDiscovery for XaeroDHTDiscovery {
    async fn publish_xaero_id(&self, groups: Vec<[u8; 32]>) -> anyhow::Result<()> {
        // Update groups in our record and republish
        let mut updated_record = self.dht_record;
        for (i, group) in groups.iter().enumerate() {
            if i < 10 {
                updated_record.groups[i] = *group;
            }
        }
        updated_record.last_seen = emit_secs();

        let record_bytes = bytemuck::bytes_of(&updated_record);
        let user_data = hex::encode(record_bytes);
        self.iroh_state
            .endpoint
            .set_user_data_for_discovery(Some(user_data.parse().expect("cannot parse")));

        tracing::info!("Published XaeroID with {} groups", groups.len());
        Ok(())
    }

    async fn discover_group_members(&self, group_id: [u8; 32]) -> anyhow::Result<Vec<XaeroDHTRecord>> {
        // let peers = self.discover_local_network_peers().await?;
        // let group_members: Vec<XaeroDHTRecord> = peers
        //     .into_iter()
        //     .filter(|peer| peer.groups.contains(&group_id))
        //     .collect();
        //
        // tracing::info!(
        //     "Found {} members in group {:?}",
        //     group_members.len(),
        //     hex::encode(&group_id[..8])
        // );
        // Ok(group_members)
        todo!()
    }

    async fn find_peers_with_zk_proof(&self, capability: XaeroProof) -> anyhow::Result<Vec<XaeroDHTRecord>> {
        // let peers = self.discover_local_network_peers().await?;
        // let capable_peers: Vec<XaeroDHTRecord> = peers
        //     .into_iter()
        //     .filter(|peer| true /* peer.zk_proofs.contains(&capability) */)
        //     .collect();
        // // TODO: add compare
        // tracing::info!("Found {} peers with required ZK proof capability", capable_peers.len());
        // Ok(capable_peers)
        todo!()
    }

    async fn discover_local_network_peers(&self) -> anyhow::Result<Vec<XaeroDHTId>> {
        let mut peers = Vec::new();
        let mut seen_nodes = HashSet::new();
        let mut discovery_stream = self.iroh_state.endpoint.discovery_stream();

        tracing::info!("🔍 Starting local network discovery...");

        // Set a timeout for discovery
        let timeout = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(discovery_result) = discovery_stream.next().await {
                match discovery_result {
                    Ok(discovered_peer) => {
                        let node_id = discovered_peer.node_id();

                        // Skip if we've already seen this node
                        if seen_nodes.contains(&node_id) {
                            continue;
                        }
                        seen_nodes.insert(node_id);

                        // Skip our own node
                        if node_id == self.iroh_state.endpoint.node_id() {
                            continue;
                        }

                        tracing::debug!("📡 Discovered peer: {}", node_id);

                        match discovered_peer.user_data() {
                            Some(user_data) => {
                                let data_str = user_data.to_string();
                                match hex::decode(&data_str) {
                                    Ok(bytes) => match XaeroDHTId::from_bytes(&bytes) {
                                        Ok(peer_record) => {
                                            tracing::info!(
                                                "✅ Found XaeroFlux peer: {} with XaeroID: {}",
                                                node_id,
                                                hex::encode(&peer_record.xaero_id_hash[..8])
                                            );
                                            peers.push(*peer_record);
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "❌ Failed to parse XaeroDHTRecord from peer {}: {}",
                                                node_id,
                                                e
                                            );
                                        }
                                    },
                                    Err(e) => {
                                        tracing::debug!(
                                            "⚠️ Peer {} has non-hex user data (not XaeroFlux): {}",
                                            node_id,
                                            e
                                        );
                                    }
                                }
                            }
                            None => {
                                tracing::debug!("⚠️ Peer {} has no user data (not XaeroFlux)", node_id);
                            }
                        }
                    }
                    Err(_) => {
                        tracing::warn!("⚠️ Discovery stream lagged, some peers may have been missed");
                        continue;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        match timeout.await {
            Ok(_) => {
                tracing::info!("🎉 Discovery completed, found {} XaeroFlux peers", peers.len());
            }
            Err(_) => {
                tracing::info!("⏰ Discovery timeout reached, found {} XaeroFlux peers", peers.len());
            }
        }

        Ok(peers)
    }

    async fn discover_nearby_peers(&self, max_latency_ms: u32) -> anyhow::Result<Vec<XaeroDHTRecord>> {
        // For now, just return local network peers
        // TODO: Add actual latency measurement
        // tracing::warn!("discover_nearby_peers not fully implemented, returning local network peers");
        // self.discover_local_network_peers().await
        todo!()
    }

    async fn update_sync_state(&self, sync_state: XaeroSyncState) -> anyhow::Result<()> {
        self.iroh_state.update_discovery_data(&sync_state);
        tracing::info!("Updated sync state in discovery");
        Ok(())
    }

    async fn assess_sync_capability(&self, peer: &XaeroDHTRecord, my_sync_state: &XaeroSyncState) -> SyncAssessment {
        // Basic vector clock comparison
        if peer.sync_state.event_count > my_sync_state.event_count {
            SyncAssessment::CanProvideAll
        } else if peer.sync_state.event_count == my_sync_state.event_count {
            if peer.sync_state.last_event_hash == my_sync_state.last_event_hash {
                SyncAssessment::CannotProvide
            } else {
                SyncAssessment::Concurrent
            }
        } else {
            SyncAssessment::CannotProvide
        }
    }
}
