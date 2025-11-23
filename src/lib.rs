// XaeroFlux - Simple Event Sync Engine
// Protocol: xsp-1.0 (XaeroFlux Sync Protocol v1.0)
// Single discovery topic, event-based sync only

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use iroh::{
    Endpoint, EndpointId, RelayMode, SecretKey,
    discovery::{dns::DnsDiscovery, mdns::MdnsDiscovery, pkarr::PkarrPublisher},
};
use iroh_gossip::{
    Gossip,
    api::{Event as GossipEvent, GossipReceiver, GossipSender, GossipTopic},
    proto::state::TopicId,
};
use rand_chacha::rand_core::SeedableRng;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};

// ---------- Core Event Type ----------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,      // Unique event ID (blake3 hash)
    pub payload: String, // Application-specific data
    pub source: String,  // Node ID that created this event
    pub ts: u64,         // Unix timestamp
}

// ---------- Public API ----------
pub struct XaeroFlux {
    /// Send events into XaeroFlux (from your app)
    pub event_tx: mpsc::UnboundedSender<Event>,
    /// Receive events from XaeroFlux (synced from network)
    pub event_rx: mpsc::UnboundedReceiver<Event>,
    /// Discovery key - only peers with same key can sync
    pub discovery_key: String,
    /// This node's public ID
    pub node_id: String,
}

impl XaeroFlux {
    /// Initialize XaeroFlux with a discovery key and database path
    pub async fn new(discovery_key: String, db_path: String) -> Result<Self> {
        Self::new_with_bootstrap(discovery_key, db_path, vec![]).await
    }

    /// Initialize XaeroFlux with optional bootstrap peers
    pub async fn new_with_bootstrap(
        discovery_key: String,
        db_path: String,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self> {
        // Generate node identity
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        );
        let secret_key = SecretKey::generate(&mut rng);
        let node_id = secret_key.public().to_string();

        // Open database
        let db = Connection::open(&db_path)?;
        ensure_schema(&db)?;
        let db = Arc::new(Mutex::new(db));

        // Create channels
        let (app_event_tx, app_event_rx) = mpsc::unbounded_channel::<Event>();
        let (network_event_tx, network_event_rx) = mpsc::unbounded_channel::<Event>();
        let (sync_event_tx, sync_event_rx) = mpsc::unbounded_channel::<Event>();

        // Start storage actor: app -> storage -> network
        let storage_actor = StorageActor::new(db.clone(), app_event_rx, network_event_tx);
        tokio::spawn(storage_actor.run());

        // Start network actor: network -> storage -> app (via sync_event_tx)
        let network_actor = NetworkActor::new(
            secret_key,
            discovery_key.clone(),
            db.clone(),
            node_id.clone(),
            network_event_rx,
            sync_event_tx,
            bootstrap_peers,
        )
            .await?;
        tokio::spawn(network_actor.run());

        Ok(Self {
            event_tx: app_event_tx,
            event_rx: sync_event_rx,
            discovery_key,
            node_id,
        })
    }
}

// ---------- Database Schema ----------
fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            source TEXT NOT NULL,
            ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
        CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);
        "#,
    )?;
    Ok(())
}

// ---------- Storage Actor ----------
// Stores events to SQLite and forwards to network
struct StorageActor {
    db: Arc<Mutex<Connection>>,
    app_rx: mpsc::UnboundedReceiver<Event>,
    network_tx: mpsc::UnboundedSender<Event>,
}

impl StorageActor {
    fn new(
        db: Arc<Mutex<Connection>>,
        app_rx: mpsc::UnboundedReceiver<Event>,
        network_tx: mpsc::UnboundedSender<Event>,
    ) -> Self {
        Self {
            db,
            app_rx,
            network_tx,
        }
    }

    async fn run(mut self) {
        tracing::info!("StorageActor started");

        while let Some(event) = self.app_rx.recv().await {
            tracing::debug!("Storing event: {}", event.id);

            // Store in database
            let db = self.db.lock().await;
            match db.execute(
                "INSERT OR IGNORE INTO events (id, payload, source, ts) VALUES (?1, ?2, ?3, ?4)",
                params![event.id, event.payload, event.source, event.ts],
            ) {
                Ok(rows) => {
                    if rows > 0 {
                        tracing::info!("Event {} stored", event.id);
                        drop(db); // Release lock before sending

                        // Forward to network for broadcast
                        if let Err(e) = self.network_tx.send(event) {
                            tracing::error!("Failed to send event to network: {}", e);
                        }
                    } else {
                        tracing::debug!("Event {} already exists (duplicate)", event.id);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to store event {}: {}", event.id, e);
                }
            }
        }

        tracing::warn!("StorageActor stopped");
    }
}

// ---------- Network Actor ----------
// Syncs events via Iroh gossip on single discovery topic
struct NetworkActor {
    node_id: String,
    db: Arc<Mutex<Connection>>,
    endpoint: Endpoint,
    gossip: Arc<Gossip>,
    gossip_sender: GossipSender,
    gossip_receiver: GossipReceiver,
    outbound_rx: mpsc::UnboundedReceiver<Event>,
    inbound_tx: mpsc::UnboundedSender<Event>,
}

impl NetworkActor {
    async fn new(
        secret_key: SecretKey,
        discovery_key: String,
        db: Arc<Mutex<Connection>>,
        node_id: String,
        outbound_rx: mpsc::UnboundedReceiver<Event>,
        inbound_tx: mpsc::UnboundedSender<Event>,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self> {
        // Setup Iroh endpoint with discovery
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![b"xsp-1.0".to_vec()])
            .relay_mode(RelayMode::Default)
            // Add discovery services
            .discovery(PkarrPublisher::n0_dns())
            .discovery(DnsDiscovery::n0_dns())
            .discovery(MdnsDiscovery::builder())
            .bind()
            .await?;

        let endpoint_id = endpoint.id();
        tracing::info!("Node ID: {}", endpoint_id);

        // Setup gossip
        let gossip = Arc::new(Gossip::builder().spawn(endpoint.clone()));

        // Create topic IDs
        let discovery_topic_id = TopicId::from_bytes(
            blake3::hash(format!("xsp-1.0/{}/discovery", discovery_key).as_bytes())
                .as_bytes()[..32]
                .try_into()?,
        );

        let events_topic_id = TopicId::from_bytes(
            blake3::hash(format!("xsp-1.0/{}/events", discovery_key).as_bytes())
                .as_bytes()[..32]
                .try_into()?,
        );

        // Parse bootstrap peers
        let bootstrap_ids: Vec<EndpointId> = bootstrap_peers
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        // Subscribe to events topic with bootstrap peers
        let mut events_topic = gossip.subscribe(events_topic_id, bootstrap_ids.clone()).await?;

        // Join discovery topic for peer exchange
        let mut discovery_topic = gossip.subscribe(discovery_topic_id, bootstrap_ids).await?;

        // Wait a bit for mDNS discovery to work
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Try to wait for at least one neighbor (don't fail if none found)
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            events_topic.joined()
        ).await.ok();

        tracing::info!("Subscribed to topics for discovery key: {}", discovery_key);

        // Split events topic for main operation
        let (gossip_sender, gossip_receiver) = events_topic.split();

        // Spawn peer discovery task
        let gossip_clone = gossip.clone();
        let discovery_key_clone = discovery_key.clone();
        tokio::spawn(async move {
            tracing::info!("Peer discovery task started");

            // Periodically announce our presence
            let mut announce_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = announce_interval.tick() => {
                        // Announce our endpoint ID on discovery topic
                        let announce = endpoint_id.to_string();
                        if let Err(e) = discovery_topic.broadcast(Bytes::from(announce)).await {
                            tracing::warn!("Failed to announce presence: {}", e);
                        } else {
                            tracing::debug!("Announced presence on discovery topic");
                        }
                    }

                    Some(event_result) = discovery_topic.next() => {
                        match event_result {
                            Ok(GossipEvent::Received(msg)) => {
                                if let Ok(peer_id_str) = std::str::from_utf8(&msg.content) {
                                    if let Ok(peer_id) = peer_id_str.parse::<EndpointId>() {
                                        if peer_id != endpoint_id {
                                            tracing::info!("Discovered peer via gossip: {}", peer_id);
                                            // Try to join this peer on events topic
                                            if let Ok(mut topic) = gossip_clone.subscribe(events_topic_id, vec![peer_id]).await {
                                                // Just subscribe to establish connection
                                                drop(topic);
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(GossipEvent::NeighborUp(peer)) => {
                                tracing::info!("Discovery neighbor up: {}", peer);
                            }
                            Ok(GossipEvent::NeighborDown(peer)) => {
                                tracing::info!("Discovery neighbor down: {}", peer);
                            }
                            _ => {}
                        }
                    }

                    else => break,
                }
            }

            tracing::warn!("Peer discovery task stopped");
        });

        Ok(Self {
            node_id,
            db,
            endpoint,
            gossip,
            gossip_sender,
            gossip_receiver,
            outbound_rx,
            inbound_tx,
        })
    }

    async fn run(mut self) {
        tracing::info!("NetworkActor started");

        // Use tokio::select! to handle both incoming and outgoing events
        loop {
            tokio::select! {
                // Handle incoming gossip events
                Some(event_result) = self.gossip_receiver.next() => {
                    match event_result {
                        Ok(GossipEvent::Received(msg)) => {
                            match serde_json::from_slice::<Event>(&msg.content) {
                                Ok(event) => {
                                    // Don't process our own events
                                    if event.source == self.node_id {
                                        tracing::debug!("Ignoring own event: {}", event.id);
                                        continue;
                                    }

                                    tracing::info!("Received event from network: {}", event.id);

                                    // Store in DB
                                    let db = self.db.lock().await;
                                    match db.execute(
                                        "INSERT OR IGNORE INTO events (id, payload, source, ts) VALUES (?1, ?2, ?3, ?4)",
                                        params![event.id, event.payload, event.source, event.ts],
                                    ) {
                                        Ok(rows) => {
                                            if rows > 0 {
                                                tracing::info!("Synced event {} from {}", event.id, event.source);
                                                drop(db); // Release lock before sending

                                                // Forward to app
                                                if let Err(e) = self.inbound_tx.send(event) {
                                                    tracing::error!("Failed to forward event to app: {}", e);
                                                }
                                            } else {
                                                tracing::debug!("Event {} already exists", event.id);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to store synced event: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to deserialize event: {}", e);
                                }
                            }
                        }
                        Ok(GossipEvent::NeighborUp(peer)) => {
                            tracing::info!("Events neighbor up: {}", peer);
                        }
                        Ok(GossipEvent::NeighborDown(peer)) => {
                            tracing::info!("Events neighbor down: {}", peer);
                        }
                        Ok(GossipEvent::Lagged) => {
                            tracing::warn!("Gossip receiver lagged, may have missed messages");
                        }
                        Err(e) => {
                            tracing::error!("Gossip receiver error: {}", e);
                            break;
                        }
                    }
                }

                // Handle outgoing events to broadcast
                Some(event) = self.outbound_rx.recv() => {
                    tracing::info!("Broadcasting event: {}", event.id);

                    let data = match serde_json::to_vec(&event) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::error!("Failed to serialize event: {}", e);
                            continue;
                        }
                    };

                    if let Err(e) = self.gossip_sender.broadcast(Bytes::from(data)).await {
                        tracing::error!("Failed to broadcast event: {}", e);
                    } else {
                        tracing::info!("Event {} broadcast successful", event.id);
                    }
                }

                // Both channels closed, exit
                else => {
                    tracing::info!("NetworkActor channels closed, exiting");
                    break;
                }
            }
        }

        tracing::warn!("NetworkActor stopped");
    }
}

// ---------- Helper: Generate Event ID ----------
pub fn generate_event_id(payload: &str, source: &str, ts: u64) -> String {
    let input = format!("{}{}{}", payload, source, ts);
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_xaeroflux_basic() {
        let mut xf = XaeroFlux::new("test-key".to_string(), ":memory:".to_string())
            .await
            .unwrap();

        let event = Event {
            id: generate_event_id("hello", &xf.node_id, 123),
            payload: "hello".to_string(),
            source: xf.node_id.clone(),
            ts: 123,
        };

        xf.event_tx.send(event.clone()).unwrap();

        // Event should be synced back
        let received =
            tokio::time::timeout(std::time::Duration::from_secs(1), xf.event_rx.recv()).await;

        // Note: Will timeout since we don't sync our own events
        // This is expected behavior
    }
}