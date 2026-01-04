// XaeroFlux - Simple Event Sync Engine with Peer Introduction
// Protocol: xsp-1.0 (XaeroFlux Sync Protocol v1.0)
//
// Features:
// - Event sync via gossip (events topic)
// - Peer discovery and introduction (discovery topic)
// - SQLite persistence for events AND peer tracking
// - peer_introduction broadcast for mesh formation
//
// Use as bootstrap server:
//   let xf = XaeroFlux::builder()
//       .discovery_key("cyan-dev")
//       .db_path("/opt/cyan/data/bootstrap.db")
//       .relay_url("https://quic.dev.cyan.blockxaero.io")
//       .build().await?;

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use iroh::{
    discovery::{dns::DnsDiscovery, mdns::MdnsDiscovery, pkarr::PkarrPublisher},
    protocol::Router,
    Endpoint, PublicKey, RelayMap, RelayMode, RelayUrl, SecretKey,
};
use iroh_gossip::{
    api::{Event as GossipEvent, GossipReceiver, GossipSender},
    proto::TopicId,
    Gossip,
};
use rand_chacha::rand_core::SeedableRng;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, RwLock};

// ---------- Core Event Type ----------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,      // Unique event ID (blake3 hash)
    pub payload: String, // Application-specific data
    pub source: String,  // Node ID that created this event
    pub ts: u64,         // Unix timestamp
}

// ---------- Configuration ----------
#[derive(Clone)]
pub struct XaeroFluxConfig {
    pub discovery_key: String,
    pub db_path: String,
    pub relay_url: Option<String>,
    pub bootstrap_peers: Vec<String>,
    pub use_n0_discovery: bool,
    pub use_mdns: bool,
}

impl Default for XaeroFluxConfig {
    fn default() -> Self {
        Self {
            discovery_key: "xaeroflux".to_string(),
            db_path: "xaeroflux.db".to_string(),
            relay_url: None,
            bootstrap_peers: vec![],
            use_n0_discovery: true,
            use_mdns: true,
        }
    }
}

// ---------- Builder Pattern ----------
pub struct XaeroFluxBuilder {
    config: XaeroFluxConfig,
}

impl XaeroFluxBuilder {
    pub fn new() -> Self {
        Self {
            config: XaeroFluxConfig::default(),
        }
    }

    pub fn discovery_key(mut self, key: impl Into<String>) -> Self {
        self.config.discovery_key = key.into();
        self
    }

    pub fn db_path(mut self, path: impl Into<String>) -> Self {
        self.config.db_path = path.into();
        self
    }

    pub fn relay_url(mut self, url: impl Into<String>) -> Self {
        self.config.relay_url = Some(url.into());
        self
    }

    pub fn bootstrap_peers(mut self, peers: Vec<String>) -> Self {
        self.config.bootstrap_peers = peers;
        self
    }

    pub fn bootstrap_peer(mut self, peer: impl Into<String>) -> Self {
        self.config.bootstrap_peers.push(peer.into());
        self
    }

    pub fn no_n0_discovery(mut self) -> Self {
        self.config.use_n0_discovery = false;
        self
    }

    pub fn no_mdns(mut self) -> Self {
        self.config.use_mdns = false;
        self
    }

    pub async fn build(self) -> Result<XaeroFlux> {
        XaeroFlux::from_config(self.config).await
    }
}

impl Default for XaeroFluxBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------- Public API ----------
pub struct XaeroFlux {
    pub event_tx: mpsc::UnboundedSender<Event>,
    pub event_rx: mpsc::UnboundedReceiver<Event>,
    pub discovery_key: String,
    pub node_id: String,
}

impl XaeroFlux {
    pub fn builder() -> XaeroFluxBuilder {
        XaeroFluxBuilder::new()
    }

    pub async fn new(discovery_key: String, db_path: String) -> Result<Self> {
        Self::builder()
            .discovery_key(discovery_key)
            .db_path(db_path)
            .build()
            .await
    }

    pub async fn new_with_bootstrap(
        discovery_key: String,
        db_path: String,
        bootstrap_peers: Vec<String>,
    ) -> Result<Self> {
        Self::builder()
            .discovery_key(discovery_key)
            .db_path(db_path)
            .bootstrap_peers(bootstrap_peers)
            .build()
            .await
    }

    async fn from_config(config: XaeroFluxConfig) -> Result<Self> {
        // Load or generate persistent node identity
        let key_path = std::path::Path::new(&config.db_path)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("node.key");

        let secret_key = if key_path.exists() {
            let bytes = std::fs::read(&key_path)?;
            let bytes: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid key file"))?;
            tracing::info!("🔑 Loaded existing secret key from {:?}", key_path);
            SecretKey::from_bytes(&bytes)
        } else {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed)
                .map_err(|e| anyhow::anyhow!("Failed to get random seed: {}", e))?;
            let mut rng = rand_chacha::ChaCha8Rng::from_seed(seed);
            let key = SecretKey::generate(&mut rng);

            if let Some(parent) = key_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&key_path, key.to_bytes())?;
            tracing::info!("🆕 Generated new secret key at {:?}", key_path);
            key
        };

        let node_id = secret_key.public().to_string();

        // Open database
        let db = Connection::open(&config.db_path)?;
        ensure_schema(&db)?;
        let db = Arc::new(Mutex::new(db));

        // Create channels
        let (app_event_tx, app_event_rx) = mpsc::unbounded_channel::<Event>();
        let (network_event_tx, network_event_rx) = mpsc::unbounded_channel::<Event>();
        let (sync_event_tx, sync_event_rx) = mpsc::unbounded_channel::<Event>();

        // Start storage actor
        let storage_actor = StorageActor::new(db.clone(), app_event_rx, network_event_tx);
        tokio::spawn(storage_actor.run());

        // Start network actor
        let network_actor = NetworkActor::new(
            secret_key,
            config.clone(),
            db.clone(),
            node_id.clone(),
            network_event_rx,
            sync_event_tx,
        )
            .await?;
        tokio::spawn(network_actor.run());

        Ok(Self {
            event_tx: app_event_tx,
            event_rx: sync_event_rx,
            discovery_key: config.discovery_key,
            node_id,
        })
    }
}

// ---------- Database Schema ----------
fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;

        -- Events table (for event sync to Iggy later)
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            source TEXT NOT NULL,
            ts INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
        CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);

        -- Peer tracking table (for peer introduction)
        CREATE TABLE IF NOT EXISTS group_peers (
            group_id TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            is_online INTEGER DEFAULT 1,
            PRIMARY KEY (group_id, peer_id)
        );
        CREATE INDEX IF NOT EXISTS idx_group_peers_group ON group_peers(group_id);
        CREATE INDEX IF NOT EXISTS idx_group_peers_online ON group_peers(is_online);
        "#,
    )?;
    Ok(())
}

// ---------- Storage Actor ----------
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

            let db = self.db.lock().await;
            match db.execute(
                "INSERT OR IGNORE INTO events (id, payload, source, ts) VALUES (?1, ?2, ?3, ?4)",
                params![event.id, event.payload, event.source, event.ts],
            ) {
                Ok(rows) => {
                    if rows > 0 {
                        tracing::info!("Event {} stored", event.id);
                        drop(db);

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

// ---------- Peer Tracker ----------
struct PeerTracker {
    db: Arc<Mutex<Connection>>,
    cache: RwLock<HashMap<String, Vec<String>>>,
}

impl PeerTracker {
    fn new(db: Arc<Mutex<Connection>>) -> Self {
        Self {
            db,
            cache: RwLock::new(HashMap::new()),
        }
    }

    async fn load_cache(&self) -> Result<()> {
        // Collect from DB first (don't hold stmt across await)
        let rows: Vec<(String, String)> = {
            let db = self.db.lock().await;
            let mut stmt = db.prepare("SELECT group_id, peer_id FROM group_peers WHERE is_online = 1")?;
            let mapped = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            mapped.collect::<Result<Vec<_>, _>>()?
        };

        // Now update cache
        let mut cache = self.cache.write().await;
        cache.clear();

        for (group_id, peer_id) in rows {
            cache
                .entry(group_id)
                .or_insert_with(Vec::new)
                .push(peer_id);
        }

        Ok(())
    }

    async fn upsert_peer(&self, group_id: &str, peer_id: &str) -> Result<bool> {
        let now = chrono::Utc::now().timestamp();
        let is_new: bool;

        {
            let db = self.db.lock().await;

            let exists: bool = db
                .query_row(
                    "SELECT 1 FROM group_peers WHERE group_id = ?1 AND peer_id = ?2",
                    params![group_id, peer_id],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            if exists {
                db.execute(
                    "UPDATE group_peers SET last_seen = ?1, is_online = 1
                     WHERE group_id = ?2 AND peer_id = ?3",
                    params![now, group_id, peer_id],
                )?;
                is_new = false;
            } else {
                db.execute(
                    "INSERT INTO group_peers (group_id, peer_id, first_seen, last_seen, is_online)
                     VALUES (?1, ?2, ?3, ?3, 1)",
                    params![group_id, peer_id, now],
                )?;
                is_new = true;
            }
        }

        // Update cache
        {
            let mut cache = self.cache.write().await;
            let peers = cache.entry(group_id.to_string()).or_insert_with(Vec::new);
            if !peers.contains(&peer_id.to_string()) {
                peers.push(peer_id.to_string());
            }
        }

        Ok(is_new)
    }

    async fn mark_offline(&self, peer_id: &str) -> Result<Vec<String>> {
        let now = chrono::Utc::now().timestamp();
        let affected_groups: Vec<String>;

        {
            let db = self.db.lock().await;

            // Collect affected groups first
            let mut stmt =
                db.prepare("SELECT group_id FROM group_peers WHERE peer_id = ?1 AND is_online = 1")?;
            let rows = stmt.query_map(params![peer_id], |row| row.get::<_, String>(0))?;
            affected_groups = rows.collect::<Result<Vec<_>, _>>()?;

            db.execute(
                "UPDATE group_peers SET is_online = 0, last_seen = ?1 WHERE peer_id = ?2",
                params![now, peer_id],
            )?;
        }

        // Update cache (after releasing db lock)
        {
            let mut cache = self.cache.write().await;
            for peers in cache.values_mut() {
                peers.retain(|p| p != peer_id);
            }
        }

        Ok(affected_groups)
    }

    async fn get_peers(&self, group_id: &str) -> Vec<String> {
        let cache = self.cache.read().await;
        cache.get(group_id).cloned().unwrap_or_default()
    }

    async fn get_active_groups(&self) -> Vec<String> {
        let cache = self.cache.read().await;
        cache
            .iter()
            .filter(|(_, peers)| !peers.is_empty())
            .map(|(k, _)| k.clone())
            .collect()
    }

    async fn prune_stale(&self, max_age: Duration) -> Result<usize> {
        let cutoff = chrono::Utc::now().timestamp() - max_age.as_secs() as i64;

        let count = {
            let db = self.db.lock().await;
            db.execute(
                "UPDATE group_peers SET is_online = 0 WHERE last_seen < ?1 AND is_online = 1",
                params![cutoff],
            )?
        };

        self.load_cache().await?;
        Ok(count)
    }

    async fn stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        let groups = cache.len();
        let peers: usize = cache.values().map(|v| v.len()).sum();
        (groups, peers)
    }
}

// ---------- Network Actor ----------
struct NetworkActor {
    node_id: String,
    db: Arc<Mutex<Connection>>,
    #[allow(dead_code)]
    endpoint: Endpoint,
    gossip: Arc<Gossip>,
    #[allow(dead_code)]
    router: Router,
    gossip_sender: GossipSender,
    gossip_receiver: GossipReceiver,
    outbound_rx: mpsc::UnboundedReceiver<Event>,
    inbound_tx: mpsc::UnboundedSender<Event>,
    peer_tracker: Arc<PeerTracker>,
    discovery_key: String,
}

impl NetworkActor {
    async fn new(
        secret_key: SecretKey,
        config: XaeroFluxConfig,
        db: Arc<Mutex<Connection>>,
        node_id: String,
        outbound_rx: mpsc::UnboundedReceiver<Event>,
        inbound_tx: mpsc::UnboundedSender<Event>,
    ) -> Result<Self> {
        // Configure relay mode
        let relay_mode = if let Some(ref url_str) = config.relay_url {
            match RelayUrl::from_str(url_str) {
                Ok(url) => {
                    tracing::info!("🌐 Using custom relay: {}", url);
                    RelayMode::Custom(RelayMap::from(url))
                }
                Err(e) => {
                    tracing::warn!("⚠️ Invalid relay URL '{}': {}, using default", url_str, e);
                    RelayMode::Default
                }
            }
        } else {
            tracing::info!("🌐 Using default Iroh relays");
            RelayMode::Default
        };

        // Build endpoint
        let mut builder = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![iroh_gossip::ALPN.to_vec(), b"xsp-1.0".to_vec()])
            .relay_mode(relay_mode);

        if config.use_n0_discovery {
            builder = builder
                .discovery(PkarrPublisher::n0_dns())
                .discovery(DnsDiscovery::n0_dns());
        }

        if config.use_mdns {
            builder = builder.discovery(MdnsDiscovery::builder());
        }

        let endpoint = builder.bind().await?;
        let endpoint_id = endpoint.id();
        tracing::info!("Node ID: {}", endpoint_id);

        // Setup gossip
        let gossip = Arc::new(Gossip::builder().spawn(endpoint.clone()));

        let router = Router::builder(endpoint.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        // Create topic IDs
        let discovery_topic_id = TopicId::from_bytes(
            blake3::hash(format!("cyan/discovery/{}", config.discovery_key).as_bytes()).as_bytes()
                [..32]
                .try_into()?,
        );

        let events_topic_id = TopicId::from_bytes(
            blake3::hash(format!("cyan/events/{}", config.discovery_key).as_bytes()).as_bytes()
                [..32]
                .try_into()?,
        );

        // Parse bootstrap peers
        let bootstrap_ids: Vec<PublicKey> = config
            .bootstrap_peers
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();

        if !bootstrap_ids.is_empty() {
            tracing::info!("📡 Bootstrapping with {} peers", bootstrap_ids.len());
        }

        // Join events topic
        let mut events_topic = gossip
            .subscribe(events_topic_id, bootstrap_ids.clone())
            .await?;

        // Join discovery topic
        let mut discovery_topic = gossip
            .subscribe(discovery_topic_id, bootstrap_ids)
            .await?;

        tokio::time::sleep(Duration::from_millis(500)).await;
        tokio::time::timeout(Duration::from_secs(2), events_topic.joined())
            .await
            .ok();

        tracing::info!(
            "Subscribed to topics for discovery key: {}",
            config.discovery_key
        );

        let (gossip_sender, gossip_receiver) = events_topic.split();

        // Initialize peer tracker
        let peer_tracker = Arc::new(PeerTracker::new(db.clone()));
        peer_tracker.load_cache().await?;

        let (groups, peers) = peer_tracker.stats().await;
        tracing::info!("📊 Loaded {} groups with {} online peers", groups, peers);

        // Spawn discovery listener task
        let peer_tracker_clone = peer_tracker.clone();
        let discovery_key_clone = config.discovery_key.clone();
        let endpoint_id_clone = endpoint_id;

        tokio::spawn(async move {
            tracing::info!("Peer discovery task started");

            let mut announce_interval = tokio::time::interval(Duration::from_secs(30));
            let mut prune_interval = tokio::time::interval(Duration::from_secs(300));

            loop {
                tokio::select! {
                    _ = announce_interval.tick() => {
                        // Announce presence
                        let announce = endpoint_id_clone.to_string();
                        if let Err(e) = discovery_topic.broadcast(Bytes::from(announce)).await {
                            tracing::warn!("Failed to announce presence: {}", e);
                        }

                        // Re-broadcast peer introductions for all active groups
                        let groups = peer_tracker_clone.get_active_groups().await;
                        for group_id in groups {
                            let peers = peer_tracker_clone.get_peers(&group_id).await;
                            if peers.len() > 1 {
                                let intro_msg = serde_json::json!({
                                    "msg_type": "peer_introduction",
                                    "group_id": group_id,
                                    "peers": peers,
                                });

                                let _ = discovery_topic.broadcast(
                                    Bytes::from(intro_msg.to_string())
                                ).await;

                                tracing::debug!(
                                    "🔄 Re-broadcast peer_introduction for {} ({} peers)",
                                    &group_id[..16.min(group_id.len())],
                                    peers.len()
                                );
                            }
                        }
                    }

                    _ = prune_interval.tick() => {
                        match peer_tracker_clone.prune_stale(Duration::from_secs(300)).await {
                            Ok(count) if count > 0 => {
                                tracing::info!("🗑️ Pruned {} stale peers", count);
                            }
                            Err(e) => tracing::warn!("Failed to prune: {}", e),
                            _ => {}
                        }
                    }

                    Some(event_result) = discovery_topic.next() => {
                        match event_result {
                            Ok(GossipEvent::Received(msg)) => {
                                let sender_peer = msg.delivered_from;
                                let sender_str = sender_peer.to_string();

                                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&msg.content) {
                                    if let Some(msg_type) = json.get("msg_type").and_then(|v| v.as_str()) {
                                        if msg_type == "groups_exchange" {
                                            let from_node = json.get("node_id")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("unknown");

                                            tracing::info!(
                                                "📩 groups_exchange from {}",
                                                &from_node[..16.min(from_node.len())]
                                            );

                                            if let Some(groups) = json.get("local_groups").and_then(|v| v.as_array()) {
                                                let mut introductions: Vec<(String, Vec<String>)> = Vec::new();

                                                for group in groups {
                                                    if let Some(gid) = group.as_str() {
                                                        match peer_tracker_clone.upsert_peer(gid, &sender_str).await {
                                                            Ok(is_new) => {
                                                                let peers = peer_tracker_clone.get_peers(gid).await;

                                                                if is_new {
                                                                    tracing::info!(
                                                                        "📝 New peer {} for group {} ({} total)",
                                                                        &sender_str[..16],
                                                                        &gid[..16.min(gid.len())],
                                                                        peers.len()
                                                                    );
                                                                }

                                                                if peers.len() > 1 {
                                                                    introductions.push((gid.to_string(), peers));
                                                                }
                                                            }
                                                            Err(e) => tracing::warn!("Failed to track peer: {}", e),
                                                        }
                                                    }
                                                }

                                                // Broadcast peer introductions
                                                for (group_id, peers) in introductions {
                                                    let intro_msg = serde_json::json!({
                                                        "msg_type": "peer_introduction",
                                                        "group_id": group_id,
                                                        "peers": peers,
                                                    });

                                                    tracing::info!(
                                                        "📢 Broadcasting peer_introduction for {} ({} peers)",
                                                        &group_id[..16.min(group_id.len())],
                                                        peers.len()
                                                    );

                                                    let _ = discovery_topic.broadcast(
                                                        Bytes::from(intro_msg.to_string())
                                                    ).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            Ok(GossipEvent::NeighborUp(peer)) => {
                                tracing::info!("🟢 Discovery neighbor up: {}", &peer.to_string()[..16]);
                            }

                            Ok(GossipEvent::NeighborDown(peer)) => {
                                let peer_str = peer.to_string();
                                tracing::info!("🔴 Discovery neighbor down: {}", &peer_str[..16]);

                                match peer_tracker_clone.mark_offline(&peer_str).await {
                                    Ok(affected_groups) => {
                                        for group_id in affected_groups {
                                            let peers = peer_tracker_clone.get_peers(&group_id).await;
                                            if peers.len() > 1 {
                                                let intro_msg = serde_json::json!({
                                                    "msg_type": "peer_introduction",
                                                    "group_id": group_id,
                                                    "peers": peers,
                                                });

                                                tracing::info!(
                                                    "📢 Re-broadcast peer_introduction for {} (peer left)",
                                                    &group_id[..16.min(group_id.len())]
                                                );

                                                let _ = discovery_topic.broadcast(
                                                    Bytes::from(intro_msg.to_string())
                                                ).await;
                                            }
                                        }
                                    }
                                    Err(e) => tracing::warn!("Failed to mark offline: {}", e),
                                }
                            }

                            _ => {}
                        }
                    }

                    else => break,
                }
            }

            tracing::info!("Peer discovery task for '{}' terminated", discovery_key_clone);
        });

        Ok(Self {
            node_id,
            db,
            endpoint,
            gossip,
            router,
            gossip_sender,
            gossip_receiver,
            outbound_rx,
            inbound_tx,
            peer_tracker,
            discovery_key: config.discovery_key,
        })
    }

    async fn run(mut self) {
        tracing::info!("NetworkActor started");

        // Heartbeat task
        let peer_tracker = self.peer_tracker.clone();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let (groups, peers) = peer_tracker.stats().await;
                println!(
                    "💓 Heartbeat - Node ID: {} | {} groups, {} peers",
                    &node_id[..16],
                    groups,
                    peers
                );
            }
        });

        loop {
            tokio::select! {
                Some(event_result) = self.gossip_receiver.next() => {
                    match event_result {
                        Ok(GossipEvent::Received(msg)) => {
                            match serde_json::from_slice::<Event>(&msg.content) {
                                Ok(event) => {
                                    if event.source != self.node_id {
                                        tracing::info!("Received event from network: {}", event.id);

                                        let db = self.db.lock().await;
                                        match db.execute(
                                            "INSERT OR IGNORE INTO events (id, payload, source, ts) VALUES (?1, ?2, ?3, ?4)",
                                            params![event.id, event.payload, event.source, event.ts],
                                        ) {
                                            Ok(rows) => {
                                                if rows > 0 {
                                                    tracing::info!("Synced event {} from {}", event.id, event.source);
                                                    drop(db);

                                                    if let Err(e) = self.inbound_tx.send(event) {
                                                        tracing::error!("Failed to forward synced event: {}", e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!("Failed to store synced event: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!("Non-event gossip message: {}", e);
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
                            tracing::warn!("Gossip receiver lagged");
                        }
                        Err(e) => {
                            tracing::warn!("Error in gossip receiver: {}", e);
                        }
                    }
                }

                Some(event) = self.outbound_rx.recv() => {
                    match serde_json::to_vec(&event) {
                        Ok(bytes) => {
                            if let Err(e) = self.gossip_sender.broadcast(Bytes::from(bytes)).await {
                                tracing::error!("Failed to broadcast event {}: {}", event.id, e);
                            } else {
                                tracing::info!("Broadcasted event {} to gossip network", event.id);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to serialize event {}: {}", event.id, e);
                        }
                    }
                }

                else => {
                    tracing::warn!("NetworkActor channel closed, exiting");
                    break;
                }
            }
        }

        tracing::warn!("NetworkActor stopped");
    }
}

// ---------- Helper ----------
pub fn generate_event_id(payload: &str, source: &str, ts: u64) -> String {
    let input = format!("{}{}{}", payload, source, ts);
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn basic_integration_sanity() {
        let xf = XaeroFlux::new("test".to_string(), ":memory:".to_string())
            .await
            .expect("failed to create XaeroFlux");
        assert!(!xf.node_id.is_empty());
    }

    #[tokio::test]
    async fn builder_with_custom_relay() {
        let xf = XaeroFlux::builder()
            .discovery_key("test")
            .db_path(":memory:")
            .relay_url("https://quic.dev.cyan.blockxaero.io")
            .no_n0_discovery()
            .build()
            .await
            .expect("failed to create XaeroFlux with custom relay");
        assert!(!xf.node_id.is_empty());
    }
}