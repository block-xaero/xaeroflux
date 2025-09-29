use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Error, Result};
use bytemuck::{Pod, Zeroable};
use crc_fast::{CrcAlgorithm::Crc32IsoHdlc, checksum_file};
use iroh::{
    Endpoint, NodeAddr, PublicKey, SecretKey,
    discovery::UserData,
    endpoint::{Connection, ControlMsg::Ping, ReadExactError, RecvStream, SendStream},
};
use rusted_ring::{EventUtils, PooledEvent, Reader, RingBuffer, Writer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};
use tracing::{error, warn};
use xaeroflux_core::{
    date_time::emit_secs,
    event::{EventType, SYNC_EVENT_TYPE_BASE},
    hash::{blake_hash, blake_hash_slice},
    pool::XaeroPeerEvent,
    vector_clock::XaeroVectorClock,
};
use xaeroid::{
    XaeroID,
    cache::{XaeroIdCacheS, XaeroIdCacheXS, XaeroIdHotCache},
};

use crate::{
    aof::{
        ring_buffer_actor::AofState,
        storage::{
            format::MmrMeta,
            lmdb::{LmdbEnv, get_current_vector_clock, get_node_id_by_xaero_id, get_xaero_id_by_xaero_id_hash, put_xaero_id},
        },
    },
    networking::format::XaeroFileHeader,
    vector_clock_actor,
    vector_clock_actor::{VC_DELTA_OUTPUT_RING, VectorClockActor},
};

#[repr(C, align(64))]
#[derive(Debug, Copy, Clone)]
pub struct XaeroPing {
    pub sender_id: XaeroID,
}
unsafe impl Zeroable for XaeroPing {}
unsafe impl Pod for XaeroPing {}

pub struct XaeroQuicStream {
    pub name: String,
    pub stream_type: StreamType,
    pub send: SendStream,
    pub recv: RecvStream,
}

impl XaeroQuicStream {
    pub fn new(name: String, stream_type: StreamType, send: SendStream, recv: RecvStream) -> Self {
        Self { name, stream_type, send, recv }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StreamType {
    VectorClock = 0,
    Event = 1,
    Mmr = 2,
    Audio = 3,
    Video = 4,
    File = 5,
    Ping = 6,
}

pub struct XspConnMeta {
    pub connected_at: u64,
    pub alpn: String,
}

impl XspConnMeta {
    pub fn new(connected_at: u64, alpn: String) -> Self {
        Self { connected_at, alpn }
    }
}

// Add activity tracking for energy efficiency
pub struct XspConnection {
    pub our_xaero_id: [u8; 32],
    pub peer_id: [u8; 32],
    pub conn: Connection,
    pub meta: XspConnMeta,
    pub vector_clock_stream: XaeroQuicStream,
    pub event_stream: XaeroQuicStream,
    pub file_stream: XaeroQuicStream,
    pub ping_stream: XaeroQuicStream,
    // Energy efficiency fields
    pub last_activity: Instant,
    pub poll_interval: Duration,
    pub idle_counter: u32,
    lmdb_env: Arc<std::sync::Mutex<LmdbEnv>>,
}

impl XspConnection {
    pub async fn new(lmdb_env: Arc<std::sync::Mutex<LmdbEnv>>, our_xaero_id: [u8; 32], peer_id: [u8; 32], alpn: String, connection: Connection) -> Result<Self> {
        let (event_sender, event_receiver) = connection.open_bi().await?;
        let event_stream = XaeroQuicStream::new("event$".to_string(), StreamType::Event, event_sender, event_receiver);

        let (vc_sender, vc_receiver) = connection.open_bi().await?;
        let vector_clock_stream = XaeroQuicStream::new("vc$".to_string(), StreamType::VectorClock, vc_sender, vc_receiver);

        let (file_sender, file_receiver) = connection.open_bi().await?;
        let file_stream = XaeroQuicStream::new("file$".to_string(), StreamType::File, file_sender, file_receiver);

        let (ping_sender, ping_receiver) = connection.open_bi().await?;
        let ping_stream = XaeroQuicStream::new("pingpong$".to_string(), StreamType::Ping, ping_sender, ping_receiver);

        Ok(XspConnection {
            our_xaero_id,
            peer_id,
            conn: connection,
            meta: XspConnMeta::new(emit_secs(), alpn),
            vector_clock_stream,
            event_stream,
            file_stream,
            ping_stream,
            last_activity: Instant::now(),
            poll_interval: Duration::from_millis(1),
            idle_counter: 0,
            lmdb_env,
        })
    }

    pub async fn request_xaero_id(&mut self) -> Result<XaeroID> {
        let xaero_zero_id = XaeroID::zeroed();
        let full_xid = get_xaero_id_by_xaero_id_hash(&self.lmdb_env, self.our_xaero_id)?.unwrap_or(&xaero_zero_id);
        let xid_bytes = &XaeroPing { sender_id: *full_xid };
        let ping = bytemuck::bytes_of(xid_bytes);
        self.ping_stream.send.write_all(ping).await?;
        let mut response_bytes = vec![0u8; std::mem::size_of::<XaeroID>()];
        let res = tokio::time::timeout(Duration::from_secs(5), async {
            self.ping_stream.recv.read_exact(&mut response_bytes).await?;
            Ok::<Vec<u8>, Error>(response_bytes)
        })
        .await??;
        let xaero_id_of_peer = bytemuck::from_bytes::<XaeroID>(res.as_slice());
        Ok(*xaero_id_of_peer)
    }

    pub async fn send_vector_clock(&mut self, vc: &XaeroVectorClock) -> Result<(), Error> {
        let vc_bytes = bytemuck::bytes_of(vc);
        self.vector_clock_stream.send.write_all(vc_bytes).await?;
        self.vector_clock_stream.send.flush().await?;
        Ok(())
    }

    pub async fn send_event<const TSHIRT: usize>(&mut self, event: &PooledEvent<TSHIRT>) -> Result<(), Error> {
        let event_bytes = bytemuck::bytes_of(event);
        self.event_stream.send.write_all(event_bytes).await?;
        self.event_stream.send.flush().await?;
        Ok(())
    }

    pub async fn send_file<const CHUNK_SIZE: usize>(&mut self, location: String) -> Result<(), Error> {
        let loc = location.as_str();
        let file_data = tokio::fs::read(loc).await?;
        let crc32 = checksum_file(Crc32IsoHdlc, loc, Some(CHUNK_SIZE))?;

        let header = XaeroFileHeader {
            magic: *b"XAER",
            size: file_data.len() as u64,
            crc32,
        };
        self.file_stream.send.write_all(bytemuck::bytes_of(&header)).await?;
        self.file_stream.send.write_all(&file_data).await?;
        Ok(())
    }

    // Check if we should poll this connection
    pub fn should_poll(&self) -> bool {
        self.last_activity.elapsed() >= self.poll_interval
    }

    // Mark activity and reset interval
    pub fn mark_active(&mut self) {
        self.last_activity = Instant::now();
        self.idle_counter = 0;
        self.poll_interval = Duration::from_millis(1);
    }

    // Increase backoff for idle connection
    pub fn mark_idle(&mut self) {
        self.idle_counter += 1;
        // Exponential backoff: 1ms, 2ms, 4ms, ..., max 1s
        self.poll_interval = self.poll_interval.mul_f32(2.0).min(Duration::from_secs(1));
    }
}

#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct XaeroUserData {
    pub xaero_id_hash: [u8; 32],
    pub vector_clock_hash: [u8; 32],
    pub topic: [u8; 32],
}

unsafe impl Pod for XaeroUserData {}
unsafe impl Zeroable for XaeroUserData {}

pub struct P2pActor<const TSHIRT: usize, const RING_CAPACITY: usize> {
    our_xaero_id: [u8; 32],
    endpoint: Endpoint,
    active_peers: HashMap<[u8; 32], XspConnection>,
    running: Arc<AtomicBool>,
    aof_actor: Arc<AofState>,
    vector_clock_actor: VectorClockActor,
    xaero_id_cache: XaeroIdCacheS,
    node_id_to_xaero_id_mapping: HashMap<[u8; 32], [u8; 32]>,
    last_global_vc_sync: Instant,
    lmdb_env: Arc<std::sync::Mutex<LmdbEnv>>,
}

impl<const TSHIRT: usize, const RING_CAPACITY: usize> P2pActor<TSHIRT, RING_CAPACITY> {
    pub async fn new(
        ring_buffer: &'static RingBuffer<TSHIRT, RING_CAPACITY>,
        our_xaero_id: XaeroID,
        aof_actor: Arc<AofState>,
        lmdb_env: Arc<std::sync::Mutex<LmdbEnv>>,
    ) -> Result<(Self, Writer<TSHIRT, RING_CAPACITY>, Reader<TSHIRT, RING_CAPACITY>, Reader<1024, 1>)> {
        let xid = put_xaero_id(&lmdb_env, our_xaero_id)?;

        let vc_clock_res = get_current_vector_clock(&lmdb_env);
        let hash_to_vc_clock = match vc_clock_res {
            Ok(vc_clock) => {
                let vc = vc_clock.unwrap_or_else(XaeroVectorClock::zeroed);
                (blake_hash_slice(bytemuck::bytes_of(&vc)), vc)
            }
            Err(_) => {
                let default_vc = XaeroVectorClock::zeroed();
                (blake_hash_slice(bytemuck::bytes_of(&default_vc)), default_vc)
            }
        };

        let xaero_user_data = XaeroUserData {
            xaero_id_hash: xid,
            vector_clock_hash: hash_to_vc_clock.0,
            topic: our_xaero_id.credential.proofs.to_vec().first().expect("failed to get zk proof for topic").zk_proof,
        };

        let sk = SecretKey::from_bytes(&blake_hash_slice(&our_xaero_id.secret_key));
        let user_data_hex = hex::encode(bytemuck::bytes_of(&xaero_user_data));
        let user_data: UserData = user_data_hex.try_into().map_err(|_| anyhow::anyhow!("Invalid user data"))?;

        let endpoint = Endpoint::builder()
            .secret_key(sk)
            .discovery_dht()
            .user_data_for_discovery(user_data)
            .discovery_local_network()
            .alpns(vec![b"xsp-1.0".to_vec()])
            .bind()
            .await?;

        let actor = Self {
            our_xaero_id: blake_hash_slice(bytemuck::bytes_of(&our_xaero_id)),
            endpoint,
            active_peers: HashMap::new(),
            running: Arc::new(AtomicBool::new(false)),
            aof_actor,
            vector_clock_actor: VectorClockActor::new(lmdb_env.clone()),
            xaero_id_cache: XaeroIdCacheS::new(),
            node_id_to_xaero_id_mapping: HashMap::new(),
            last_global_vc_sync: Instant::now(),
            lmdb_env,
        };

        let writer = Writer::new(ring_buffer);
        let reader = Reader::new(ring_buffer);
        let vc_updates_reader = Reader::new(VC_DELTA_OUTPUT_RING.get().expect("failed to initialize vc delta ring buffer"));

        Ok((actor, writer, reader, vc_updates_reader))
    }

    pub async fn start(&mut self, mut writer: Writer<TSHIRT, RING_CAPACITY>, mut reader: Reader<TSHIRT, RING_CAPACITY>, mut vc_updates_reader: Reader<1024, 1>) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);
        tracing::info!("Starting energy-efficient P2P actor");

        let mut cleanup_timer = tokio::time::interval(Duration::from_secs(30));
        let mut vc_sync_timer = tokio::time::interval(Duration::from_secs(30));

        // Reusable buffers to reduce allocations
        let mut event_buf = vec![0u8; TSHIRT];
        let mut vc_buf = [0u8; 504];
        let mut ping_buf = [0u8; std::mem::size_of::<XaeroPing>()];

        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                // Handle incoming connections
                Some(incoming) = self.endpoint.accept() => {
                    if let Ok(connecting) = incoming.accept()
                        && let Ok(conn) = connecting.await {
                            let peer_node_id = conn.remote_node_id()?;
                            let peer_xaero_id = self.node_id_to_xaero_id(peer_node_id).await?;
                            let peer_id_hash = blake_hash_slice(bytemuck::bytes_of(&peer_xaero_id));

                            if let Ok(xsp_conn) = XspConnection::new(
                                self.lmdb_env.clone(),
                                self.our_xaero_id,
                                peer_id_hash,
                                "xsp-1.0".to_string(),
                                conn
                            ).await {
                                self.active_peers.insert(peer_id_hash, xsp_conn);
                                tracing::info!("New peer connected: {:?}", hex::encode(peer_id_hash));
                            }
                        }
                },

                // Broadcast events from ring buffer
                Some(event) = async { reader.next() } => {
                    for xsp_conn in self.active_peers.values_mut() {
                        // Non-blocking send
                        let _ = xsp_conn.send_event(&event).await;
                    }
                },

                _ = async {
                    let mut processed_any = false;

                    for (peer_id, xsp_conn) in &mut self.active_peers {
                        // Skip if not time to poll this peer yet
                        if !xsp_conn.should_poll() {
                            continue;
                        }

                        let mut had_activity = false;

                        // Try to receive event (non-blocking with short timeout)
                        if let Ok(Ok(_)) = tokio::time::timeout(
                            Duration::from_micros(100),
                            xsp_conn.event_stream.recv.read_exact(&mut event_buf)
                        ).await
                            && let Ok(event) = EventUtils::create_pooled_event::<TSHIRT>(&event_buf, 0) {
                                writer.add(event);
                                had_activity = true;
                                processed_any = true;
                                tracing::debug!("Received event from peer {:?}", hex::encode(peer_id));
                            }

                        // Try to receive vector clock
                        if let Ok(Ok(_)) = tokio::time::timeout(
                            Duration::from_micros(100),
                            xsp_conn.vector_clock_stream.recv.read_exact(&mut vc_buf)
                        ).await {
                            let vc = bytemuck::from_bytes::<XaeroVectorClock>(&vc_buf);
                            let event = EventUtils::create_pooled_event(
                                bytemuck::bytes_of(vc),
                                SYNC_EVENT_TYPE_BASE
                            ).expect("failed to create VC event");
                            self.vector_clock_actor.writer.add(event);
                            had_activity = true;
                            processed_any = true;
                            tracing::debug!("Received VC from peer {:?}", hex::encode(peer_id));
                        }

                        // Update activity tracking
                        if had_activity {
                            xsp_conn.mark_active();
                        } else {
                            xsp_conn.mark_idle();
                        }
                    }

                    // Dynamic sleep based on activity
                    if processed_any {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                } => {},

                // Periodic vector clock sync (every 30s)
                _ = vc_sync_timer.tick() => {
                    if let Ok(Some(our_vc)) = get_current_vector_clock(&self.lmdb_env) {
                        let vc_bytes = bytemuck::bytes_of(&our_vc);
                        for (peer_id, xsp_conn) in &mut self.active_peers {
                            if let Err(e) = xsp_conn.vector_clock_stream.send.write_all(vc_bytes).await {
                                tracing::warn!("Failed to send VC to {:?}: {:?}", hex::encode(peer_id), e);
                            }
                        }
                        tracing::info!("Broadcast vector clock to {} peers", self.active_peers.len());
                    }
                },

                // Cleanup dead connections
                _ = cleanup_timer.tick() => {
                    self.active_peers.retain(|peer_id, xsp_conn| {
                        if xsp_conn.conn.close_reason().is_some() {
                            tracing::info!("Removing dead peer: {:?}", hex::encode(peer_id));
                            false
                        } else {
                            true
                        }
                    });
                }
            }
        }
        Ok(())
    }

    async fn send_event_to_peer_static(xsp_conn: &mut XspConnection, event: PooledEvent<TSHIRT>) -> Result<()> {
        xsp_conn.send_event(&event).await
    }

    pub async fn connect_to_peer(&mut self, peer_xaero_id: XaeroID) -> Result<()> {
        let node_id = self.xaero_id_to_node_id(peer_xaero_id)?;
        let node_addr = NodeAddr::new(node_id);
        let conn = self.endpoint.connect(node_addr, b"xsp-1.0").await?;
        let peer_id_hash = blake_hash_slice(bytemuck::bytes_of(&peer_xaero_id));

        let xsp_conn = XspConnection::new(self.lmdb_env.clone(), self.our_xaero_id, peer_id_hash, "xsp-1.0".to_string(), conn).await?;
        self.active_peers.insert(peer_id_hash, xsp_conn);
        Ok(())
    }

    pub fn peer_count(&self) -> usize {
        self.active_peers.len()
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    fn xaero_id_to_node_id(&self, xaero_id: XaeroID) -> Result<iroh::NodeId> {
        let xaero_id_bytes = bytemuck::bytes_of(&xaero_id);
        let hash = blake_hash_slice(xaero_id_bytes);
        Ok(iroh::NodeId::from_bytes(&hash)?)
    }

    async fn node_id_to_xaero_id(&mut self, node_id: iroh::NodeId) -> Result<XaeroID, anyhow::Error> {
        let n_id = node_id.as_bytes();
        let xaero_id_hash_opt = self.node_id_to_xaero_id_mapping.get(n_id).cloned();

        match xaero_id_hash_opt {
            Some(xaero_id_hash) => {
                if let Ok(Some(xaero_id)) = get_xaero_id_by_xaero_id_hash(&self.aof_actor.env, xaero_id_hash) {
                    return Ok(*xaero_id);
                }
                if let Some(xaero_id_full) = self.xaero_id_cache.get(xaero_id_hash) {
                    return Ok(xaero_id_full);
                }
                if let Some(peer_conn) = self.active_peers.get_mut(n_id) {
                    let r_x_id = peer_conn.request_xaero_id().await?;
                    put_xaero_id(&self.aof_actor.env, r_x_id)?;
                    return Ok(r_x_id);
                }
                Err(anyhow::anyhow!("XaeroID not in storage and no active connection"))
            }
            None => {
                if let Some(peer_conn) = self.active_peers.get_mut(n_id) {
                    let r_x_id = peer_conn.request_xaero_id().await?;
                    let hash = blake_hash_slice(bytemuck::bytes_of(&r_x_id));
                    self.node_id_to_xaero_id_mapping.insert(*n_id, hash);
                    put_xaero_id(&self.aof_actor.env, r_x_id)?;
                    return Ok(r_x_id);
                }
                Err(anyhow::anyhow!("No XaeroID hash mapping and no active connection"))
            }
        }
    }
}
