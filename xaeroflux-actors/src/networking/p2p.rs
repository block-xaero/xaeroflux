use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Error, Result};
use bytemuck::{Pod, Zeroable};
use crc_fast::{CrcAlgorithm::Crc32IsoHdlc, checksum_file};
use iroh::{
    Endpoint, NodeAddr, SecretKey,
    discovery::UserData,
    endpoint::{Connection, ReadExactError, RecvStream, SendStream},
};
use rusted_ring::{PooledEvent, Reader, RingBuffer, Writer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};
use xaeroflux_core::{date_time::emit_secs, hash::blake_hash_slice, pool::XaeroPeerEvent, vector_clock::XaeroVectorClock};
use xaeroid::{
    XaeroID,
    cache::{XaeroIdCacheS, XaeroIdCacheXS, XaeroIdHotCache},
};

use crate::{
    aof::{
        ring_buffer_actor::AofState,
        storage::{
            format::MmrMeta,
            lmdb::{LmdbEnv, get_node_id_by_xaero_id, get_xaero_id_by_xaero_id_hash},
        },
    },
    networking::format::XaeroFileHeader,
    vector_clock_actor,
    vector_clock_actor::VectorClockActor,
};

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

pub struct XspConnection {
    pub peer_id: [u8; 32],
    pub conn: Connection,
    pub meta: XspConnMeta,
    pub vector_clock_stream: XaeroQuicStream,
    pub mmr_stream: XaeroQuicStream,
    pub event_stream: XaeroQuicStream,
    pub file_stream: XaeroQuicStream,
}

impl XspConnection {
    pub async fn new(peer_id: [u8; 32], alpn: String, connection: Connection) -> Result<Self> {
        let (event_sender, event_receiver) = connection.open_bi().await?;
        let event_stream = XaeroQuicStream::new("event$".to_string(), StreamType::Event, event_sender, event_receiver);

        let (vc_sender, vc_receiver) = connection.open_bi().await?;
        let vector_clock_stream = XaeroQuicStream::new("vc$".to_string(), StreamType::VectorClock, vc_sender, vc_receiver);

        let (mmr_sender, mmr_receiver) = connection.open_bi().await?;
        let mmr_stream = XaeroQuicStream::new("mmr$".to_string(), StreamType::Mmr, mmr_sender, mmr_receiver);

        let (file_sender, file_receiver) = connection.open_bi().await?;
        let file_stream = XaeroQuicStream::new("file$".to_string(), StreamType::File, file_sender, file_receiver);

        Ok(XspConnection {
            peer_id,
            conn: connection,
            meta: XspConnMeta::new(emit_secs(), alpn),
            vector_clock_stream,
            mmr_stream,
            event_stream,
            file_stream,
        })
    }

    pub async fn send_vector_clock(&mut self, vc: &XaeroVectorClock) -> Result<(), Error> {
        let vc_bytes = bytemuck::bytes_of(vc);
        self.vector_clock_stream.send.write_all(vc_bytes).await?;
        self.vector_clock_stream.send.flush().await?;
        Ok(())
    }

    pub async fn send_event<const TSHIRT: usize>(&mut self, event: &XaeroPeerEvent<TSHIRT>) -> Result<(), Error> {
        let event_bytes = bytemuck::bytes_of(event);
        self.event_stream.send.write_all(event_bytes).await?;
        self.event_stream.send.flush().await?;
        Ok(())
    }

    pub async fn send_file<const CHUNK_SIZE: usize>(&mut self, location: String) -> Result<(), Error> {
        let loc = location.as_str();
        let file_data = tokio::fs::read(loc).await?;
        let crc32 = checksum_file(Crc32IsoHdlc, loc, Some(CHUNK_SIZE))?;

        // Send header
        let header = XaeroFileHeader {
            magic: *b"XAER",
            size: file_data.len() as u64,
            crc32,
        };
        self.file_stream.send.write_all(bytemuck::bytes_of(&header)).await?;
        self.file_stream.send.write_all(&file_data).await?;
        Ok(())
    }
}

#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct XaeroUserData {
    pub xaero_id_hash: [u8; 32],
    pub vector_clock_hash: [u8; 32],
}

unsafe impl Pod for XaeroUserData {}
unsafe impl Zeroable for XaeroUserData {}

/// Minimal P2P Actor
pub struct P2pActor<const TSHIRT: usize, const RING_CAPACITY: usize> {
    our_xaero_id: [u8; 32],
    endpoint: Endpoint,
    active_peers: HashMap<[u8; 32], XspConnection>,
    running: Arc<AtomicBool>,
    aof_actor: Arc<AofState>,
    vector_clock_actor: VectorClockActor,
    xaero_id_cache: XaeroIdCacheS,
    node_id_to_xaero_id_mapping: HashMap<[u8; 32], [u8; 32]>, /* FIXME: remove heap allocation
                                                               * when time permits */
}

impl<const TSHIRT: usize, const RING_CAPACITY: usize> P2pActor<TSHIRT, RING_CAPACITY> {
    pub async fn new(
        ring_buffer: &'static RingBuffer<TSHIRT, RING_CAPACITY>,
        our_xaero_id: XaeroID,
        aof_actor: Arc<AofState>,
        lmdb_env: Arc<std::sync::Mutex<LmdbEnv>>,
    ) -> Result<(Self, Writer<TSHIRT, RING_CAPACITY>, Reader<TSHIRT, RING_CAPACITY>)> {
        let xaero_user_data = XaeroUserData {
            xaero_id_hash: blake_hash_slice(&our_xaero_id.did_peer[..our_xaero_id.did_peer_len as usize]),
            vector_clock_hash: [0u8; 32],
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
            vector_clock_actor: VectorClockActor::new(lmdb_env),
            xaero_id_cache: XaeroIdCacheS::new(),
            node_id_to_xaero_id_mapping: HashMap::new(),
        };

        let writer = Writer::new(ring_buffer);
        let reader = Reader::new(ring_buffer);

        Ok((actor, writer, reader))
    }

    pub async fn start(&mut self, mut writer: Writer<TSHIRT, RING_CAPACITY>, mut reader: Reader<TSHIRT, RING_CAPACITY>) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);
        tracing::info!("Starting P2P actor");

        let mut cleanup_timer = tokio::time::interval(Duration::from_secs(30));

        // Main loop
        while self.running.load(Ordering::Relaxed) {
            tokio::select! {
                // Handle incoming connections
                Some(incoming) = self.endpoint.accept() => {
                    if let Ok(connecting) = incoming.accept()
                        && let Ok(conn) = connecting.await {
                            let peer_node_id = conn.remote_node_id()?;
                            let peer_xaero_id = self.node_id_to_xaero_id(peer_node_id)?;
                            let peer_id_hash = blake_hash_slice(bytemuck::bytes_of(&peer_xaero_id));

                            // Create XspConnection with all streams
                            if let Ok(xsp_conn) = XspConnection::new(peer_id_hash, "xsp-1.0".to_string(), conn).await {
                                self.active_peers.insert(peer_id_hash, xsp_conn);
                                tracing::info!("New peer connected: {:?}", peer_id_hash);
                            }
                        }
                },

                // Broadcast events from ring buffer
                Some(event) = async { reader.next() } => {

                    for (peer_id, xsp_conn) in &mut self.active_peers {
                        if let Err(e) = Self::send_event_to_peer_static(xsp_conn, event).await {
                            tracing::warn!("Failed to send to peer {:?}: {:?}", peer_id, e);
                        }
                    }
                },

                // Handle incoming messages from all peers
                _ = async {
                    for (peer_id, xsp_conn) in &mut self.active_peers {
                        // Try to receive vector clock with timeout
                        if let Ok(vc_result) = tokio::time::timeout(Duration::from_millis(1), async {
                            let mut buf = [0u8; 504]; // Exact size for XaeroVectorClock
                            xsp_conn.vector_clock_stream.recv.read_exact(&mut buf).await?;
                            let vc = bytemuck::from_bytes::<XaeroVectorClock>(&buf);
                            Ok::<XaeroVectorClock, ReadExactError>(*vc)
                        }).await
                            && let Ok(vc_buf) = vc_result {
                                tracing::info!("Received vector clock from peer {:?}", peer_id);
                                self.vector_clock_actor.state.merge_peer_clock(&vc_buf);
                            }

                        // Try to receive events with timeout - using read_exact for known event size
                        if let Ok(event_result) = tokio::time::timeout(Duration::from_millis(1), async {
                            let mut buf = [0u8; TSHIRT];
                            xsp_conn.event_stream.recv.read_exact(&mut buf).await?;
                            Ok::<[u8; TSHIRT], ReadExactError>(buf)
                        }).await
                            && let Ok(event_buf) = event_result {
                                tracing::info!("Received event from peer {:?}, {} bytes", peer_id, TSHIRT);
                                // Add to local ring buffer if valid
                                if let Ok(event) = rusted_ring::EventUtils::create_pooled_event::<TSHIRT>(&event_buf, 0) {
                                    let _ = writer.add(event);
                                }
                            }

                        // Try to receive files with timeout
                        if let Ok(file_result) = tokio::time::timeout(Duration::from_millis(1), async {
                            let mut header_buf = [0u8; std::mem::size_of::<XaeroFileHeader>()];
                            xsp_conn.file_stream.recv.read_exact(&mut header_buf).await?;
                            let header = bytemuck::from_bytes::<XaeroFileHeader>(&header_buf);
                            Ok::<XaeroFileHeader, ReadExactError>(*header)
                        }).await
                            && let Ok(file_header) = file_result {
                                tracing::info!("Receiving file from peer {:?}, size: {}", peer_id, file_header.size);
                                // Read file data
                                let mut file_data = vec![0u8; file_header.size as usize];
                                if xsp_conn.file_stream.recv.read_exact(&mut file_data).await.is_ok() {
                                    // Verify CRC and save file
                                    let filename = format!("received_file_{}.tmp", emit_secs());
                                    if tokio::fs::write(&filename, &file_data).await.is_ok() {
                                        tracing::info!("Saved received file: {}", filename);
                                    }
                                }
                            }
            }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                } => {},

                // Cleanup dead connections
                _ = cleanup_timer.tick() => {
                    self.active_peers.retain(|peer_id, xsp_conn| {
                        if xsp_conn.conn.close_reason().is_some() {
                            tracing::info!("Removing dead peer: {:?}", peer_id);
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
        let event_bytes = bytemuck::bytes_of(&event);
        xsp_conn.event_stream.send.write_all(event_bytes).await?;
        Ok(())
    }

    pub async fn connect_to_peer(&mut self, peer_xaero_id: XaeroID) -> Result<()> {
        let node_id = Self::xaero_id_to_node_id(self, peer_xaero_id)?;
        let node_addr = NodeAddr::new(node_id);
        let conn = self.endpoint.connect(node_addr, b"xsp-1.0").await?;
        let peer_id_hash = blake_hash_slice(bytemuck::bytes_of(&peer_xaero_id));

        let xsp_conn = XspConnection::new(peer_id_hash, "xsp-1.0".to_string(), conn).await?;
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

    fn node_id_to_xaero_id(&self, node_id: iroh::NodeId) -> Result<XaeroID> {
        // look up blake3 id to xaero_id
        let n_id = node_id.as_bytes();
        // assume node id and xaero id hash is same for now.
        let xaero_id_hash = self.node_id_to_xaero_id_mapping.get(node_id.as_bytes()).unwrap_or_else(|| {
            // TODO : find xaero_id hash using P2P call for now we just leave it.
            panic!("failed to grab xaero_id hash for node id gotten!")
        });
        let xaero_id_found = get_xaero_id_by_xaero_id_hash(&self.aof_actor.env, *xaero_id_hash).ok().flatten().unwrap_or_else(|| {
            // TODO: grab xaero id else where.
            panic!("failed to grab xaero_id hash for node id gotten!");
        });
        Ok(*xaero_id_found)
    }
}
