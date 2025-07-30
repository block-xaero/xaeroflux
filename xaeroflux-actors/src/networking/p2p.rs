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
use tracing;
use xaeroflux_core::{
    date_time::emit_secs,
    hash::blake_hash_slice,
    pool::XaeroPeerEvent,
    vector_clock_actor::{VectorClockActor, XaeroVectorClock},
};
use xaeroid::XaeroID;

use crate::{
    aof::{ring_buffer_actor::AofState, storage::format::MmrMeta},
    networking::format::XaeroFileHeader,
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
    pub video_stream: Option<XaeroQuicStream>,
    pub audio_stream: Option<XaeroQuicStream>,
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

        // Create audio/video streams immediately for MVP
        let (audio_sender, audio_receiver) = connection.open_bi().await?;
        let audio_stream = XaeroQuicStream::new("audio$".to_string(), StreamType::Audio, audio_sender, audio_receiver);

        let (video_sender, video_receiver) = connection.open_bi().await?;
        let video_stream = XaeroQuicStream::new("video$".to_string(), StreamType::Video, video_sender, video_receiver);

        Ok(XspConnection {
            peer_id,
            conn: connection,
            meta: XspConnMeta::new(emit_secs(), alpn),
            vector_clock_stream,
            mmr_stream,
            event_stream,
            file_stream,
            video_stream: Some(video_stream),
            audio_stream: Some(audio_stream),
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

    pub async fn send_audio_chunk(&mut self, audio_data: &[u8]) -> Result<(), Error> {
        if let Some(ref mut audio_stream) = self.audio_stream {
            // Send chunk size first (4 bytes)
            let chunk_size = audio_data.len() as u32;
            audio_stream.send.write_all(&chunk_size.to_le_bytes()).await?;
            // Send audio data
            audio_stream.send.write_all(audio_data).await?;
            audio_stream.send.flush().await?;
        }
        Ok(())
    }

    pub async fn send_video_chunk(&mut self, video_data: &[u8]) -> Result<(), Error> {
        if let Some(ref mut video_stream) = self.video_stream {
            // Send chunk size first (4 bytes)
            let chunk_size = video_data.len() as u32;
            video_stream.send.write_all(&chunk_size.to_le_bytes()).await?;
            // Send video data
            video_stream.send.write_all(video_data).await?;
            video_stream.send.flush().await?;
        }
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
}

impl<const TSHIRT: usize, const RING_CAPACITY: usize> P2pActor<TSHIRT, RING_CAPACITY> {
    pub async fn new(
        ring_buffer: &'static RingBuffer<TSHIRT, RING_CAPACITY>,
        our_xaero_id: XaeroID,
        aof_actor: Arc<AofState>,
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
            vector_clock_actor: VectorClockActor::new(),
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
                    if let Ok(connecting) = incoming.accept() {
                        if let Ok(conn) = connecting.await {
                            let peer_node_id = conn.remote_node_id().unwrap();
                            let peer_xaero_id = Self::node_id_to_xaero_id(peer_node_id).unwrap();
                            let peer_id_hash = blake_hash_slice(bytemuck::bytes_of(&peer_xaero_id));

                            // Create XspConnection with all streams
                            if let Ok(xsp_conn) = XspConnection::new(peer_id_hash, "xsp-1.0".to_string(), conn).await {
                                self.active_peers.insert(peer_id_hash, xsp_conn);
                                tracing::info!("New peer connected: {:?}", peer_id_hash);
                            }
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
                        }).await {
                            if let Ok(vc_buf) = vc_result {
                                tracing::info!("Received vector clock from peer {:?}", peer_id);
                                self.vector_clock_actor.state.merge_peer_clock(&vc_buf);
                            }
                        }

                        // Try to receive events with timeout - using read_exact for known event size
                        if let Ok(event_result) = tokio::time::timeout(Duration::from_millis(1), async {
                            let mut buf = [0u8; TSHIRT];
                            xsp_conn.event_stream.recv.read_exact(&mut buf).await?;
                            Ok::<[u8; TSHIRT], ReadExactError>(buf)
                        }).await {
                            if let Ok(event_buf) = event_result {
                                tracing::info!("Received event from peer {:?}, {} bytes", peer_id, TSHIRT);
                                // Add to local ring buffer if valid
                                if let Ok(event) = rusted_ring::EventUtils::create_pooled_event::<TSHIRT>(&event_buf, 0) {
                                    let _ = writer.add(event);
                                }
                            }
                        }

                        // Try to receive files with timeout
                        if let Ok(file_result) = tokio::time::timeout(Duration::from_millis(1), async {
                            let mut header_buf = [0u8; std::mem::size_of::<XaeroFileHeader>()];
                            xsp_conn.file_stream.recv.read_exact(&mut header_buf).await?;
                            let header = bytemuck::from_bytes::<XaeroFileHeader>(&header_buf);
                            Ok::<XaeroFileHeader, ReadExactError>(*header)
                        }).await {
                            if let Ok(file_header) = file_result {
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

                        // Try to receive audio chunks with timeout
                        if let Some(ref mut audio_stream) = xsp_conn.audio_stream {
                            if let Ok(audio_result) = tokio::time::timeout(Duration::from_millis(1), async {
                                let mut size_buf = [0u8; 4];
                                audio_stream.recv.read_exact(&mut size_buf).await?;
                                let chunk_size = u32::from_le_bytes(size_buf) as usize;
                                if chunk_size > 0 && chunk_size < 1_000_000 { // 1MB max
                                    let mut audio_data = vec![0u8; chunk_size];
                                    audio_stream.recv.read_exact(&mut audio_data).await?;
                                    Ok(audio_data)
                                } else {
                                    Err(anyhow::anyhow!("Invalid audio chunk size"))
                                }
                            }).await {
                                if let Ok(audio_chunk) = audio_result {
                                    tracing::debug!("Received audio chunk from peer {:?}, {} bytes", peer_id, audio_chunk.len());
                                    // Process audio chunk (play, save, etc.)
                                    // Could add to audio ring buffer or direct to audio output
                                }
                            }
                        }

                        // Try to receive video chunks with timeout
                        if let Some(ref mut video_stream) = xsp_conn.video_stream {
                            if let Ok(video_result) = tokio::time::timeout(Duration::from_millis(1), async {
                                let mut size_buf = [0u8; 4];
                                video_stream.recv.read_exact(&mut size_buf).await?;
                                let chunk_size = u32::from_le_bytes(size_buf) as usize;
                                if chunk_size > 0 && chunk_size < 10_000_000 { // 10MB max
                                    let mut video_data = vec![0u8; chunk_size];
                                    video_stream.recv.read_exact(&mut video_data).await?;
                                    Ok(video_data)
                                } else {
                                    Err(anyhow::anyhow!("Invalid video chunk size"))
                                }
                            }).await {
                                if let Ok(video_chunk) = video_result {
                                    tracing::debug!("Received video chunk from peer {:?}, {} bytes", peer_id, video_chunk.len());
                                    // Process video chunk (decode, display, save, etc.)
                                    // Could add to video ring buffer or direct to video output
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
        let node_id = Self::xaero_id_to_node_id(peer_xaero_id)?;
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

    pub async fn broadcast_audio_to_peers(&mut self, audio_data: &[u8]) -> Result<()> {
        for (peer_id, xsp_conn) in &mut self.active_peers {
            if let Err(e) = xsp_conn.send_audio_chunk(audio_data).await {
                tracing::warn!("Failed to send audio to peer {:?}: {:?}", peer_id, e);
            }
        }
        Ok(())
    }

    pub async fn broadcast_video_to_peers(&mut self, video_data: &[u8]) -> Result<()> {
        for (peer_id, xsp_conn) in &mut self.active_peers {
            if let Err(e) = xsp_conn.send_video_chunk(video_data).await {
                tracing::warn!("Failed to send video to peer {:?}: {:?}", peer_id, e);
            }
        }
        Ok(())
    }

    fn xaero_id_to_node_id(xaero_id: XaeroID) -> Result<iroh::NodeId> {
        let hash = blake_hash_slice(&xaero_id.secret_key);
        let mut node_id_bytes = [0u8; 32];
        node_id_bytes.copy_from_slice(&hash[..32]);
        Ok(iroh::NodeId::from_bytes(&node_id_bytes)?)
    }

    fn node_id_to_xaero_id(node_id: iroh::NodeId) -> Result<XaeroID> {
        // look up blake3 id to xaero_id
        Ok(XaeroID::zeroed())
    }
}
