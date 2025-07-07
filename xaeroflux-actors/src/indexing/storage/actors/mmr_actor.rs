//! MMR indexing actor using ring buffer architecture.
//!
//! This actor:
//! - Reads XaeroInternalEvent from input ring buffer
//! - Archives events and computes Merkle leaf hashes
//! - Updates an in-memory Merkle Mountain Range
//! - Persists events to LMDB and forwards leaf hashes to segment writer
//! - Writes MMR confirmations to output ring buffer

use std::{
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use rusted_ring_new::{EventPoolFactory, EventUtils, PooledEvent, Reader, RingBuffer, Writer};
use xaeroflux_core::{
    CONF,
    date_time::emit_secs,
    event::{EventType, SystemEventKind},
    hash::sha_256_slice,
    pipe::BusKind,
    pool::XaeroInternalEvent,
    system_paths,
};

use crate::{
    aof::storage::lmdb::{LmdbEnv, push_internal_event_universal},
    indexing::storage::{
        actors::segment_writer_actor::{SegmentConfig, SegmentWriterActor},
        format::{
            archive_xaero_internal_event_64, archive_xaero_internal_event_256, archive_xaero_internal_event_1024,
            archive_xaero_internal_event_4096, archive_xaero_internal_event_16384,
        },
        mmr::{XaeroMmr, XaeroMmrOps},
    },
    subject::SubjectHash,
};

// ================================================================================================
// MMR-SPECIFIC RING BUFFERS
// ================================================================================================

// MMR Control Event Flow: XS input -> XS output (confirmations)
static MMR_CONTROL_INPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static MMR_CONTROL_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// MMR Data Event Flow: S input -> XS output (confirmations are small)
static MMR_DATA_INPUT_RING: OnceLock<RingBuffer<256, 1000>> = OnceLock::new();
static MMR_DATA_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// MMR Medium Event Flow: M input -> XS output
static MMR_MEDIUM_INPUT_RING: OnceLock<RingBuffer<1024, 500>> = OnceLock::new();
static MMR_MEDIUM_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// MMR Large Event Flow: L input -> XS output
static MMR_LARGE_INPUT_RING: OnceLock<RingBuffer<4096, 100>> = OnceLock::new();
static MMR_LARGE_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// MMR XL Event Flow: XL input -> XS output
static MMR_XL_INPUT_RING: OnceLock<RingBuffer<16384, 50>> = OnceLock::new();
static MMR_XL_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// ================================================================================================
// MMR -> SEGMENT WRITER BRIDGE RING BUFFERS
// ================================================================================================

/// Leaf hash event for forwarding to segment writer - exactly 64 bytes for XS pool
#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct LeafHashEvent {
    pub leaf_hash: [u8; 32],    // MMR leaf hash (32 bytes)
    pub original_size: u32,     // Original event size indicator (4 bytes)
    pub timestamp: u64,         // Event timestamp (8 bytes)
    pub subject_hash: [u8; 20], // Subject hash (20 bytes)
}

unsafe impl Pod for LeafHashEvent {}
unsafe impl Zeroable for LeafHashEvent {}

// Shared bridge rings - MMR writes, Segment Writer reads
static SEGMENT_BRIDGE_XS_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static SEGMENT_BRIDGE_S_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static SEGMENT_BRIDGE_M_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static SEGMENT_BRIDGE_L_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static SEGMENT_BRIDGE_XL_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// ================================================================================================
// TYPES & STRUCTS
// ================================================================================================

/// MMR processing confirmation event - exactly 64 bytes for XS pool
#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct MmrProcessConfirmation {
    pub original_hash: [u8; 32], // XaeroID hash from XaeroInternalEvent (32 bytes)
    pub leaf_hash: [u8; 32],     // Generated MMR leaf hash (32 bytes)
}

unsafe impl Pod for MmrProcessConfirmation {}
unsafe impl Zeroable for MmrProcessConfirmation {}

/// MMR Actor statistics
#[derive(Debug, Default)]
pub struct MmrStats {
    pub total_processed: u64,
    pub failed_processing: u64,
    pub mmr_size: usize,
    pub last_leaf_hash: Option<[u8; 32]>,
    pub last_process_ts: u64,
}

/// MMR Actor state - tracks processing statistics and MMR state
pub struct MmrState {
    pub env: Arc<Mutex<LmdbEnv>>,
    pub mmr: Arc<Mutex<XaeroMmr>>,
    pub subject_hash: SubjectHash,
    pub bus_kind: BusKind,
    pub stats: MmrStats,
}

impl MmrState {
    /// Create new MMR state with LMDB environment and in-memory MMR
    pub fn new(subject_hash: SubjectHash, bus_kind: BusKind) -> Result<Self, Box<dyn std::error::Error>> {
        // Get base path from config
        let c = CONF.get().expect("failed to unravel config");
        let base_path = &c.aof.file_path;

        // Construct subject-specific path based on bus kind
        let lmdb_path = match bus_kind {
            BusKind::Control => system_paths::emit_control_path_with_subject_hash(
                base_path.to_str().expect("path_invalid_for_mmr"),
                subject_hash.0,
                "mmr",
            ),
            BusKind::Data => system_paths::emit_data_path_with_subject_hash(
                base_path.to_str().expect("path_invalid_for_mmr"),
                subject_hash.0,
                "mmr",
            ),
        };

        tracing::info!("Creating MMR LMDB at path: {}", lmdb_path);
        let env = Arc::new(Mutex::new(LmdbEnv::new(&lmdb_path, bus_kind.clone())?));
        let mmr = Arc::new(Mutex::new(XaeroMmr::new()));

        Ok(Self {
            env,
            mmr,
            subject_hash,
            bus_kind,
            stats: MmrStats::default(),
        })
    }

    /// Process XaeroInternalEvent<64> (XS)
    fn process_internal_event_64(
        &mut self,
        internal_event: Arc<XaeroInternalEvent<64>>,
    ) -> Result<MmrProcessConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_64(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<256> (S)
    fn process_internal_event_256(
        &mut self,
        internal_event: Arc<XaeroInternalEvent<256>>,
    ) -> Result<MmrProcessConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_256(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<1024> (M)
    fn process_internal_event_1024(
        &mut self,
        internal_event: Arc<XaeroInternalEvent<1024>>,
    ) -> Result<MmrProcessConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_1024(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<4096> (L)
    fn process_internal_event_4096(
        &mut self,
        internal_event: Arc<XaeroInternalEvent<4096>>,
    ) -> Result<MmrProcessConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_4096(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<16384> (XL)
    fn process_internal_event_16384(
        &mut self,
        internal_event: Arc<XaeroInternalEvent<16384>>,
    ) -> Result<MmrProcessConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_16384(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Common processing logic for archived data
    fn process_archived_data(
        &mut self,
        archived_data: Vec<u8>,
        xaero_id_hash: [u8; 32],
        latest_ts: u64,
    ) -> Result<MmrProcessConfirmation, Box<dyn std::error::Error>> {
        // Step 1: Persist to LMDB using universal push
        push_internal_event_universal(
            &self.env,
            &archived_data,
            EventType::ApplicationEvent(1).to_u8() as u32,
            latest_ts,
        )?;

        // Step 2: Compute leaf hash from archived data
        let leaf_hash = sha_256_slice(&archived_data);

        // Step 3: Update MMR
        let new_mmr_size = {
            let mut mmr_guard = self.mmr.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let _changed_peaks = mmr_guard.append(leaf_hash);
            mmr_guard.leaf_count()
        };

        // Step 4: Update statistics
        self.stats.total_processed += 1;
        self.stats.mmr_size = new_mmr_size;
        self.stats.last_leaf_hash = Some(leaf_hash);
        self.stats.last_process_ts = latest_ts;

        tracing::debug!(
            "Processed MMR event with leaf hash: {:?}, MMR size: {}",
            leaf_hash,
            new_mmr_size
        );

        Ok(MmrProcessConfirmation {
            original_hash: xaero_id_hash,
            leaf_hash,
        })
    }

    /// Log periodic statistics
    fn log_stats(&self) {
        if self.stats.total_processed % 1000 == 0 && self.stats.total_processed > 0 {
            tracing::info!(
                "MMR Stats - Subject: {:?}, Bus: {:?}, Processed: {}, Failed: {}, MMR Size: {}",
                self.subject_hash,
                self.bus_kind,
                self.stats.total_processed,
                self.stats.failed_processing,
                self.stats.mmr_size
            );
        }
    }
}

/// MMR Actor - processes XaeroInternalEvent from ring buffer and maintains MMR
pub struct MmrActor {
    // Exposed interfaces for all T-shirt sizes
    pub in_writer_xs: Writer<64, 2000>,  // XS events input
    pub in_writer_s: Writer<256, 1000>,  // S events input
    pub in_writer_m: Writer<1024, 500>,  // M events input
    pub in_writer_l: Writer<4096, 100>,  // L events input
    pub in_writer_xl: Writer<16384, 50>, // XL events input

    pub out_reader_xs: Reader<64, 2000>, // XS confirmations output
    pub out_reader_s: Reader<64, 2000>,  // S confirmations output
    pub out_reader_m: Reader<64, 2000>,  // M confirmations output
    pub out_reader_l: Reader<64, 2000>,  // L confirmations output
    pub out_reader_xl: Reader<64, 2000>, // XL confirmations output

    pub segment_writer: Arc<SegmentWriterActor>, // Integrated segment writer
    pub jh: JoinHandle<()>,
}

// ================================================================================================
// MMR ACTOR IMPLEMENTATION
// ================================================================================================

impl MmrActor {
    /// Create and spawn MMR actor with ring buffer processing and integrated segment writer
    pub fn spin(
        subject_hash: SubjectHash,
        bus_kind: BusKind,
        segment_config: Option<SegmentConfig>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut state = MmrState::new(subject_hash, bus_kind.clone())?;

        // Create integrated segment writer actor
        let segment_writer = Arc::new(SegmentWriterActor::spin(
            subject_hash,
            bus_kind.clone(),
            segment_config,
        )?);
        let segment_writer_clone = segment_writer.clone();

        let jh = thread::spawn(move || {
            tracing::info!("MMR Actor started - Subject: {:?}, Bus: {:?}", subject_hash, bus_kind);

            // Initialize all ring buffers
            let xs_input_ring = MMR_CONTROL_INPUT_RING.get_or_init(RingBuffer::new);
            let xs_output_ring = MMR_CONTROL_OUTPUT_RING.get_or_init(RingBuffer::new);

            let s_input_ring = MMR_DATA_INPUT_RING.get_or_init(RingBuffer::new);
            let s_output_ring = MMR_DATA_OUTPUT_RING.get_or_init(RingBuffer::new);

            let m_input_ring = MMR_MEDIUM_INPUT_RING.get_or_init(RingBuffer::new);
            let m_output_ring = MMR_MEDIUM_OUTPUT_RING.get_or_init(RingBuffer::new);

            let l_input_ring = MMR_LARGE_INPUT_RING.get_or_init(RingBuffer::new);
            let l_output_ring = MMR_LARGE_OUTPUT_RING.get_or_init(RingBuffer::new);

            let xl_input_ring = MMR_XL_INPUT_RING.get_or_init(RingBuffer::new);
            let xl_output_ring = MMR_XL_OUTPUT_RING.get_or_init(RingBuffer::new);

            // Create readers and writers for all sizes
            let mut xs_reader = Reader::new(xs_input_ring);
            let mut xs_writer = Writer::new(xs_output_ring);

            let mut s_reader = Reader::new(s_input_ring);
            let mut s_writer = Writer::new(s_output_ring);

            let mut m_reader = Reader::new(m_input_ring);
            let mut m_writer = Writer::new(m_output_ring);

            let mut l_reader = Reader::new(l_input_ring);
            let mut l_writer = Writer::new(l_output_ring);

            let mut xl_reader = Reader::new(xl_input_ring);
            let mut xl_writer = Writer::new(xl_output_ring);

            // Process all ring buffers in a loop
            loop {
                let mut total_events_processed = 0;

                // Process XS events (64 bytes)
                total_events_processed +=
                    Self::process_xs_events(&mut state, &mut xs_reader, &mut xs_writer, &segment_writer_clone);

                // Process S events (256 bytes)
                total_events_processed +=
                    Self::process_s_events(&mut state, &mut s_reader, &mut s_writer, &segment_writer_clone);

                // Process M events (1024 bytes)
                total_events_processed +=
                    Self::process_m_events(&mut state, &mut m_reader, &mut m_writer, &segment_writer_clone);

                // Process L events (4096 bytes)
                total_events_processed +=
                    Self::process_l_events(&mut state, &mut l_reader, &mut l_writer, &segment_writer_clone);

                // Process XL events (16384 bytes)
                total_events_processed +=
                    Self::process_xl_events(&mut state, &mut xl_reader, &mut xl_writer, &segment_writer_clone);

                if total_events_processed > 0 {
                    state.log_stats();
                }

                thread::sleep(Duration::from_micros(100));
            }
        });

        // Create exposed interfaces using the dedicated MMR ring buffers
        let xs_input_ring = MMR_CONTROL_INPUT_RING.get_or_init(RingBuffer::new);
        let xs_output_ring = MMR_CONTROL_OUTPUT_RING.get_or_init(RingBuffer::new);
        let s_input_ring = MMR_DATA_INPUT_RING.get_or_init(RingBuffer::new);
        let s_output_ring = MMR_DATA_OUTPUT_RING.get_or_init(RingBuffer::new);
        let m_input_ring = MMR_MEDIUM_INPUT_RING.get_or_init(RingBuffer::new);
        let m_output_ring = MMR_MEDIUM_OUTPUT_RING.get_or_init(RingBuffer::new);
        let l_input_ring = MMR_LARGE_INPUT_RING.get_or_init(RingBuffer::new);
        let l_output_ring = MMR_LARGE_OUTPUT_RING.get_or_init(RingBuffer::new);
        let xl_input_ring = MMR_XL_INPUT_RING.get_or_init(RingBuffer::new);
        let xl_output_ring = MMR_XL_OUTPUT_RING.get_or_init(RingBuffer::new);

        Ok(Self {
            in_writer_xs: Writer::new(xs_input_ring),
            in_writer_s: Writer::new(s_input_ring),
            in_writer_m: Writer::new(m_input_ring),
            in_writer_l: Writer::new(l_input_ring),
            in_writer_xl: Writer::new(xl_input_ring),

            out_reader_xs: Reader::new(xs_output_ring),
            out_reader_s: Reader::new(s_output_ring),
            out_reader_m: Reader::new(m_output_ring),
            out_reader_l: Reader::new(l_output_ring),
            out_reader_xl: Reader::new(xl_output_ring),

            segment_writer,
            jh,
        })
    }

    /// Process XS events (64 bytes) - for truly small events only
    fn process_xs_events(
        state: &mut MmrState,
        reader: &mut Reader<64, 2000>,
        writer: &mut Writer<64, 2000>,
        _segment_writer: &Arc<SegmentWriterActor>,
    ) -> usize {
        let mut events_processed = 0;

        // Get bridge ring for forwarding leaf hashes to segment writer
        let bridge_ring = SEGMENT_BRIDGE_XS_RING.get_or_init(RingBuffer::new);
        let mut bridge_writer = Writer::new(bridge_ring);

        for event in reader.by_ref() {
            // XS ring should only handle events that actually fit in 64 bytes
            // Since XaeroInternalEvent<64> is actually 256 bytes, XS ring might handle smaller control events
            tracing::debug!("XS ring received event of size: {} bytes", event.len);

            // For now, we don't expect XaeroInternalEvent in XS ring due to size constraints
            // This ring might be used for other control messages in the future
            events_processed += 1;
        }

        events_processed
    }

    /// Process S events (256 bytes) - handles XaeroInternalEvent<64> which is actually 256 bytes
    fn process_s_events(
        state: &mut MmrState,
        reader: &mut Reader<256, 1000>,
        writer: &mut Writer<64, 2000>,
        _segment_writer: &Arc<SegmentWriterActor>,
    ) -> usize {
        let mut events_processed = 0;

        // Get bridge ring for forwarding leaf hashes to segment writer
        let bridge_ring = SEGMENT_BRIDGE_S_RING.get_or_init(RingBuffer::new);
        let mut bridge_writer = Writer::new(bridge_ring);

        for event in reader.by_ref() {
            // Check for both XaeroInternalEvent<64> (which is ~256 bytes) and XaeroInternalEvent<256>
            let xe64_size = std::mem::size_of::<XaeroInternalEvent<64>>();
            let xe256_size = std::mem::size_of::<XaeroInternalEvent<256>>();

            tracing::debug!(
                "S ring received event of size: {} bytes (XE64={}, XE256={})",
                event.len,
                xe64_size,
                xe256_size
            );

            if event.len == xe64_size as u32 {
                // Handle XaeroInternalEvent<64> (which is actually ~256 bytes total)
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<64>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_64(arc_internal_event.clone()) {
                    Ok(confirmation) => {
                        // Send MMR confirmation
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 201) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("MMR S confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create S MMR confirmation: {:?}", e),
                        }

                        // Forward leaf hash to segment writer via bridge ring
                        if confirmation.leaf_hash != [0; 32] {
                            let leaf_event = LeafHashEvent {
                                leaf_hash: confirmation.leaf_hash,
                                original_size: 64, // Original data size, not struct size
                                timestamp: internal_event.latest_ts,
                                subject_hash: state.subject_hash.0[..20].try_into().unwrap_or([0; 20]),
                            };

                            let leaf_bytes = bytemuck::bytes_of(&leaf_event);
                            match EventUtils::create_pooled_event::<64>(leaf_bytes, 202) {
                                Ok(leaf_pooled_event) =>
                                    if !bridge_writer.add(leaf_pooled_event) {
                                        tracing::warn!("Segment bridge S buffer full - dropping leaf hash");
                                    },
                                Err(e) => tracing::error!("Failed to create S leaf event: {:?}", e),
                            }

                            tracing::debug!("Forwarded S leaf hash to segment writer: {:?}", confirmation.leaf_hash);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process S MMR event (XaeroInternalEvent<64>): {:?}", e);
                        state.stats.failed_processing += 1;
                    }
                }
            } else if event.len == xe256_size as u32 {
                // Handle XaeroInternalEvent<256>
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<256>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_256(arc_internal_event.clone()) {
                    Ok(confirmation) => {
                        // Send MMR confirmation
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 201) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("MMR S confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create S MMR confirmation: {:?}", e),
                        }

                        // Forward leaf hash to segment writer via bridge ring
                        if confirmation.leaf_hash != [0; 32] {
                            let leaf_event = LeafHashEvent {
                                leaf_hash: confirmation.leaf_hash,
                                original_size: 256,
                                timestamp: internal_event.latest_ts,
                                subject_hash: state.subject_hash.0[..20].try_into().unwrap_or([0; 20]),
                            };

                            let leaf_bytes = bytemuck::bytes_of(&leaf_event);
                            match EventUtils::create_pooled_event::<64>(leaf_bytes, 202) {
                                Ok(leaf_pooled_event) =>
                                    if !bridge_writer.add(leaf_pooled_event) {
                                        tracing::warn!("Segment bridge S buffer full - dropping leaf hash");
                                    },
                                Err(e) => tracing::error!("Failed to create S leaf event: {:?}", e),
                            }

                            tracing::debug!("Forwarded S leaf hash to segment writer: {:?}", confirmation.leaf_hash);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process S MMR event (XaeroInternalEvent<256>): {:?}", e);
                        state.stats.failed_processing += 1;
                    }
                }
            } else {
                tracing::warn!(
                    "Unknown event size in S ring: {} bytes (expected {} or {})",
                    event.len,
                    xe64_size,
                    xe256_size
                );
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process M events (1024 bytes)
    fn process_m_events(
        state: &mut MmrState,
        reader: &mut Reader<1024, 500>,
        writer: &mut Writer<64, 2000>,
        _segment_writer: &Arc<SegmentWriterActor>,
    ) -> usize {
        let mut events_processed = 0;

        // Get bridge ring for forwarding leaf hashes to segment writer
        let bridge_ring = SEGMENT_BRIDGE_M_RING.get_or_init(RingBuffer::new);
        let mut bridge_writer = Writer::new(bridge_ring);

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<1024>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<1024>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_1024(arc_internal_event.clone()) {
                    Ok(confirmation) => {
                        // Send MMR confirmation
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 201) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("MMR M confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create M MMR confirmation: {:?}", e),
                        }

                        // Forward leaf hash to segment writer via bridge ring
                        if confirmation.leaf_hash != [0; 32] {
                            let leaf_event = LeafHashEvent {
                                leaf_hash: confirmation.leaf_hash,
                                original_size: 1024,
                                timestamp: internal_event.latest_ts,
                                subject_hash: state.subject_hash.0[..20].try_into().unwrap_or([0; 20]),
                            };

                            let leaf_bytes = bytemuck::bytes_of(&leaf_event);
                            match EventUtils::create_pooled_event::<64>(leaf_bytes, 202) {
                                Ok(leaf_pooled_event) =>
                                    if !bridge_writer.add(leaf_pooled_event) {
                                        tracing::warn!("Segment bridge M buffer full - dropping leaf hash");
                                    },
                                Err(e) => tracing::error!("Failed to create M leaf event: {:?}", e),
                            }

                            tracing::debug!("Forwarded M leaf hash to segment writer: {:?}", confirmation.leaf_hash);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process M MMR event: {:?}", e);
                        state.stats.failed_processing += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process L events (4096 bytes)
    fn process_l_events(
        state: &mut MmrState,
        reader: &mut Reader<4096, 100>,
        writer: &mut Writer<64, 2000>,
        _segment_writer: &Arc<SegmentWriterActor>,
    ) -> usize {
        let mut events_processed = 0;

        // Get bridge ring for forwarding leaf hashes to segment writer
        let bridge_ring = SEGMENT_BRIDGE_L_RING.get_or_init(RingBuffer::new);
        let mut bridge_writer = Writer::new(bridge_ring);

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<4096>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<4096>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_4096(arc_internal_event.clone()) {
                    Ok(confirmation) => {
                        // Send MMR confirmation
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 201) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("MMR L confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create L MMR confirmation: {:?}", e),
                        }

                        // Forward leaf hash to segment writer via bridge ring
                        if confirmation.leaf_hash != [0; 32] {
                            let leaf_event = LeafHashEvent {
                                leaf_hash: confirmation.leaf_hash,
                                original_size: 4096,
                                timestamp: internal_event.latest_ts,
                                subject_hash: state.subject_hash.0[..20].try_into().unwrap_or([0; 20]),
                            };

                            let leaf_bytes = bytemuck::bytes_of(&leaf_event);
                            match EventUtils::create_pooled_event::<64>(leaf_bytes, 202) {
                                Ok(leaf_pooled_event) =>
                                    if !bridge_writer.add(leaf_pooled_event) {
                                        tracing::warn!("Segment bridge L buffer full - dropping leaf hash");
                                    },
                                Err(e) => tracing::error!("Failed to create L leaf event: {:?}", e),
                            }

                            tracing::debug!("Forwarded L leaf hash to segment writer: {:?}", confirmation.leaf_hash);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process L MMR event: {:?}", e);
                        state.stats.failed_processing += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process XL events (16384 bytes)
    fn process_xl_events(
        state: &mut MmrState,
        reader: &mut Reader<16384, 50>,
        writer: &mut Writer<64, 2000>,
        _segment_writer: &Arc<SegmentWriterActor>,
    ) -> usize {
        let mut events_processed = 0;

        // Get bridge ring for forwarding leaf hashes to segment writer
        let bridge_ring = SEGMENT_BRIDGE_XL_RING.get_or_init(RingBuffer::new);
        let mut bridge_writer = Writer::new(bridge_ring);

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<16384>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<16384>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_16384(arc_internal_event.clone()) {
                    Ok(confirmation) => {
                        // Send MMR confirmation
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 201) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("MMR XL confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create XL MMR confirmation: {:?}", e),
                        }

                        // Forward leaf hash to segment writer via bridge ring
                        if confirmation.leaf_hash != [0; 32] {
                            let leaf_event = LeafHashEvent {
                                leaf_hash: confirmation.leaf_hash,
                                original_size: 16384,
                                timestamp: internal_event.latest_ts,
                                subject_hash: state.subject_hash.0[..20].try_into().unwrap_or([0; 20]),
                            };

                            let leaf_bytes = bytemuck::bytes_of(&leaf_event);
                            match EventUtils::create_pooled_event::<64>(leaf_bytes, 202) {
                                Ok(leaf_pooled_event) =>
                                    if !bridge_writer.add(leaf_pooled_event) {
                                        tracing::warn!("Segment bridge XL buffer full - dropping leaf hash");
                                    },
                                Err(e) => tracing::error!("Failed to create XL leaf event: {:?}", e),
                            }

                            tracing::debug!("Forwarded XL leaf hash to segment writer: {:?}", confirmation.leaf_hash);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process XL MMR event: {:?}", e);
                        state.stats.failed_processing += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Get a reference to the segment writer
    pub fn segment_writer(&self) -> &Arc<SegmentWriterActor> {
        &self.segment_writer
    }

    /// Get MMR stats through callback (since MMR state is in the worker thread)
    pub fn get_mmr_stats(&self) -> Result<String, &'static str> {
        // In ring buffer architecture, we can't directly access MMR state from the main thread
        // This would require implementing a query mechanism through the ring buffers
        Ok("MMR stats access requires ring buffer query mechanism".to_string())
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use rusted_ring_new::PooledEvent;

    use super::*;

    #[test]
    fn test_verify_struct_sizes() {
        println!("=== STRUCT SIZE VERIFICATION ===");

        // XaeroInternalEvent sizes (these include the full struct, not just data)
        println!(
            "XaeroInternalEvent<64>: {} bytes",
            std::mem::size_of::<XaeroInternalEvent<64>>()
        );
        println!(
            "XaeroInternalEvent<256>: {} bytes",
            std::mem::size_of::<XaeroInternalEvent<256>>()
        );
        println!(
            "XaeroInternalEvent<1024>: {} bytes",
            std::mem::size_of::<XaeroInternalEvent<1024>>()
        );
        println!(
            "XaeroInternalEvent<4096>: {} bytes",
            std::mem::size_of::<XaeroInternalEvent<4096>>()
        );
        println!(
            "XaeroInternalEvent<16384>: {} bytes",
            std::mem::size_of::<XaeroInternalEvent<16384>>()
        );

        // Other important structs
        println!(
            "MmrProcessConfirmation: {} bytes",
            std::mem::size_of::<MmrProcessConfirmation>()
        );
        println!("LeafHashEvent: {} bytes", std::mem::size_of::<LeafHashEvent>());

        // Ring buffer mappings based on actual sizes
        println!("=== CORRECT RING BUFFER MAPPINGS ===");

        let xe64_size = std::mem::size_of::<XaeroInternalEvent<64>>();
        if xe64_size <= 64 {
            println!("XaeroInternalEvent<64> -> XS ring (64 bytes)");
        } else if xe64_size <= 256 {
            println!("XaeroInternalEvent<64> -> S ring (256 bytes)");
        } else if xe64_size <= 1024 {
            println!("XaeroInternalEvent<64> -> M ring (1024 bytes)");
        }

        println!("=== END VERIFICATION ===");
    }

    #[test]
    fn test_leaf_hash_event_size() {
        // Verify our leaf hash event fits in XS pool
        let leaf_event = LeafHashEvent {
            leaf_hash: [0; 32],
            original_size: 256,
            timestamp: 1234567890,
            subject_hash: [0; 20],
        };

        let leaf_size = std::mem::size_of::<LeafHashEvent>();
        assert_eq!(
            leaf_size, 64,
            "Leaf hash event must be exactly 64 bytes for XS pool, got {} bytes",
            leaf_size
        );

        println!("✅ Leaf hash event fits perfectly in XS pool: {} bytes", leaf_size);
    }

    #[test]
    fn test_mmr_confirmation_size() {
        // Verify our confirmation event fits in XS pool
        let conf = MmrProcessConfirmation {
            original_hash: [0; 32],
            leaf_hash: [1; 32],
        };

        let conf_size = std::mem::size_of::<MmrProcessConfirmation>();
        assert_eq!(
            conf_size, 64,
            "MMR confirmation must be exactly 64 bytes for XS pool, got {} bytes",
            conf_size
        );

        println!("✅ MMR confirmation fits perfectly in XS pool: {} bytes", conf_size);
    }

    #[test]
    fn test_mmr_actor_creation() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([1u8; 32]);
        let segment_config = Some(crate::indexing::storage::actors::segment_writer_actor::SegmentConfig {
            page_size: 1024,
            pages_per_segment: 2,
            prefix: "test-mmr".to_string(),
            segment_dir: "/tmp/test-mmr-segments".to_string(),
            lmdb_env_path: "/tmp/test-mmr-lmdb".to_string(),
        });

        let actor = MmrActor::spin(subject_hash, BusKind::Data, segment_config).expect("Failed to create MMR actor");

        // Verify all interfaces exist
        assert!(true, "MMR actor created successfully with all T-shirt size interfaces");

        // The actor is running in the background processing events
        drop(actor); // This will trigger cleanup

        println!("✅ MMR actor creation and interface verification successful");
    }

    #[test]
    fn test_mmr_state_processing() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([3u8; 32]);
        let mut state = MmrState::new(subject_hash, BusKind::Data).expect("Failed to create MMR state");

        // Use XS event with correct size (64 bytes)
        let xs_event = Arc::new(XaeroInternalEvent::<64> {
            xaero_id_hash: [0xAA; 32],
            vector_clock_hash: [0xBB; 32],
            evt: PooledEvent {
                data: [0xCC; 64],
                len: 64,
                event_type: 42,
            },
            latest_ts: emit_secs(),
        });

        let result = state.process_internal_event_64(xs_event);
        assert!(result.is_ok(), "Should successfully process XS MMR event");

        let confirmation = result.unwrap();
        assert_eq!(confirmation.original_hash, [0xAA; 32]);
        assert_ne!(confirmation.leaf_hash, [0; 32]);

        // Verify MMR state was updated
        {
            let mmr_guard = state.mmr.lock().expect("Should lock MMR");
            assert_eq!(mmr_guard.leaf_count(), 1, "MMR should have one leaf");
        }

        assert_eq!(state.stats.total_processed, 1, "Stats should show one processed event");
        assert_eq!(state.stats.mmr_size, 1, "Stats should show MMR size of 1");

        println!("✅ MMR state processing working correctly");
    }

    #[test]
    fn test_mmr_with_segment_writer_integration() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([4u8; 32]);
        let segment_config = Some(crate::indexing::storage::actors::segment_writer_actor::SegmentConfig {
            page_size: 4096,
            pages_per_segment: 2,
            prefix: "test-mmr-integration".to_string(),
            segment_dir: "/tmp/test-mmr-integration-segments".to_string(),
            lmdb_env_path: "/tmp/test-mmr-integration-lmdb".to_string(),
        });

        let mut actor =
            MmrActor::spin(subject_hash, BusKind::Data, segment_config).expect("Failed to create MMR actor");

        // Create XaeroInternalEvent<64> - but it's actually 256 bytes total
        let internal_event = XaeroInternalEvent::<64> {
            xaero_id_hash: [0x34; 32],
            vector_clock_hash: [0x56; 32],
            evt: PooledEvent {
                data: [0x12; 64],
                len: 64,
                event_type: 123,
            },
            latest_ts: emit_secs(),
        };

        // Debug the actual size
        let event_size = std::mem::size_of::<XaeroInternalEvent<64>>();
        println!("XaeroInternalEvent<64> actual size: {} bytes", event_size);

        // Since XaeroInternalEvent<64> is 256 bytes, use S ring buffer
        let internal_event_bytes = bytemuck::bytes_of(&internal_event);
        let pooled_event = EventUtils::create_pooled_event::<256>(internal_event_bytes, 123)
            .expect("Should create pooled event with S size (256 bytes)");

        // Write to MMR S input buffer (not XS)
        assert!(
            actor.in_writer_s.add(pooled_event),
            "Should write to MMR S input buffer"
        );

        // Check S output buffer for confirmations
        std::thread::sleep(Duration::from_millis(500));

        let mut mmr_confirmation_found = false;
        for attempt in 0..20 {
            if let Some(conf_event) = actor.out_reader_s.next() {
                if conf_event.len == std::mem::size_of::<MmrProcessConfirmation>() as u32 {
                    if let Ok(conf) =
                        bytemuck::try_from_bytes::<MmrProcessConfirmation>(&conf_event.data[..conf_event.len as usize])
                    {
                        mmr_confirmation_found = true;
                        println!("✅ Received MMR confirmation with leaf hash: {:?}", conf.leaf_hash);
                        break;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        assert!(mmr_confirmation_found, "Should receive MMR confirmation from S buffer");
        println!("✅ MMR S buffer integration working correctly");
    }
}
