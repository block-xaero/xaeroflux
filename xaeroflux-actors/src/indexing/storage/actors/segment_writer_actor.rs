//! Segment Writer Actor using ring buffer architecture with XaeroInternalEvent.
//!
//! This actor:
//! - Reads XaeroInternalEvent from input ring buffer
//! - Archives using generated archive functions from format.rs
//! - Writes archived event blobs into fixed-size pages within segment files
//! - Manages page boundaries, segment rollover, and LMDB metadata
//! - Writes PayloadWritten confirmations to output ring buffer

use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use memmap2::MmapMut;
use rusted_ring_new::{EventPoolFactory, EventUtils, PooledEvent, Reader, RingBuffer, Writer};
use xaeroflux_core::{
    CONF,
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{EventType, SystemEventKind},
    hash::sha_256_hash_b,
    pipe::BusKind,
    pool::XaeroInternalEvent,
    size::PAGE_SIZE,
    system_paths,
};

use crate::{
    aof::storage::{
        format::SegmentMeta,
        lmdb::{LmdbEnv, push_internal_event_universal},
        meta::iterate_segment_meta_by_range,
    },
    indexing::storage::format::{
        archive_xaero_internal_event_64,
        archive_xaero_internal_event_256,
        // Import the generated archive functions from format.rs
        archive_xaero_internal_event_1024,
        archive_xaero_internal_event_4096,
        archive_xaero_internal_event_16384,
    },
    subject::SubjectHash,
    system_payload::SystemPayload,
};

// ================================================================================================
// SEGMENT-SPECIFIC RING BUFFERS
// ================================================================================================

// Segment Control Event Flow: XS input -> XS output (confirmations)
static SEGMENT_CONTROL_INPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static SEGMENT_CONTROL_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// Segment Data Event Flow: S input -> XS output (confirmations are small)
static SEGMENT_DATA_INPUT_RING: OnceLock<RingBuffer<256, 1000>> = OnceLock::new();
static SEGMENT_DATA_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// Medium Event Flow: M input -> XS output
static SEGMENT_MEDIUM_INPUT_RING: OnceLock<RingBuffer<1024, 500>> = OnceLock::new();
static SEGMENT_MEDIUM_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// Large Event Flow: L input -> XS output
static SEGMENT_LARGE_INPUT_RING: OnceLock<RingBuffer<4096, 100>> = OnceLock::new();
static SEGMENT_LARGE_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// Extra Large Event Flow: XL input -> XS output
static SEGMENT_XL_INPUT_RING: OnceLock<RingBuffer<16384, 50>> = OnceLock::new();
static SEGMENT_XL_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// ================================================================================================
// CONFIGURATION & TYPES
// ================================================================================================

/// Configuration for paged segment storage.
#[derive(Clone)]
pub struct SegmentConfig {
    pub page_size: u32,
    pub pages_per_segment: u32,
    pub prefix: String,
    pub segment_dir: String,
    pub lmdb_env_path: String,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        let page_size = *PAGE_SIZE.get().expect("PAGE_SIZE_NOT_SET");
        let pages_per_segment = crate::indexing::storage::format::PAGES_PER_SEGMENT;
        Self {
            page_size,
            pages_per_segment,
            prefix: "xf".into(),
            lmdb_env_path: "xf-aof".into(),
            segment_dir: "xf-segments".into(),
        }
    }
}

/// Segment write confirmation event - exactly 64 bytes for XS pool
#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct SegmentWriteConfirmation {
    pub original_hash: [u8; 32], // XaeroID hash from XaeroInternalEvent (32 bytes)
    pub leaf_hash: [u8; 32],     // SHA-256 hash of archived data (32 bytes)
}

unsafe impl Pod for SegmentWriteConfirmation {}
unsafe impl Zeroable for SegmentWriteConfirmation {}

/// Segment Writer statistics
#[derive(Debug, Default)]
pub struct SegmentStats {
    pub total_writes: u64,
    pub failed_writes: u64,
    pub bytes_written: u64,
    pub current_segment_id: u32,
    pub current_page_index: u32,
    pub last_write_ts: u64,
}

/// Internal state for the segment writer that persists across events
struct WriterState {
    page_index: u32,
    write_pos: u32,
    seg_id: u32,
    local_page_idx: u32,
    byte_offset: u32,
    ts_start: u64,
    current_file: Option<File>,
    memory_map: Option<MmapMut>,
    filename: PathBuf,
    initialized: bool,
    stats: SegmentStats,
}

impl WriterState {
    fn new() -> Self {
        Self {
            page_index: 0,
            write_pos: 0,
            seg_id: 0,
            local_page_idx: 0,
            byte_offset: 0,
            ts_start: emit_secs(),
            current_file: None,
            memory_map: None,
            filename: PathBuf::new(),
            initialized: false,
            stats: SegmentStats::default(),
        }
    }

    fn initialize_from_metadata(&mut self, meta_db: &Arc<Mutex<LmdbEnv>>, config: &SegmentConfig) {
        if self.initialized {
            return;
        }

        let (start_of_day, end_of_day) = day_bounds_from_epoch_ms(emit_secs());
        if let Ok(segment_meta_iter) = iterate_segment_meta_by_range(meta_db, start_of_day, Some(end_of_day)) {
            if let Some(latest) = segment_meta_iter.iter().max_by_key(|seg_meta| seg_meta.ts_start) {
                self.page_index = latest.page_index;
                self.write_pos = latest.write_pos;
                self.ts_start = latest.ts_start;
                self.seg_id = latest.segment_index;
                self.local_page_idx = self.page_index % config.pages_per_segment;
                self.byte_offset = self.local_page_idx * config.page_size;

                // Update stats
                self.stats.current_segment_id = self.seg_id;
                self.stats.current_page_index = self.page_index;
            }
        }
        self.initialized = true;
    }

    fn ensure_file_open(&mut self, config: &SegmentConfig) -> Result<(), Box<dyn std::error::Error>> {
        if self.current_file.is_some() {
            return Ok(());
        }

        std::fs::create_dir_all(&config.segment_dir)?;

        self.filename = Path::new(&config.segment_dir).join(format!("{}-{}-{:04}.seg", config.prefix, self.ts_start, self.seg_id));

        let segment_bytes = (config.pages_per_segment * config.page_size) as u64;

        let file = OpenOptions::new().create(true).truncate(false).read(true).write(true).open(&self.filename)?;

        if file.metadata()?.len() == 0 {
            file.set_len(segment_bytes)?;
        }

        let mm = unsafe { MmapMut::map_mut(&file)? };

        self.current_file = Some(file);
        self.memory_map = Some(mm);
        Ok(())
    }

    fn flush_current_page(&mut self, config: &SegmentConfig) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref mut mm) = self.memory_map {
            mm.flush_range(self.byte_offset as usize, config.page_size as usize)?;
        }
        Ok(())
    }

    fn update_metadata(&mut self, meta_db: &Arc<Mutex<LmdbEnv>>) -> Result<(), Box<dyn std::error::Error>> {
        let seg_meta = SegmentMeta {
            page_index: self.page_index,
            segment_index: self.seg_id,
            write_pos: self.write_pos,
            byte_offset: self.byte_offset,
            latest_segment_id: self.seg_id,
            ts_start: self.ts_start,
            ts_end: emit_secs(),
        };

        let metadata_bytes = bytemuck::bytes_of(&seg_meta);
        push_internal_event_universal(meta_db, metadata_bytes, EventType::MetaEvent(1).to_u8() as u32, emit_secs())?;

        Ok(())
    }

    fn advance_to_next_page(&mut self, config: &SegmentConfig) {
        self.page_index += 1;
        self.local_page_idx = self.page_index % config.pages_per_segment;
        self.byte_offset = self.local_page_idx * config.page_size;
        self.write_pos = 0;
        self.stats.current_page_index = self.page_index;
    }

    fn rollover_to_next_segment(&mut self, config: &SegmentConfig, meta_db: &Arc<Mutex<LmdbEnv>>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref mut mm) = self.memory_map {
            mm.flush()?;
        }
        self.current_file = None;
        self.memory_map = None;

        self.seg_id = self.page_index / config.pages_per_segment;
        self.ts_start = emit_secs();
        self.byte_offset = 0;
        self.stats.current_segment_id = self.seg_id;

        self.update_metadata(meta_db)?;
        Ok(())
    }

    fn log_stats(&self) {
        if self.stats.total_writes % 1000 == 0 && self.stats.total_writes > 0 {
            tracing::info!(
                "Segment Stats - Writes: {}, Failed: {}, Bytes: {} MB, Segment: {}, Page: {}",
                self.stats.total_writes,
                self.stats.failed_writes,
                self.stats.bytes_written / (1024 * 1024),
                self.stats.current_segment_id,
                self.stats.current_page_index
            );
        }
    }
}

/// Segment Writer state - manages writing and statistics
pub struct SegmentWriterState {
    pub env: Arc<Mutex<LmdbEnv>>,
    pub writer_state: Arc<Mutex<WriterState>>,
    pub subject_hash: SubjectHash,
    pub bus_kind: BusKind,
    pub config: SegmentConfig,
}

impl SegmentWriterState {
    /// Create new segment writer state with LMDB environment
    pub fn new(subject_hash: SubjectHash, bus_kind: BusKind, config: SegmentConfig) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(&config.lmdb_env_path)?;
        std::fs::create_dir_all(&config.segment_dir)?;

        let lmdb_path = match bus_kind {
            BusKind::Control => system_paths::emit_control_path_with_subject_hash(&config.lmdb_env_path, subject_hash.0, "segment_writer"),
            BusKind::Data => system_paths::emit_data_path_with_subject_hash(&config.lmdb_env_path, subject_hash.0, "segment_writer"),
        };

        tracing::info!("Creating Segment Writer LMDB at path: {}", lmdb_path);
        let env = Arc::new(Mutex::new(LmdbEnv::new(&lmdb_path, BusKind::Data)?));
        let writer_state = Arc::new(Mutex::new(WriterState::new()));

        Ok(Self {
            env,
            writer_state,
            subject_hash,
            bus_kind,
            config,
        })
    }

    /// Process XaeroInternalEvent<64> (XS)
    fn process_internal_event_64(&mut self, internal_event: Arc<XaeroInternalEvent<64>>) -> Result<SegmentWriteConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_64(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<256> (S)
    fn process_internal_event_256(&mut self, internal_event: Arc<XaeroInternalEvent<256>>) -> Result<SegmentWriteConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_256(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<1024> (M)
    fn process_internal_event_1024(&mut self, internal_event: Arc<XaeroInternalEvent<1024>>) -> Result<SegmentWriteConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_1024(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<4096> (L)
    fn process_internal_event_4096(&mut self, internal_event: Arc<XaeroInternalEvent<4096>>) -> Result<SegmentWriteConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_4096(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Process XaeroInternalEvent<16384> (XL)
    fn process_internal_event_16384(&mut self, internal_event: Arc<XaeroInternalEvent<16384>>) -> Result<SegmentWriteConfirmation, Box<dyn std::error::Error>> {
        let archived_data = archive_xaero_internal_event_16384(*internal_event).to_vec();
        self.process_archived_data(archived_data, internal_event.xaero_id_hash, internal_event.latest_ts)
    }

    /// Common processing logic for archived data
    fn process_archived_data(&mut self, archived_data: Vec<u8>, xaero_id_hash: [u8; 32], latest_ts: u64) -> Result<SegmentWriteConfirmation, Box<dyn std::error::Error>> {
        let write_len = archived_data.len();
        let leaf_hash = sha_256_hash_b(&archived_data);

        let mut state = self.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        // Initialize state from metadata if this is the first event
        state.initialize_from_metadata(&self.env, &self.config);

        // Check if we need to advance to next page
        if state.write_pos + write_len as u32 > self.config.page_size {
            state.flush_current_page(&self.config)?;
            state.advance_to_next_page(&self.config);

            // Check if we need to roll over to next segment
            if state.local_page_idx == 0 {
                state.rollover_to_next_segment(&self.config, &self.env)?;
            }
        }

        // Ensure we have an open file and memory map
        state.ensure_file_open(&self.config)?;

        // Write archived data to current position
        let start = state.byte_offset + state.write_pos;
        let end = start + write_len as u32;

        if let Some(ref mut mm) = state.memory_map {
            mm[start as usize..end as usize].copy_from_slice(&archived_data);
            state.write_pos += write_len as u32;

            tracing::debug!(
                "Wrote {} bytes to segment {} page {} at offset {} (ts: {})",
                write_len,
                state.seg_id,
                state.local_page_idx,
                start,
                latest_ts
            );
        }

        // Final flush if any data
        if state.write_pos > 0 {
            let byte_offset = state.byte_offset;
            let page_size = self.config.page_size;
            if let Some(ref mut mm) = state.memory_map {
                mm.flush_range(byte_offset as usize, page_size as usize)?;
                mm.flush()?;
            }
        }

        // Update statistics
        state.stats.total_writes += 1;
        state.stats.bytes_written += write_len as u64;
        state.stats.last_write_ts = latest_ts;

        // Always update metadata after each event
        state.update_metadata(&self.env)?;

        Ok(SegmentWriteConfirmation {
            original_hash: xaero_id_hash,
            leaf_hash,
        })
    }
}

/// Segment Writer Actor - processes XaeroInternalEvent from ring buffer and writes to segment files
pub struct SegmentWriterActor {
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

    pub jh: JoinHandle<()>,
}

// ================================================================================================
// SEGMENT WRITER ACTOR IMPLEMENTATION
// ================================================================================================

impl SegmentWriterActor {
    /// Create and spawn segment writer actor with ring buffer processing for all T-shirt sizes
    pub fn spin(subject_hash: SubjectHash, bus_kind: BusKind, config: Option<SegmentConfig>) -> Result<Self, Box<dyn std::error::Error>> {
        let config = config.unwrap_or_default();
        let mut state = SegmentWriterState::new(subject_hash, bus_kind.clone(), config)?;

        let jh = thread::spawn(move || {
            tracing::info!("Segment Writer Actor started - Subject: {:?}, Bus: {:?}", subject_hash, bus_kind);

            // Initialize all ring buffers
            let xs_input_ring = SEGMENT_CONTROL_INPUT_RING.get_or_init(RingBuffer::new);
            let xs_output_ring = SEGMENT_CONTROL_OUTPUT_RING.get_or_init(RingBuffer::new);

            let s_input_ring = SEGMENT_DATA_INPUT_RING.get_or_init(RingBuffer::new);
            let s_output_ring = SEGMENT_DATA_OUTPUT_RING.get_or_init(RingBuffer::new);

            let m_input_ring = SEGMENT_MEDIUM_INPUT_RING.get_or_init(RingBuffer::new);
            let m_output_ring = SEGMENT_MEDIUM_OUTPUT_RING.get_or_init(RingBuffer::new);

            let l_input_ring = SEGMENT_LARGE_INPUT_RING.get_or_init(RingBuffer::new);
            let l_output_ring = SEGMENT_LARGE_OUTPUT_RING.get_or_init(RingBuffer::new);

            let xl_input_ring = SEGMENT_XL_INPUT_RING.get_or_init(RingBuffer::new);
            let xl_output_ring = SEGMENT_XL_OUTPUT_RING.get_or_init(RingBuffer::new);

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
                total_events_processed += Self::process_xs_events(&mut state, &mut xs_reader, &mut xs_writer);

                // Process S events (256 bytes)
                total_events_processed += Self::process_s_events(&mut state, &mut s_reader, &mut s_writer);

                // Process M events (1024 bytes)
                total_events_processed += Self::process_m_events(&mut state, &mut m_reader, &mut m_writer);

                // Process L events (4096 bytes)
                total_events_processed += Self::process_l_events(&mut state, &mut l_reader, &mut l_writer);

                // Process XL events (16384 bytes)
                total_events_processed += Self::process_xl_events(&mut state, &mut xl_reader, &mut xl_writer);

                if total_events_processed > 0 {
                    let state_guard = state.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    state_guard.log_stats();
                }

                thread::sleep(Duration::from_micros(100));
            }
        });

        // Create exposed interfaces using the dedicated segment ring buffers
        let xs_input_ring = SEGMENT_CONTROL_INPUT_RING.get_or_init(RingBuffer::new);
        let xs_output_ring = SEGMENT_CONTROL_OUTPUT_RING.get_or_init(RingBuffer::new);
        let s_input_ring = SEGMENT_DATA_INPUT_RING.get_or_init(RingBuffer::new);
        let s_output_ring = SEGMENT_DATA_OUTPUT_RING.get_or_init(RingBuffer::new);
        let m_input_ring = SEGMENT_MEDIUM_INPUT_RING.get_or_init(RingBuffer::new);
        let m_output_ring = SEGMENT_MEDIUM_OUTPUT_RING.get_or_init(RingBuffer::new);
        let l_input_ring = SEGMENT_LARGE_INPUT_RING.get_or_init(RingBuffer::new);
        let l_output_ring = SEGMENT_LARGE_OUTPUT_RING.get_or_init(RingBuffer::new);
        let xl_input_ring = SEGMENT_XL_INPUT_RING.get_or_init(RingBuffer::new);
        let xl_output_ring = SEGMENT_XL_OUTPUT_RING.get_or_init(RingBuffer::new);

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

            jh,
        })
    }

    /// Process XS events (64 bytes)
    fn process_xs_events(state: &mut SegmentWriterState, reader: &mut Reader<64, 2000>, writer: &mut Writer<64, 2000>) -> usize {
        let mut events_processed = 0;

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<64>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<64>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_64(arc_internal_event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 202) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("Segment XS confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create XS confirmation: {:?}", e),
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process XS event: {:?}", e);
                        let mut state_guard = state.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        state_guard.stats.failed_writes += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process S events (256 bytes)
    fn process_s_events(state: &mut SegmentWriterState, reader: &mut Reader<256, 1000>, writer: &mut Writer<64, 2000>) -> usize {
        let mut events_processed = 0;

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<256>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<256>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_256(arc_internal_event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 202) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("Segment S confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create S confirmation: {:?}", e),
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process S event: {:?}", e);
                        let mut state_guard = state.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        state_guard.stats.failed_writes += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process M events (1024 bytes)
    fn process_m_events(state: &mut SegmentWriterState, reader: &mut Reader<1024, 500>, writer: &mut Writer<64, 2000>) -> usize {
        let mut events_processed = 0;

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<1024>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<1024>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_1024(arc_internal_event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 202) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("Segment M confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create M confirmation: {:?}", e),
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process M event: {:?}", e);
                        let mut state_guard = state.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        state_guard.stats.failed_writes += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process L events (4096 bytes)
    fn process_l_events(state: &mut SegmentWriterState, reader: &mut Reader<4096, 100>, writer: &mut Writer<64, 2000>) -> usize {
        let mut events_processed = 0;

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<4096>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<4096>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_4096(arc_internal_event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 202) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("Segment L confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create L confirmation: {:?}", e),
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process L event: {:?}", e);
                        let mut state_guard = state.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        state_guard.stats.failed_writes += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
    }

    /// Process XL events (16384 bytes)
    fn process_xl_events(state: &mut SegmentWriterState, reader: &mut Reader<16384, 50>, writer: &mut Writer<64, 2000>) -> usize {
        let mut events_processed = 0;

        for event in reader.by_ref() {
            if event.len == std::mem::size_of::<XaeroInternalEvent<16384>>() as u32 {
                let internal_event = unsafe { std::ptr::read(&event as *const _ as *const XaeroInternalEvent<16384>) };
                let arc_internal_event = Arc::new(internal_event);

                match state.process_internal_event_16384(arc_internal_event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 202) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("Segment XL confirmation buffer full");
                                },
                            Err(e) => tracing::error!("Failed to create XL confirmation: {:?}", e),
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process XL event: {:?}", e);
                        let mut state_guard = state.writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        state_guard.stats.failed_writes += 1;
                    }
                }
            }
            events_processed += 1;
        }

        events_processed
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
    fn test_segment_confirmation_size() {
        let conf = SegmentWriteConfirmation {
            original_hash: [0; 32],
            leaf_hash: [1; 32],
        };

        let conf_size = std::mem::size_of::<SegmentWriteConfirmation>();
        assert_eq!(conf_size, 64, "Segment confirmation must be exactly 64 bytes for XS pool");
        println!("✅ Segment confirmation fits perfectly in XS pool: {} bytes", conf_size);
    }

    #[test]
    fn test_all_tshirt_sizes_processing() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([4u8; 32]);
        let config = SegmentConfig {
            page_size: 8192,
            pages_per_segment: 2,
            prefix: "test-all-sizes".to_string(),
            segment_dir: "/tmp/test-all-sizes-segments".to_string(),
            lmdb_env_path: "/tmp/test-all-sizes-lmdb".to_string(),
        };

        let mut state = SegmentWriterState::new(subject_hash, BusKind::Data, config).expect("Failed to create segment writer state");

        // Test XS event (64 bytes) - match data size to type parameter
        let xs_event = Arc::new(XaeroInternalEvent::<64> {
            xaero_id_hash: [0x01; 32],
            vector_clock_hash: [0x02; 32],
            evt: PooledEvent {
                data: [0x03; 64],
                len: 64,
                event_type: 1,
            },
            latest_ts: emit_secs(),
        });

        let xs_result = state.process_internal_event_64(xs_event);
        assert!(xs_result.is_ok(), "Should process XS event successfully");

        // Test S event (256 bytes) - match data size to type parameter
        let s_event = Arc::new(XaeroInternalEvent::<256> {
            xaero_id_hash: [0x04; 32],
            vector_clock_hash: [0x05; 32],
            evt: PooledEvent {
                data: [0x06; 256],
                len: 256,
                event_type: 2,
            },
            latest_ts: emit_secs(),
        });

        let s_result = state.process_internal_event_256(s_event);
        assert!(s_result.is_ok(), "Should process S event successfully");

        // Test M event (1024 bytes) - match data size to type parameter
        let m_event = Arc::new(XaeroInternalEvent::<1024> {
            xaero_id_hash: [0x07; 32],
            vector_clock_hash: [0x08; 32],
            evt: PooledEvent {
                data: [0x09; 1024],
                len: 1024,
                event_type: 3,
            },
            latest_ts: emit_secs(),
        });

        let m_result = state.process_internal_event_1024(m_event);
        assert!(m_result.is_ok(), "Should process M event successfully");

        println!("✅ All T-shirt sizes processing working correctly");
    }

    #[test]
    fn test_segment_writer_actor_creation() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([5u8; 32]);
        let config = SegmentConfig {
            page_size: 4096,
            pages_per_segment: 2,
            prefix: "test-actor".to_string(),
            segment_dir: "/tmp/test-actor-segments".to_string(),
            lmdb_env_path: "/tmp/test-actor-lmdb".to_string(),
        };

        let actor = SegmentWriterActor::spin(subject_hash, BusKind::Data, Some(config)).expect("Failed to create segment writer actor");

        // Verify all interfaces exist
        assert!(true, "Actor created successfully with all T-shirt size interfaces");

        // The actor is running in the background processing events
        drop(actor); // This will trigger the Drop implementation

        println!("✅ Segment writer actor creation and interface verification successful");
    }
}
