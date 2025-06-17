//! Segment Writer Actor for xaeroflux-actors.
//!
//! This module provides:
//! - `SegmentConfig`: configuration parameters for paging and segmentation.
//! - `SegmentWriterActor`: an actor that listens for archived event blobs and writes them into
//!   fixed-size pages within segment files.
//! - Unit tests verifying segment math, flush behavior, and rollover/resume logic.

use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use crossbeam::channel::SendError;
use memmap2::MmapMut;
use xaeroflux_core::{
    XAERO_DISPATCHER_POOL,
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{
        Event, EventType, EventType::SystemEvent, ScanWindow, SystemEventKind, SystemEventKind::Shutdown, XaeroEvent,
    },
    hash::sha_256_hash_b,
    listeners::EventListener,
    size::PAGE_SIZE,
    system_paths::{emit_control_path_with_subject_hash, emit_data_path_with_subject_hash},
};

use crate::{
    BusKind, Pipe,
    aof::storage::{
        format::SegmentMeta,
        lmdb::{LmdbEnv, push_event},
        meta::iterate_segment_meta_by_range,
    },
    indexing::storage::format::archive,
    subject::SubjectHash,
    system_payload::SystemPayload,
};

// thread_local! {
//     // Each thread gets its own Vec with pre-allocated capacity
//     static ARCHIVE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(8192));
//     static PAYLOAD_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(512));
// }

pub static NAME_PREFIX: &str = "segment_writer";

/// Configuration for paged segment storage.
#[derive(Clone)]
pub struct SegmentConfig {
    pub page_size: usize,
    pub pages_per_segment: usize,
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

/// Internal state for the segment writer that persists across events
struct WriterState {
    page_index: usize,
    write_pos: usize,
    seg_id: usize,
    local_page_idx: usize,
    byte_offset: usize,
    ts_start: u64,
    current_file: Option<File>,
    memory_map: Option<MmapMut>,
    filename: PathBuf,
    initialized: bool,
    // archive_buffer: Vec<u8>,
    // payload_buffer: Vec<u8>
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
            }
        }
        self.initialized = true;
    }

    fn ensure_file_open(&mut self, config: &SegmentConfig) -> Result<(), Box<dyn std::error::Error>> {
        if self.current_file.is_some() {
            return Ok(());
        }

        std::fs::create_dir_all(&config.segment_dir)?;

        self.filename =
            Path::new(&config.segment_dir).join(format!("{}-{}-{:04}.seg", config.prefix, self.ts_start, self.seg_id));

        let segment_bytes = (config.pages_per_segment * config.page_size) as u64;

        // Use append mode to avoid truncating existing data
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.filename)?;

        // Only set length if file is new/empty
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
            mm.flush_range(self.byte_offset, config.page_size)?;
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

        let data_b_segment_meta = bytemuck::bytes_of(&seg_meta);
        push_event(
            meta_db,
            &Event::new(data_b_segment_meta.to_vec(), EventType::MetaEvent(1).to_u8()),
        )?;

        Ok(())
    }

    fn advance_to_next_page(&mut self, config: &SegmentConfig) {
        self.page_index += 1;
        self.local_page_idx = self.page_index % config.pages_per_segment;
        self.byte_offset = self.local_page_idx * config.page_size;
        self.write_pos = 0;
    }

    fn rollover_to_next_segment(
        &mut self,
        config: &SegmentConfig,
        meta_db: &Arc<Mutex<LmdbEnv>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Flush and close current file
        if let Some(ref mut mm) = self.memory_map {
            mm.flush()?;
        }
        self.current_file = None;
        self.memory_map = None;

        // Update segment info
        self.seg_id = self.page_index / config.pages_per_segment;
        self.ts_start = emit_secs();
        self.byte_offset = 0;

        // Save metadata for rollover with immediate flush
        self.update_metadata(meta_db)?;

        Ok(())
    }

    fn current_segment_meta(&self) -> SegmentMeta {
        SegmentMeta {
            page_index: self.page_index,
            segment_index: self.seg_id,
            write_pos: self.write_pos,
            byte_offset: self.byte_offset,
            latest_segment_id: self.seg_id,
            ts_start: self.ts_start,
            ts_end: emit_secs(),
        }
    }
}

/// Actor responsible for writing serialized event frames into segment files.
pub struct SegmentWriterActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    pub meta_db: Arc<Mutex<LmdbEnv>>,
    pub segment_config: SegmentConfig,
    _xaero_event_loop_handle: Option<std::thread::JoinHandle<()>>,
    _event_handler_loop_handle: Option<std::thread::JoinHandle<()>>,
}
impl Drop for SegmentWriterActor {
    fn drop(&mut self) {
        let res = self.pipe.sink.tx.send(XaeroEvent {
            evt: Event::new(vec![], SystemEvent(Shutdown).to_u8()),
            merkle_proof: None,
            author_id: None,
            latest_ts: None,
        });
        match res {
            Ok(_) => {
                tracing::debug!("MmrIndexingActor :: Shutdown initiated");
            }
            Err(e) => {
                tracing::error!("MmrIndexingActor :: Error sending shutdown event: {:?}", e);
            }
        }
        if let Some(handle) = self._xaero_event_loop_handle.take() {
            if let Err(e) = handle.join() {
                tracing::error!("Failed to join xaero event loop thread: {:?}", e);
            }
        }

        if let Some(handle) = self._event_handler_loop_handle.take() {
            if let Err(e) = handle.join() {
                tracing::error!("Failed to join event handler thread: {:?}", e);
            }
        }
    }
}
impl SegmentWriterActor {
    /// Create a `SegmentWriterActor` with default settings.
    pub fn new(name: SubjectHash, pipe: Arc<Pipe>) -> Self {
        Self::new_with_config(name, pipe, SegmentConfig::default())
    }

    /// Create a `SegmentWriterActor` with a custom configuration.
    pub fn new_with_config(name: SubjectHash, pipe: Arc<Pipe>, config: SegmentConfig) -> Self {
        std::fs::create_dir_all(&config.lmdb_env_path).expect("failed to create directory");
        std::fs::create_dir_all(&config.segment_dir).expect("failed to create segment directory");

        let cfc = config.clone();
        let meta_db = match pipe.sink.kind {
            BusKind::Control => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_control_path_with_subject_hash(config.lmdb_env_path.as_str(), name.0, NAME_PREFIX).as_str(),
                    BusKind::Data,
                )
                .expect("failed_to_create_lmdb_env"),
            )),
            BusKind::Data => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_data_path_with_subject_hash(config.lmdb_env_path.as_str(), name.0, NAME_PREFIX).as_str(),
                    BusKind::Data,
                )
                .expect("failed_to_create_lmdb_env"),
            )),
        };

        let metadb_clone = meta_db.clone();
        let pc_c1 = pipe.clone();

        // Create persistent writer state
        let writer_state = Arc::new(Mutex::new(WriterState::new()));
        let state_clone = writer_state.clone();
        let (buffer_tx, buffer_rx) = crossbeam::channel::unbounded();
        let listener = EventListener::new(
            "segment_writer_actor",
            Arc::new(move |event: Event<Vec<u8>>| {
                let res = buffer_tx.send(event);
                match res {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("failed to send event: {}", e);
                    }
                }
            }),
            None,
            Some(1), // single-threaded handler
        );

        let pipe_clone0 = pc_c1.clone();
        let xeh = thread::Builder::new()
            .name(format!("xaeroflux-seg-writer-actor-{}", hex::encode(name.0)))
            .spawn(move || {
                while let Ok(framed) = pipe_clone0.sink.rx.recv() {
                    if (framed.evt.event_type == SystemEvent(Shutdown)) {
                        tracing::warn!("sending Shutdown to listener");
                        if let Err(e) = listener.inbox.send(framed.evt) {
                            tracing::error!("Failed to send event to listener: {}", e);
                        }
                        break;
                    }
                    if let Err(e) = listener.inbox.send(framed.evt) {
                        tracing::error!("Failed to send event to listener: {}", e);
                    }
                }
            })
            .expect("failed to spawn xaeroflux-seg-writer-actor thread");

        // looper
        let _event_loop_handle = thread::Builder::new()
            .spawn(move || {
                while let Ok(event) = buffer_rx.recv() {
                    if let Err(e) = Self::handle_event(&event, &config, &metadb_clone, &pipe, &state_clone) {
                        tracing::error!("Failed to handle event: {}", e);
                    }
                }
            })
            .expect("failed to spawn xaeroflux-seg-writer-actor event loop");

        SegmentWriterActor {
            name,
            pipe: pc_c1,
            meta_db,
            segment_config: cfc,
            _xaero_event_loop_handle: Some(xeh),
            _event_handler_loop_handle: Some(_event_loop_handle),
        }
    }

    fn handle_event(
        event: &Event<Vec<u8>>,
        config: &SegmentConfig,
        meta_db: &Arc<Mutex<LmdbEnv>>,
        pipe: &Arc<Pipe>,
        writer_state: &Arc<Mutex<WriterState>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let data = archive(event);
        let write_len = data.len();
        let leaf_hash = sha_256_hash_b(&data);

        let mut state = writer_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        // Initialize state from metadata if this is the first event
        state.initialize_from_metadata(meta_db, config);

        // Check if we need to advance to next page
        if state.write_pos + write_len > config.page_size {
            state.flush_current_page(config)?;
            state.advance_to_next_page(config);

            // Check if we need to roll over to next segment
            if state.local_page_idx == 0 {
                state.rollover_to_next_segment(config, meta_db)?;
            }
        }

        // Ensure we have an open file and memory map
        state.ensure_file_open(config)?;

        // Write data to current position
        let start = state.byte_offset + state.write_pos;
        let end = start + write_len;
        let seg_id = state.seg_id;
        let local_page_idx = state.local_page_idx;

        if let Some(ref mut mm) = state.memory_map {
            mm[start..end].copy_from_slice(&data);
            state.write_pos += write_len;

            tracing::debug!(
                "Wrote {} bytes to segment {} page {} at offset {}",
                write_len,
                seg_id,
                local_page_idx,
                start
            );
        }

        // Send PayloadWritten event
        let bytes_of_payload_written = bytemuck::bytes_of::<SystemPayload>(&SystemPayload::PayloadWritten {
            leaf_hash,
            meta: state.current_segment_meta(),
        })
        .to_vec();

        if let Err(e) = pipe.source.tx.send(XaeroEvent {
            evt: Event::new(
                bytes_of_payload_written,
                EventType::SystemEvent(SystemEventKind::PayloadWritten).to_u8(),
            ),
            merkle_proof: None,
            author_id: None,
            latest_ts: None,
        }) {
            tracing::error!("Failed to send PayloadWritten message: {}", e);
        } else {
            tracing::debug!("Payload written message sent successfully");
        }

        // Final flush if any data
        let write_pos = state.write_pos;
        let byte_offset = state.byte_offset;
        if write_pos > 0 {
            if let Some(ref mut mm) = state.memory_map {
                mm.flush_range(byte_offset, config.page_size)?;
                mm.flush()?; // Ensure all data is persisted to disk
            }
        }

        // Always update metadata after each event to keep LMDB in sync
        state.update_metadata(meta_db)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread::sleep, time::Duration};

    use serial_test::serial;
    use tempfile::tempdir;
    use xaeroflux_core::{event::EventType, init_xaero_pool, initialize, shutdown_all_pools};

    use super::*;
    use crate::indexing::storage::format::archive;

    // Helper to wrap an Event<Vec<u8>> into a XaeroEvent and send it via pipe.
    fn send_app_event(pipe: &Arc<Pipe>, data: Vec<u8>) {
        let e = Event::new(data, EventType::ApplicationEvent(1).to_u8());
        let xaero_evt = XaeroEvent {
            evt: e,
            merkle_proof: None,
            author_id: None,
            latest_ts: None,
        };
        pipe.sink.tx.send(xaero_evt).expect("failed to send event");
    }

    #[test]
    #[serial]
    fn test_segment_meta_initial() {
        initialize();
        let dir = tempdir().expect("failed to unpack tempdir");
        let arc_env = Arc::new(std::sync::Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap"), BusKind::Data).expect("failed_to_unwrap"),
        ));

        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: 0,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 0,
            ts_start: emit_secs(),
            ts_end: emit_secs(),
        };
        let bytes = bytemuck::bytes_of(&seg_meta).to_vec();
        let ev = Event::new(bytes, EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &ev).expect("failed_to_unwrap");

        // Verify metadata was stored correctly - use local variables for packed struct access
        let g_p_idx = seg_meta.page_index;
        let g_s_idx = seg_meta.segment_index;
        let g_w_pos = seg_meta.write_pos;
        let g_b_off = seg_meta.byte_offset;
        let g_l_s_id = seg_meta.latest_segment_id;

        assert_eq!(g_p_idx, 0);
        assert_eq!(g_s_idx, 0);
        assert_eq!(g_w_pos, 0);
        assert_eq!(g_b_off, 0);
        assert_eq!(g_l_s_id, 0);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown mmr actor: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn test_page_segment_math() {
        initialize();
        const PAGE_SIZE: usize = 16;
        const PAGES_PER_SEGMENT: usize = 4;
        let cases = [
            (0, 0, 0, 0),
            (1, 0, 1, 16),
            (3, 0, 3, 48),
            (4, 1, 0, 0),
            (5, 1, 1, 16),
            (7, 1, 3, 48),
            (8, 2, 0, 0),
        ];
        for (idx, seg, local, offset) in cases {
            assert_eq!(idx / PAGES_PER_SEGMENT, seg);
            assert_eq!(idx % PAGES_PER_SEGMENT, local);
            assert_eq!(local * PAGE_SIZE, offset);
        }
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown mmr actor: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn test_actor_writes_single_event() {
        initialize();
        let tmp = tempdir().expect("failed to create tempdir");

        let cfg = SegmentConfig {
            page_size: 1024,
            pages_per_segment: 2,
            prefix: "test".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Data, None);
        let subject_hash = SubjectHash::from([0u8; 32]);
        let _actor = SegmentWriterActor::new_with_config(subject_hash, pipe.clone(), cfg.clone());

        // Send a test event
        let payload = b"test payload".to_vec();
        send_app_event(&pipe, payload.clone());

        // Wait for processing
        sleep(Duration::from_millis(100));

        // Verify segment file was created
        let files: Vec<_> = std::fs::read_dir(&cfg.segment_dir)
            .expect("failed to read segment dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("seg"))
            .collect();

        assert!(!files.is_empty(), "No segment files were created");
        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown mmr actor: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn test_actor_page_boundary_handling() {
        initialize();
        let tmp = tempdir().expect("failed to create tempdir");

        // Create events that will fill exactly one page each
        let ev1 = Event::new(vec![1; 50], EventType::ApplicationEvent(1).to_u8());
        let ev2 = Event::new(vec![2; 50], EventType::ApplicationEvent(1).to_u8());

        let framed1 = archive(&ev1);
        let framed2 = archive(&ev2);

        let cfg = SegmentConfig {
            page_size: framed1.len().max(framed2.len()), // Exactly fit one event per page
            pages_per_segment: 2,
            prefix: "test-boundary".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Data, None);
        let subject_hash = SubjectHash::from([0u8; 32]);
        let _actor = SegmentWriterActor::new_with_config(subject_hash, pipe.clone(), cfg.clone());

        // Send both events
        send_app_event(&pipe, vec![1; 50]);
        send_app_event(&pipe, vec![2; 50]);

        // Wait for processing
        sleep(Duration::from_millis(200));

        // Verify segment file exists
        let files: Vec<_> = std::fs::read_dir(&cfg.segment_dir)
            .expect("failed to read segment dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("seg"))
            .collect();

        assert_eq!(files.len(), 1, "Expected exactly one segment file");
        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown mmr actor: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn test_actor_segment_rollover() {
        initialize();
        let tmp = tempdir().expect("failed to create tempdir");

        // Create small pages and segments to force rollover
        let payload_size = 20;
        let event = Event::new(vec![42; payload_size], EventType::ApplicationEvent(1).to_u8());
        let framed = archive(&event);

        let cfg = SegmentConfig {
            page_size: framed.len(), // Exactly one event per page
            pages_per_segment: 2,    // Only 2 pages per segment
            prefix: "test-rollover".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Data, None);
        let subject_hash = SubjectHash::from([0u8; 32]);
        let _actor = SegmentWriterActor::new_with_config(subject_hash, pipe.clone(), cfg.clone());

        // Send 4 events to force rollover (2 pages per segment = 2 events per segment)
        for i in 1..=4 {
            send_app_event(&pipe, vec![i; payload_size]);
        }

        // Wait for processing
        sleep(Duration::from_millis(300));

        // Verify multiple segment files were created
        let files: Vec<_> = std::fs::read_dir(&cfg.segment_dir)
            .expect("failed to read segment dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("seg"))
            .collect();

        assert!(
            files.len() >= 2,
            "Expected at least 2 segment files after rollover, got {}",
            files.len()
        );
        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown mmr actor: {:?}", e);
            }
        }
    }

    #[test]
    fn test_actor_sends_payload_written_events() {
        initialize();
        init_xaero_pool();
        let tmp = tempdir().expect("failed to create tempdir");

        let cfg = SegmentConfig {
            page_size: 1024,
            pages_per_segment: 2,
            prefix: "test".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Data, None);
        let rx_out = pipe.source.rx.clone();
        let subject_hash = SubjectHash::from([0u8; 32]);
        let _actor = SegmentWriterActor::new_with_config(subject_hash, pipe.clone(), cfg);

        // Send a test event
        let payload = b"test payload".to_vec();
        send_app_event(&pipe, payload);

        // Wait for processing
        sleep(Duration::from_millis(100));

        // Try to receive PayloadWritten event
        let mut received_payload_written = false;
        let timeout = Duration::from_millis(500);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match rx_out.try_recv() {
                Ok(event) =>
                    if event.evt.event_type == EventType::SystemEvent(SystemEventKind::PayloadWritten) {
                        received_payload_written = true;
                        break;
                    },
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    panic!("Channel error: {:?}", e);
                }
            }
        }

        assert!(received_payload_written, "Expected to receive PayloadWritten event");
        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown mmr actor: {:?}", e);
            }
        }
    }
}
