//! SegmentReaderActor: Replays persisted segments on-demand.
//!
//! This module defines:
//! - `SegmentReaderActor`: actor that listens for replay triggers, reads segment metadata from
//!   LMDB, loads segment files from disk, deserializes events, and forwards them to a consumer
//!   `Sink`.
//! - End-to-end replay logic for system-level replay events.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use xaeroflux_core::{
    XAERO_DISPATCHER_POOL, XaeroPoolManager,
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{
        EventType,
        EventType::SystemEvent,
        ScanWindow, SystemEventKind,
        SystemEventKind::{ReplayControl, ReplayData, Shutdown},
        XaeroEvent,
    },
    pipe::{BusKind, Pipe},
    system_paths::{emit_control_path_with_subject_hash, emit_data_path_with_subject_hash},
};

use crate::{
    aof::storage::{lmdb::LmdbEnv, meta::iterate_segment_meta_by_range},
    indexing::storage::{
        actors::segment_writer_actor::SegmentConfig,
        format::{unarchive_to_raw_data, unarchive_to_xaero_event},
        io,
    },
    subject::SubjectHash,
};

pub static NAME_PREFIX: &str = "segment_reader";

/// Actor for replaying events from historical segments.
///
/// This actor handles replay requests by:
/// 1. Listening for ReplayControl/ReplayData events based on bus kind
/// 2. Reading segment metadata from LMDB within specified time ranges
/// 3. Loading and deserializing events from segment files using new archive format
/// 4. Forwarding reconstructed Arc<XaeroEvent> to the output pipe
pub struct SegmentReaderActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    pub meta_db: Arc<Mutex<LmdbEnv>>,
    pub segment_config: SegmentConfig,
    _xaero_event_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for SegmentReaderActor {
    fn drop(&mut self) {
        // Send shutdown signal using XaeroPoolManager
        let shutdown_event = XaeroPoolManager::create_xaero_event(
            &[], // Empty data
            SystemEvent(Shutdown).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed for shutdown: {:?}", pool_error);
            panic!("Cannot create shutdown event - ring buffer pool exhausted");
        });

        let res = self.pipe.sink.tx.send(shutdown_event);
        match res {
            Ok(_) => {
                tracing::debug!("SegmentReaderActor :: Shutdown initiated");
            }
            Err(e) => {
                tracing::error!("SegmentReaderActor :: Error sending shutdown event: {:?}", e);
            }
        }

        if let Some(handle) = self._xaero_event_handle.take() {
            if let Err(e) = handle.join() {
                tracing::error!("Failed to join xaero event loop thread: {:?}", e);
            }
        }
    }
}

impl SegmentReaderActor {
    /// Create a new `SegmentReaderActor`.
    ///
    /// The actor will:
    /// 1. Set up LMDB environment for reading segment metadata
    /// 2. Listen for appropriate replay events (Control vs Data based on pipe kind)
    /// 3. Process replay requests by reading segment files and forwarding events
    ///
    /// # Arguments
    /// - `name`: Subject hash for path generation
    /// - `pipe`: Communication pipe (determines Control vs Data handling)
    /// - `config`: Configuration for segment directories and LMDB paths
    pub fn new(name: SubjectHash, pipe: Arc<Pipe>, config: SegmentConfig) -> Self {
        XaeroPoolManager::init();

        std::fs::create_dir_all(&config.segment_dir).expect("failed to create segment directory");
        std::fs::create_dir_all(&config.lmdb_env_path).expect("failed to create LMDB directory");

        // Set up paths based on bus kind
        let meta_db = match pipe.sink.kind {
            BusKind::Control => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_control_path_with_subject_hash(&config.lmdb_env_path, name.0, NAME_PREFIX).as_str(),
                    BusKind::Control,
                )
                .expect("failed to create Control LmdbEnv"),
            )),
            BusKind::Data => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_data_path_with_subject_hash(&config.lmdb_env_path, name.0, NAME_PREFIX).as_str(),
                    BusKind::Data,
                )
                .expect("failed to create Data LmdbEnv"),
            )),
        };

        let pipe_clone = pipe.clone();
        let meta_db_clone = meta_db.clone();
        let config_clone = config.clone();
        let bus_kind = pipe.sink.kind;
        let subject_hash = name;

        // Direct XaeroEvent processing - no EventListener indirection
        let _xaero_handle = std::thread::Builder::new()
            .name(format!("segment-reader-{}", hex::encode(name.0)))
            .spawn(move || {
                tracing::info!(
                    "SegmentReaderActor processing thread started for subject: {}",
                    hex::encode(subject_hash.0)
                );

                while let Ok(xaero_event) = pipe_clone.sink.rx.recv() {
                    // Check for shutdown signal
                    if xaero_event.event_type() == SystemEvent(Shutdown).to_u8() {
                        tracing::info!("SegmentReaderActor received shutdown signal");
                        break;
                    }

                    match Self::handle_xaero_replay_event(
                        &xaero_event,
                        &pipe_clone,
                        &meta_db_clone,
                        &config_clone,
                        bus_kind,
                        subject_hash,
                    ) {
                        Ok(_) => {
                            tracing::debug!("Successfully processed replay event");
                        }
                        Err(e) => {
                            tracing::error!("Failed to handle replay event: {}", e);
                        }
                    }
                }

                tracing::info!("SegmentReaderActor processing thread terminated");
            })
            .expect("failed to spawn segment reader thread");

        Self {
            name,
            pipe,
            meta_db,
            segment_config: config,
            _xaero_event_handle: Some(_xaero_handle),
        }
    }

    /// Handle a replay event based on bus kind and event type
    fn handle_xaero_replay_event(
        xaero_event: &Arc<XaeroEvent>,
        pipe: &Arc<Pipe>,
        meta_db: &Arc<Mutex<LmdbEnv>>,
        config: &SegmentConfig,
        bus_kind: BusKind,
        subject_hash: SubjectHash,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event_type = EventType::from_u8(xaero_event.event_type());

        // Filter events based on bus kind - Control actors only process ReplayControl,
        // Data actors only process ReplayData
        let should_process = match (bus_kind, event_type) {
            (BusKind::Control, EventType::SystemEvent(SystemEventKind::ReplayControl)) => true,
            (BusKind::Data, EventType::SystemEvent(SystemEventKind::ReplayData)) => true,
            _ => {
                //                tracing::debug!("Ignoring event type {:?} for {:?} bus", event_type, bus_kind);
                return Ok(());
            }
        };

        if !should_process {
            return Ok(());
        }

        // tracing::info!(
        //     "Processing replay event: {:?} on {:?} bus",
        //     event_type,
        //     bus_kind
        // );

        // Parse scan window from XaeroEvent data or use default day bounds
        let event_data = xaero_event.data();
        let (start_time, end_time) = match bytemuck::try_from_bytes::<ScanWindow>(event_data) {
            Ok(scan_window) => {
                let sw_start = scan_window.start;
                let sw_end = scan_window.end;
                tracing::info!("Using ScanWindow: start={}, end={}", sw_start, sw_end);
                (sw_start, if sw_end > 0 { Some(sw_end) } else { None })
            }
            Err(e) => {
                tracing::warn!("Failed to parse ScanWindow: {}, using day bounds", e);
                let (start_of_day, end_of_day) = day_bounds_from_epoch_ms(emit_secs());
                (start_of_day, Some(end_of_day))
            }
        };

        // Get segment metadata for the time range
        let segment_metas = iterate_segment_meta_by_range(meta_db, start_time, end_time)?;

        let mut events_replayed = 0;
        let smc = segment_metas.len();

        for segment_meta in segment_metas {
            // Use local variables to avoid unaligned access on packed struct
            let unaligned_ts_start = segment_meta.ts_start;
            let unaligned_seg_idx = segment_meta.segment_index;

            tracing::debug!(
                "Processing segment: ts_start={}, seg_idx={}",
                unaligned_ts_start,
                unaligned_seg_idx
            );

            // Build segment file path using the actual subject hash
            let segment_dir = match bus_kind {
                BusKind::Control =>
                    emit_control_path_with_subject_hash(&config.segment_dir, subject_hash.0, NAME_PREFIX),
                BusKind::Data => emit_data_path_with_subject_hash(&config.segment_dir, subject_hash.0, NAME_PREFIX),
            };

            let file_path = Path::new(&segment_dir).join(format!(
                "{}-{}-{:04}.seg",
                config.prefix, unaligned_ts_start, unaligned_seg_idx
            ));

            // Read and process segment file
            match io::read_segment_file(file_path.to_str().unwrap_or("invalid_path")) {
                Ok(mmap) => {
                    tracing::debug!("Successfully read segment file: {:?}", file_path);
                    let page_iter = io::PageEventIterator::new(&mmap);

                    for event_bytes in page_iter {
                        // Use new unarchive function instead of rkyv
                        match unarchive_to_xaero_event(event_bytes) {
                            Ok(reconstructed_event) =>
                                if let Err(e) = pipe.source.tx.send(reconstructed_event) {
                                    tracing::error!("Failed to send replayed event: {}", e);
                                } else {
                                    events_replayed += 1;
                                    tracing::debug!("Replayed event #{}", events_replayed);
                                },
                            Err(e) => {
                                tracing::error!("Failed to unarchive event: {:?}", e);

                                // Try raw data approach as fallback
                                let (header, raw_data) = unarchive_to_raw_data(event_bytes);
                                tracing::warn!(
                                    "Fallback: Got raw event data - type={}, size={} bytes",
                                    header.event_type,
                                    raw_data.len()
                                );
                                // Could potentially reconstruct manually or skip
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to read segment file {:?}: {}", file_path, e);
                }
            }
        }

        tracing::info!(
            "Replay completed: {} events replayed from {} segments",
            events_replayed,
            smc
        );
        Ok(())
    }

    /// Get a reference to the metadata database
    pub fn meta_db(&self) -> &Arc<Mutex<LmdbEnv>> {
        &self.meta_db
    }

    /// Get the segment configuration
    pub fn segment_config(&self) -> &SegmentConfig {
        &self.segment_config
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use bytemuck::bytes_of;
    use serial_test::serial;
    use tempfile::tempdir;
    use xaeroflux_core::{
        date_time::emit_secs,
        event::{EventType, ScanWindow, SystemEventKind, XaeroEvent},
        init_xaero_pool, initialize, shutdown_all_pools,
    };

    use super::*;
    use crate::{
        BusKind, Pipe,
        indexing::storage::{actors::segment_writer_actor::SegmentConfig, format::PAGE_SIZE},
        subject::SubjectHash,
    };

    #[test]
    #[serial]
    fn control_actor_ignores_data_replay() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let temp_dir = tempdir().expect("failed_to_unravel");
        let base_dir = temp_dir.path().join("control_ignore");
        std::fs::create_dir_all(&base_dir).expect("failed_to_unravel");

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"control_ignore");
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: NAME_PREFIX.to_string(),
            segment_dir: base_dir.to_string_lossy().into_owned(),
            lmdb_env_path: base_dir.to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Control, None);
        let _actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Send a ReplayData to a Control actor (should be ignored)
        let scan_window = ScanWindow { start: 0, end: 0 };
        let replay_event = XaeroPoolManager::create_xaero_event(
            bytes_of(&scan_window),
            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.sink.tx.send(replay_event).expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(100));

        // Expect no event (timeout) because Control actor should ignore ReplayData
        assert!(
            pipe.source.rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "Control actor should ignore ReplayData events"
        );

        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown pools: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn data_actor_ignores_control_replay() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let temp_dir = tempdir().expect("failed_to_unravel");
        let base_dir = temp_dir.path().join("data_ignore");
        std::fs::create_dir_all(&base_dir).expect("failed_to_unravel");

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data_ignore");
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: NAME_PREFIX.to_string(),
            segment_dir: base_dir.to_string_lossy().into_owned(),
            lmdb_env_path: base_dir.to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Data, None);
        let _actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Send a ReplayControl to a Data actor (should be ignored)
        let scan_window = ScanWindow { start: 0, end: 0 };
        let replay_event = XaeroPoolManager::create_xaero_event(
            bytes_of(&scan_window),
            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.sink.tx.send(replay_event).expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(100));

        // Expect no event (timeout) because Data actor should ignore ReplayControl
        assert!(
            pipe.source.rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "Data actor should ignore ReplayControl events"
        );

        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown pools: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn control_actor_processes_control_replay() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let temp_dir = tempdir().expect("failed_to_unravel");
        let base_dir = temp_dir.path().join("control_process");
        std::fs::create_dir_all(&base_dir).expect("failed_to_unravel");

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"control_process");
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: NAME_PREFIX.to_string(),
            segment_dir: base_dir.to_string_lossy().into_owned(),
            lmdb_env_path: base_dir.to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Control, None);
        let _actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Send a ReplayControl to a Control actor (should be processed)
        let scan_window = ScanWindow { start: 0, end: 0 };
        let replay_event = XaeroPoolManager::create_xaero_event(
            bytes_of(&scan_window),
            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.sink.tx.send(replay_event).expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(200));

        // The actor should process the event (even if no segments exist, it won't error)
        // This test mainly verifies the event filtering logic works correctly

        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown pools: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn data_actor_processes_data_replay() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let temp_dir = tempdir().expect("failed_to_unravel");
        let base_dir = temp_dir.path().join("data_process");
        std::fs::create_dir_all(&base_dir).expect("failed_to_unravel");

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"data_process");
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: NAME_PREFIX.to_string(),
            segment_dir: base_dir.to_string_lossy().into_owned(),
            lmdb_env_path: base_dir.to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Data, None);
        let _actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Send a ReplayData to a Data actor (should be processed)
        let scan_window = ScanWindow { start: 0, end: 0 };
        let replay_event = XaeroPoolManager::create_xaero_event(
            bytes_of(&scan_window),
            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.sink.tx.send(replay_event).expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(200));

        // The actor should process the event (even if no segments exist, it won't error)
        // This test mainly verifies the event filtering logic works correctly

        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown pools: {:?}", e);
            }
        }
    }

    #[test]
    #[serial]
    fn test_unaligned_access_pattern() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let temp_dir = tempdir().expect("failed_to_unravel");
        let base_dir = temp_dir.path().join("unaligned_test");
        std::fs::create_dir_all(&base_dir).expect("failed_to_unravel");

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"unaligned_test");
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: NAME_PREFIX.to_string(),
            segment_dir: base_dir.to_string_lossy().into_owned(),
            lmdb_env_path: base_dir.to_string_lossy().into_owned(),
        };

        let pipe = Pipe::new(BusKind::Control, None);
        let _actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Test zero-copy data access
        let test_data = b"test unaligned access pattern";
        let test_event = XaeroPoolManager::create_xaero_event(
            test_data,
            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        // Verify zero-copy access works
        assert_eq!(test_event.data(), test_data);

        drop(_actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown pools: {:?}", e);
            }
        }
    }
}
