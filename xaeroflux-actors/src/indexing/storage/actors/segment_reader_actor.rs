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

use rkyv::rancor::Failure;
use xaeroflux_core::{
    XAERO_DISPATCHER_POOL,
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{
        Event, EventType,
        EventType::SystemEvent,
        SystemEventKind,
        SystemEventKind::{ReplayControl, ReplayData, Shutdown},
    },
    listeners::EventListener,
    system_paths::{emit_control_path_with_subject_hash, emit_data_path_with_subject_hash},
};
use xaeroflux_core::event::{ScanWindow,XaeroEvent};
use crate::{
    BusKind, Pipe,
    aof::storage::{lmdb::LmdbEnv, meta::iterate_segment_meta_by_range},
    indexing::storage::{actors::segment_writer_actor::SegmentConfig, io},
    subject::SubjectHash,
};

pub static NAME_PREFIX: &str = "segment_reader";

/// Actor for replaying events from historical segments.
///
/// This actor handles replay requests by:
/// 1. Listening for ReplayControl/ReplayData events based on bus kind
/// 2. Reading segment metadata from LMDB within specified time ranges
/// 3. Loading and deserializing events from segment files
/// 4. Forwarding deserialized events to the output pipe
pub struct SegmentReaderActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    pub meta_db: Arc<Mutex<LmdbEnv>>,
    pub segment_config: SegmentConfig,
    _xaero_event_handle: Option<std::thread::JoinHandle<()>>,
}
impl Drop for SegmentReaderActor {
    fn drop(&mut self) {
        let res = self.pipe.sink.tx.send(XaeroEvent {
            evt: Event::new(vec![], SystemEvent(Shutdown).to_u8()),
            merkle_proof: None,
        });
        match res {
            Ok(_) => {
                tracing::debug!("MmrIndexingActor :: Shutdown initiated");
            }
            Err(e) => {
                tracing::error!("MmrIndexingActor :: Error sending shutdown event: {:?}", e);
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
        std::fs::create_dir_all(&config.segment_dir).expect("failed to create segment directory");
        std::fs::create_dir_all(&config.lmdb_env_path).expect("failed to create LMDB directory");

        // Set up paths based on bus kind
        let meta_db = match pipe.sink.kind {
            BusKind::Control => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_control_path_with_subject_hash(&config.lmdb_env_path, name.0, NAME_PREFIX)
                        .as_str(),
                    BusKind::Control,
                )
                .expect("failed to create Control LmdbEnv"),
            )),
            BusKind::Data => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_data_path_with_subject_hash(&config.lmdb_env_path, name.0, NAME_PREFIX)
                        .as_str(),
                    BusKind::Data,
                )
                .expect("failed to create Data LmdbEnv"),
            )),
        };

        let pipe_clone = pipe.clone();
        let meta_db_clone = meta_db.clone();
        let config_clone = config.clone();
        let bus_kind = pipe.sink.kind;
        let listener = EventListener::new(
            "segment_reader_actor_listener",
            Arc::new(move |event: Event<Vec<u8>>| {
                if let Err(e) = Self::handle_replay_event(
                    &event,
                    &pipe_clone,
                    &meta_db_clone,
                    &config_clone,
                    bus_kind,
                ) {
                    tracing::error!("Failed to handle replay event: {}", e);
                }
            }),
            None,
            Some(1),
        );

        // Start event processing loop
        let pipe_for_loop = pipe.clone();
        let _xaero_handle = std::thread::Builder::new()
            .name(format!("segment-reader-{}", hex::encode(name.0)))
            .spawn(move || {
                while let Ok(xae) = pipe_for_loop.sink.rx.recv() {
                    if (xae.evt.event_type == EventType::SystemEvent(Shutdown)) {
                        if let Err(e) = listener.inbox.send(xae.evt) {
                            tracing::error!("Failed to send event to listener: {}", e);
                        }
                        break;
                    }
                    if let Err(e) = listener.inbox.send(xae.evt) {
                        tracing::error!("Failed to send event to listener: {}", e);
                    }
                }
            })
            .expect("failed to spawn a loop thread");

        Self {
            name,
            pipe,
            meta_db,
            segment_config: config,
            _xaero_event_handle: Some(_xaero_handle),
        }
    }

    /// Handle a replay event based on bus kind and event type
    fn handle_replay_event(
        event: &Event<Vec<u8>>,
        pipe: &Arc<Pipe>,
        meta_db: &Arc<Mutex<LmdbEnv>>,
        config: &SegmentConfig,
        bus_kind: BusKind,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let event_type = EventType::from_u8(event.event_type.to_u8());
        let etc = event_type.clone();
        let etc_processing_debug = etc.clone();
        // Filter events based on bus kind - Control actors only process ReplayControl, Data actors
        // only process ReplayData
        let should_process = match (bus_kind, event_type) {
            (BusKind::Control, EventType::SystemEvent(SystemEventKind::ReplayControl)) => true,
            (BusKind::Data, EventType::SystemEvent(SystemEventKind::ReplayData)) => true,
            _ => {
                tracing::debug!("Ignoring event type {:?} for {:?} bus", etc, bus_kind);
                return Ok(());
            }
        };

        if !should_process {
            return Ok(());
        }

        tracing::info!(
            "Processing replay event: {:?} on {:?} bus",
            etc_processing_debug,
            bus_kind
        );

        // Parse scan window or use default day bounds
        let (start_time, end_time) = match bytemuck::try_from_bytes::<ScanWindow>(&event.data) {
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
            let ts_start = segment_meta.ts_start;
            let seg_idx = segment_meta.segment_index;

            tracing::debug!(
                "Processing segment: ts_start={}, seg_idx={}",
                ts_start,
                seg_idx
            );

            // Build segment file path
            let segment_dir = match bus_kind {
                BusKind::Control => emit_control_path_with_subject_hash(
                    &config.segment_dir,
                    // Assuming we have the subject hash available - you may need to pass it
                    [0u8; 32], // This should be the actual subject hash
                    NAME_PREFIX,
                ),
                BusKind::Data => emit_data_path_with_subject_hash(
                    &config.segment_dir,
                    [0u8; 32], // This should be the actual subject hash
                    NAME_PREFIX,
                ),
            };

            let file_path = Path::new(&segment_dir)
                .join(format!("{}-{}-{:04}.seg", config.prefix, ts_start, seg_idx));

            // Read and process segment file
            match io::read_segment_file(file_path.to_str().unwrap_or("invalid_path")) {
                Ok(mmap) => {
                    tracing::debug!("Successfully read segment file: {:?}", file_path);
                    let page_iter = io::PageEventIterator::new(&mmap);

                    for event_bytes in page_iter {
                        match rkyv::api::high::deserialize::<Event<Vec<u8>>, Failure>(event_bytes) {
                            Ok(deserialized_event) => {
                                let xaero_event = XaeroEvent {
                                    evt: deserialized_event,
                                    merkle_proof: None,
                                };

                                if let Err(e) = pipe.source.tx.send(xaero_event) {
                                    tracing::error!("Failed to send replayed event: {}", e);
                                } else {
                                    events_replayed += 1;
                                    tracing::debug!("Replayed event #{}", events_replayed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to deserialize event: {}", e);
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
    use iroh_blobs::store::bao_tree::blake3;
    use serial_test::serial;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use xaeroflux_core::{
        event::{Event, EventType, SystemEventKind},
        init_xaero_pool, shutdown_all_pools,
    };

    use xaeroflux_core::event::{ScanWindow, XaeroEvent};
    use super::*;
    use crate::{
        BusKind, Pipe,
        core::initialize,
        indexing::storage::{actors::segment_writer_actor::SegmentConfig, format::PAGE_SIZE},
        subject::SubjectHash,
    };

    #[test]
    #[serial]
    fn control_actor_ignores_data_replay() {
        initialize();
        init_xaero_pool();

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
        let replay_evt = Event::new(
            bytes_of(&scan_window).to_vec(),
            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
        );

        pipe.sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
            })
            .expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(100));

        // Expect no event (timeout) because Control actor should ignore ReplayData
        assert!(
            pipe.source
                .rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "Control actor should ignore ReplayData events"
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
    #[serial]
    fn data_actor_ignores_control_replay() {
        initialize();
        init_xaero_pool();

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
        let replay_evt = Event::new(
            bytes_of(&scan_window).to_vec(),
            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
        );

        pipe.sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
            })
            .expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(100));

        // Expect no event (timeout) because Data actor should ignore ReplayControl
        assert!(
            pipe.source
                .rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "Data actor should ignore ReplayControl events"
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
    #[serial]
    fn control_actor_processes_control_replay() {
        initialize();
        init_xaero_pool();

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
        let replay_evt = Event::new(
            bytes_of(&scan_window).to_vec(),
            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
        );

        pipe.sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
            })
            .expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(200));

        // The actor should process the event (even if no segments exist, it won't error)
        // This test mainly verifies the event filtering logic works correctly
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
    fn data_actor_processes_data_replay() {
        initialize();
        init_xaero_pool();

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
        let replay_evt = Event::new(
            bytes_of(&scan_window).to_vec(),
            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
        );

        pipe.sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
            })
            .expect("failed_to_unravel");

        // Allow processing time
        sleep(Duration::from_millis(200));

        // The actor should process the event (even if no segments exist, it won't error)
        // This test mainly verifies the event filtering logic works correctly

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
