//! MMR indexing actor for xaeroflux-actors.
//!
//! This module defines:
//! - `MmrIndexingActor`: actor that listens to events, archives them, computes Merkle leaf hashes,
//!   updates an in-memory Merkle Mountain Range, and forwards leaf hashes to a segment store.
//! - Constructors for default and custom segment configurations.
//! - Accessors for the in-memory MMR, segment store, and LMDB environment.

use std::sync::{Arc, Mutex};

use super::segment_writer_actor::{SegmentConfig, SegmentWriterActor};
use crate::{
    aof::storage::lmdb::{push_xaero_event, LmdbEnv},
    indexing::storage::{
        actors::ExecutionState,
        format::archive_xaero_event,
        mmr::{XaeroMmr, XaeroMmrOps},
    },
    subject::SubjectHash,
};
use xaeroflux_core::hash::sha_256_slice;
use xaeroflux_core::{
    date_time::emit_secs, event::{EventType, EventType::SystemEvent, SystemEventKind, SystemEventKind::Shutdown, XaeroEvent}, pipe::{BusKind, Pipe},
    system_paths::*,
    XaeroPoolManager,
    IO_POOL,
    XAERO_DISPATCHER_POOL,
};

pub static NAME_PREFIX: &str = "mmr_actor";

/// Metadata structure for MMR state persistence
#[derive(Clone, Copy)]
#[repr(C)]
struct MmrMeta {
    mmr_size: usize,
    last_leaf_hash: Option<[u8; 32]>,
    event_count: u64,
    ts_updated: u64,
}

unsafe impl bytemuck::Pod for MmrMeta {}
unsafe impl bytemuck::Zeroable for MmrMeta {}

/// Internal state for MMR that persists across events
struct MmrState {
    mmr_size: usize,
    last_leaf_hash: Option<[u8; 32]>,
    event_count: u64,
    initialized: bool,
    execution_state: ExecutionState,
}

impl MmrState {
    fn new() -> Self {
        Self {
            mmr_size: 0,
            last_leaf_hash: None,
            event_count: 0,
            initialized: false,
            execution_state: ExecutionState::Waiting,
        }
    }

    fn initialize_from_metadata(&mut self, meta_db: &Arc<Mutex<LmdbEnv>>) {
        if self.initialized {
            return;
        }
        // Try to recover state from LMDB
        // This would involve reading the last known MMR state
        // For now, we'll start fresh but keep the structure for future enhancement
        self.initialized = true;
    }

    fn update_metadata(&mut self, meta_db: &Arc<Mutex<LmdbEnv>>) -> Result<(), Box<dyn std::error::Error>> {
        let mmr_meta = MmrMeta {
            mmr_size: self.mmr_size,
            last_leaf_hash: self.last_leaf_hash,
            event_count: self.event_count,
            ts_updated: emit_secs(),
        };

        let data_b_mmr_meta = bytemuck::bytes_of(&mmr_meta);
        let meta_event = XaeroPoolManager::create_xaero_event(
            data_b_mmr_meta,
            EventType::MetaEvent(2).to_u8(), // Different meta event type
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed for MMR metadata: {:?}", pool_error);
            panic!("Cannot create MMR metadata event - ring buffer pool exhausted");
        });

        push_xaero_event(meta_db, &meta_event)?;
        Ok(())
    }
}

/// Actor responsible for indexing events into a Merkle Mountain Range (MMR).
///
/// This actor:
/// 1. Listens to incoming events on its pipe
/// 2. Archives each event and persists it to LMDB
/// 3. Computes SHA-256 leaf hash from archived bytes
/// 4. Appends the leaf to the in-memory MMR
/// 5. Sends the leaf hash to both the output pipe and segment writer
pub struct MmrIndexingActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    pub mmr: Arc<Mutex<XaeroMmr>>,
    pub meta_db: Arc<Mutex<LmdbEnv>>,
    pub segment_writer: Arc<SegmentWriterActor>,
    mmr_state: Arc<Mutex<MmrState>>,
    _xaero_event_loop_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for MmrIndexingActor {
    fn drop(&mut self) {
        // Send shutdown signal using XaeroPoolManager
        let shutdown_event = XaeroPoolManager::create_xaero_event(
            &[],  // Empty data
            SystemEvent(Shutdown).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed for shutdown: {:?}", pool_error);
            panic!("Cannot create shutdown event - ring buffer pool exhausted");
        });

        let res = self.pipe.sink.tx.send(shutdown_event);
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
    }
}

impl MmrIndexingActor {
    /// Create a new `MmrIndexingActor` with optional segment configuration.
    pub fn new(name: SubjectHash, pipe: Arc<Pipe>, segment_config_opt: Option<SegmentConfig>) -> Self {
        XaeroPoolManager::init();

        let encoded_hash = hex::encode(name.0);

        // Setup segment configuration
        let segment_config = segment_config_opt.unwrap_or(SegmentConfig {
            prefix: "mmr".to_string(),
            ..Default::default()
        });

        // Create a SEPARATE pipe for the segment writer - it needs its own channel
        let segment_writer_pipe = Pipe::new(BusKind::Data, None);

        // Segment writer uses its own dedicated pipe
        let segment_writer = Arc::new(SegmentWriterActor::new_with_config(
            name,
            segment_writer_pipe.clone(),
            segment_config.clone(),
        ));

        // Setup LMDB environment for raw event storage
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(
                emit_data_path_with_subject_hash(&segment_config.lmdb_env_path, name.0, NAME_PREFIX).as_str(),
                BusKind::Data,
            )
                .expect("failed to create LmdbEnv"),
        ));

        // Setup in-memory MMR and persistent state
        let mmr = Arc::new(Mutex::new(XaeroMmr::new()));
        let mmr_state = Arc::new(Mutex::new(MmrState::new()));

        // Setup event processing - direct XaeroEvent processing
        let xh = Self::setup_event_processing(
            encoded_hash,
            pipe.clone(),        // Arc<Pipe>
            segment_writer_pipe, // Arc<Pipe>
            meta_db.clone(),     // Arc<Mutex<LmdbEnv>>
            mmr.clone(),         // Arc<Mutex<XaeroMmr>>
            mmr_state.clone(),   // Arc<Mutex<MmrState>>
        );

        Self {
            name,
            pipe,
            mmr,
            meta_db,
            segment_writer,
            mmr_state,
            _xaero_event_loop_handle: xh,
        }
    }

    /// Create a new `MmrIndexingActor` with a custom `SegmentConfig`.
    pub fn new_with_config(name: SubjectHash, pipe: Arc<Pipe>, config: SegmentConfig) -> Self {
        Self::new(name, pipe, Some(config))
    }

    /// Setup the event processing pipeline - direct XaeroEvent processing
    fn setup_event_processing(
        encoded_hash: String,
        pipe: Arc<Pipe>,
        segment_writer_pipe: Arc<Pipe>,
        meta_db: Arc<Mutex<LmdbEnv>>,
        mmr: Arc<Mutex<XaeroMmr>>,
        mmr_state: Arc<Mutex<MmrState>>,
    ) -> Option<std::thread::JoinHandle<()>> {
        let pipe_clone = pipe.clone();
        let meta_db_clone = meta_db.clone();
        let mmr_clone = mmr.clone();
        let mmr_state_clone = mmr_state.clone();

        // Spawn a dedicated thread for MMR event processing - no EventListener indirection
        let dispatch_thread_name = format!("xaeroflux-actors-mmr-{}", encoded_hash);
        let xaero_handle = std::thread::Builder::new()
            .name(dispatch_thread_name)
            .spawn(move || {
                tracing::info!("MmrIndexingActor processing thread started for subject: {}", encoded_hash);

                while let Ok(xaero_event) = pipe_clone.sink.rx.recv() {
                    // Check for shutdown signal
                    if xaero_event.event_type() == SystemEvent(Shutdown).to_u8() {
                        tracing::info!("MmrIndexingActor received shutdown signal");
                        break;
                    }

                    // Process the XaeroEvent directly
                    match Self::handle_xaero_event(
                        &xaero_event,
                        &segment_writer_pipe,
                        &pipe,
                        &meta_db_clone,
                        &mmr_clone,
                        &mmr_state_clone,
                    ) {
                        Ok(_) => {
                            tracing::debug!("Successfully processed XaeroEvent in MMR pipeline");
                        }
                        Err(e) => {
                            tracing::error!("Failed to handle MMR event: {}", e);
                        }
                    }
                }

                tracing::info!("MmrIndexingActor processing thread terminated");
            })
            .expect("failed to spawn MMR dispatch thread");

        Some(xaero_handle)
    }

    /// Handle a single XaeroEvent through the MMR pipeline
    fn handle_xaero_event(
        xaero_event: &Arc<XaeroEvent>,
        segment_writer_pipe: &Arc<Pipe>,
        output_pipe: &Arc<Pipe>,
        meta_db: &Arc<Mutex<LmdbEnv>>,
        mmr: &Arc<Mutex<XaeroMmr>>,
        mmr_state: &Arc<Mutex<MmrState>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Step 1: Archive the XaeroEvent using new format
        let archived_data = archive_xaero_event(xaero_event);

        // Step 2: Persist XaeroEvent to LMDB
        push_xaero_event(meta_db, xaero_event)?;

        // Step 3: Compute leaf hash from archived data
        let leaf_hash = sha_256_slice(&archived_data);

        // Step 4: Update persistent state and MMR
        let new_mmr_size = {
            let mut state_guard = mmr_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state_guard.initialize_from_metadata(meta_db);

            let mut mmr_guard = mmr.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            mmr_guard.append(leaf_hash);

            // Update persistent state
            state_guard.mmr_size = mmr_guard.leaf_count;
            state_guard.last_leaf_hash = Some(leaf_hash);
            state_guard.event_count += 1;

            // Flush state to LMDB for crash recovery
            state_guard.update_metadata(meta_db)?;

            state_guard.mmr_size
        };

        // Step 5: Send MmrAppended event using XaeroPoolManager
        let mmr_event = XaeroPoolManager::create_xaero_event(
            &leaf_hash,
            EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed for MmrAppended: {:?}", pool_error);
            panic!("Cannot create MmrAppended event - ring buffer pool exhausted");
        });

        // Send to both output pipe and segment writer pipe
        output_pipe.source.tx.send(mmr_event.clone())?;
        segment_writer_pipe.sink.tx.send(mmr_event)?;

        tracing::debug!(
            "Processed MMR event with leaf hash: {:?}, MMR size: {}",
            leaf_hash,
            new_mmr_size
        );
        Ok(())
    }

    /// Get a reference to the in-memory MMR
    pub fn mmr(&self) -> &Arc<Mutex<XaeroMmr>> {
        &self.mmr
    }

    /// Get a reference to the segment writer
    pub fn segment_writer(&self) -> &Arc<SegmentWriterActor> {
        &self.segment_writer
    }

    /// Get a reference to the metadata database
    pub fn meta_db(&self) -> &Arc<Mutex<LmdbEnv>> {
        &self.meta_db
    }
}

#[cfg(test)]
mod actor_tests {
    use std::{sync::Arc, thread::sleep, time::Duration};

    use crossbeam::channel::Receiver;
    use iroh_blobs::store::bao_tree::blake3;
    use serial_test::serial;
    use tempfile;
    use xaeroflux_core::{
        date_time::emit_secs,
        event::{EventType, SystemEventKind, XaeroEvent}, init_xaero_pool, initialize,
        shutdown_all_pools,
        XaeroPoolManager,
    };

    use super::*;
    use crate::indexing::storage::{actors::segment_writer_actor::SegmentConfig, format::archive_xaero_event};

    // Helper to create and send a XaeroEvent via pipe using XaeroPoolManager
    fn send_app_event(pipe: &Arc<Pipe>, data: Vec<u8>) {
        let xaero_event = XaeroPoolManager::create_xaero_event(
            &data,
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.sink.tx.send(xaero_event).expect("failed to send event");
    }

    #[test]
    #[serial]
    fn actor_appends_leaf_to_pipe() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let pipe = Pipe::new(BusKind::Data, None);
        let rx_out: Receiver<Arc<XaeroEvent>> = pipe.source.rx.clone();

        let mut hasher = blake3::Hasher::new();
        hasher.update("mmr-actor".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let tmp = tempfile::tempdir().expect("failed to create tempdir");

        let cfg = SegmentConfig {
            prefix: "mmr-test".to_string(),
            page_size: 128,
            pages_per_segment: 1,
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let _actor = MmrIndexingActor::new_with_config(subject_hash, pipe.clone(), cfg);

        // Send one application event into actor
        let payload = b"foo".to_vec();
        send_app_event(&pipe, payload.clone());

        // Allow processing time
        sleep(Duration::from_millis(200));

        // Expect a SystemEvent::MmrAppended on the output pipe
        let mut received_mmr_appended = false;
        let mut received_leaf_hash = None;
        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match rx_out.try_recv() {
                Ok(got) => {
                    tracing::debug!("Received event type: {:?}", got.event_type());
                    if got.event_type() == EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8() {
                        received_leaf_hash = Some(got.data().to_vec());
                        received_mmr_appended = true;
                        break;
                    }
                }
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    panic!("Channel error: {:?}", e);
                }
            }
        }

        assert!(received_mmr_appended, "Expected to receive MmrAppended event");

        // Verify we got a 32-byte hash
        if let Some(actual_leaf_hash) = received_leaf_hash {
            assert_eq!(actual_leaf_hash.len(), 32, "Leaf hash should be 32 bytes");
            tracing::debug!("Received leaf hash: {:?}", actual_leaf_hash);
        }

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
    fn actor_persists_leaf_to_segment_writer() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let pipe = Pipe::new(BusKind::Data, None);

        let mut hasher = blake3::Hasher::new();
        hasher.update("mmr-actor".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let cfg = SegmentConfig {
            page_size: 128,
            pages_per_segment: 1,
            prefix: "mmr-test".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let _actor = MmrIndexingActor::new_with_config(subject_hash, pipe.clone(), cfg.clone());

        // Send one application event into actor
        let payload = b"bar".to_vec();
        send_app_event(&pipe, payload.clone());

        // Allow processing time - the segment writer needs time to process the MMR leaf hash
        sleep(Duration::from_millis(300));

        // Verify segment files were created by the segment writer
        let segment_files: Vec<_> = std::fs::read_dir(&cfg.segment_dir)
            .expect("failed to read segment dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("seg"))
            .collect();

        assert!(
            !segment_files.is_empty(),
            "Expected segment files to be created by segment writer consuming MMR leaf hashes"
        );

        // Verify the content contains data
        if let Some(file_entry) = segment_files.first() {
            let file_content = std::fs::read(file_entry.path()).expect("failed to read segment file");
            assert!(!file_content.is_empty(), "Segment file should contain data");
        }

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
    fn actor_updates_mmr_state() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let pipe = Pipe::new(BusKind::Data, None);

        let mut hasher = blake3::Hasher::new();
        hasher.update("mmr-actor".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let cfg = SegmentConfig {
            page_size: 128,
            pages_per_segment: 1,
            prefix: "mmr-test".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let actor = MmrIndexingActor::new_with_config(subject_hash, pipe.clone(), cfg);

        // Check initial MMR state
        {
            let mmr_guard = actor.mmr().lock().expect("failed_to_unravel");
            assert_eq!(mmr_guard.leaf_count, 0, "MMR should start empty");
        }

        // Send multiple events
        for i in 0..3 {
            let payload = format!("test-payload-{}", i).into_bytes();
            send_app_event(&pipe, payload);
        }

        // Allow processing time
        sleep(Duration::from_millis(300));

        // Check final MMR state
        {
            let mmr_guard = actor.mmr().lock().expect("failed_to_unravel");
            assert_eq!(
                mmr_guard.leaf_count, 3,
                "MMR should contain 3 elements after processing 3 events"
            );
        }

        drop(actor);
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
    fn test_zero_copy_mmr_processing() {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let pipe = Pipe::new(BusKind::Data, None);

        let mut hasher = blake3::Hasher::new();
        hasher.update("mmr-zero-copy".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let cfg = SegmentConfig {
            page_size: 256,
            pages_per_segment: 1,
            prefix: "mmr-zero-copy".to_string(),
            segment_dir: tmp.path().to_string_lossy().into_owned(),
            lmdb_env_path: tmp.path().to_string_lossy().into_owned(),
        };

        let _actor = MmrIndexingActor::new_with_config(subject_hash, pipe.clone(), cfg);

        // Test zero-copy data access
        let test_data = b"zero copy mmr test data";
        let test_event = XaeroPoolManager::create_xaero_event(
            test_data,
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        // Verify zero-copy access works
        assert_eq!(test_event.data(), test_data);

        pipe.sink.tx.send(test_event).expect("failed to send test event");
        sleep(Duration::from_millis(150));

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