//! Append-only event log (AOF) actor using LMDB for persistent storage.
//!
//! This module provides:
//! - `AOFActor`: listens for events and writes them durably into LMDB.
//! - `LmdbEnv`: wrapper around the LMDB environment and databases.
//! - Functions to push events, scan event ranges, and manage metadata.

use std::sync::{Arc, Mutex};

use xaeroflux_core::{
    CONF, XAERO_DISPATCHER_POOL, XaeroPoolManager,
    event::XaeroEvent,
    init_xaero_pool, initialize,
    system_paths::{emit_control_path_with_subject_hash, emit_data_path_with_subject_hash},
};

use super::storage::lmdb::{LmdbEnv, push_xaero_event};
use crate::{
    subject::SubjectHash,
};
use xaeroflux_core::pipe::*;
use xaeroflux_core::pipe::BusKind::{Control, Data};

pub static NAME_PREFIX: &str = "aof";

/// An append-only file (AOF) actor that persists events into LMDB.
///
/// The `AOFActor` encapsulates an LMDB environment and processes XaeroEvents
/// directly without the EventListener indirection. It writes incoming events
/// to disk for durable storage and later retrieval using the ring buffer architecture.
pub struct AOFActor {
    pub pipe: Arc<Pipe>,
    pub env: Arc<Mutex<LmdbEnv>>,
}

impl AOFActor {
    pub fn new(subject_hash: SubjectHash, pipe: Arc<Pipe>) -> Self {
        initialize();
        init_xaero_pool();
        XaeroPoolManager::init();

        let c = CONF.get().expect("failed to unravel");
        let fp = &c.aof.file_path;
        let fpc = fp.clone();

        let control_path = emit_control_path_with_subject_hash(
            fp.to_str().expect("path_invalid_for_aof"),
            subject_hash.0,
            NAME_PREFIX,
        );
        let cpc = control_path.clone();

        let data_path = emit_data_path_with_subject_hash(
            fpc.to_str().expect("path_invalid_for_aof"),
            subject_hash.0,
            NAME_PREFIX
        );
        let data_path_clone = data_path.clone();

        let (path, env) = if pipe.sink.kind == Control {
            (
                control_path,
                Arc::new(Mutex::new(
                    LmdbEnv::new(cpc.as_str(), BusKind::Control).expect("failed to unravel"),
                )),
            )
        } else if pipe.sink.kind == Data {
            (
                data_path,
                Arc::new(Mutex::new(
                    LmdbEnv::new(&data_path_clone, BusKind::Data).expect("failed to unravel"),
                )),
            )
        } else {
            panic!("unravel unknown control type")
        };

        // Clone environment for the processing thread
        let env_clone = Arc::clone(&env);
        let pipe_clone = pipe.clone();

        // Direct XaeroEvent processing - no EventListener indirection
        threadpool::Builder::new().build().execute(move || {
            tracing::info!("AOFActor processing thread started for path: {}", path);

            while let Ok(xaero_event) = pipe_clone.source.rx.recv() {
                match Self::handle_xaero_event(&env_clone, &xaero_event) {
                    Ok(_) => {
                        tracing::debug!(
                            "Successfully persisted XaeroEvent: type={}, size={} bytes",
                            xaero_event.event_type(),
                            xaero_event.data().len()
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to persist XaeroEvent: type={}, error={:?}",
                            xaero_event.event_type(),
                            e
                        );
                    }
                }
            }

            tracing::info!("AOFActor processing thread terminated for path: {}", path);
        });

        Self { pipe, env }
    }

    /// Handle a single XaeroEvent by writing it to LMDB storage
    fn handle_xaero_event(
        env: &Arc<Mutex<LmdbEnv>>,
        xaero_event: &Arc<XaeroEvent>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::trace!(
            "Processing XaeroEvent: type={}, timestamp={:?}, data_len={}",
            xaero_event.event_type(),
            xaero_event.latest_ts,
            xaero_event.data().len()
        );

        // Use the new push_xaero_event function that works with Arc<XaeroEvent>
        push_xaero_event(env, xaero_event)
            .map_err(|e| panic!("Failed to push XaeroEvent: {}", e))?;

        tracing::trace!("XaeroEvent successfully persisted to LMDB");
        Ok(())
    }
}

impl Drop for AOFActor {
    fn drop(&mut self) {
        tracing::info!("AOFActor dropping - closing LMDB environment");
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};
    use tempfile::tempdir;
    use xaeroflux_core::{
        config::Config,
        event::{EventType, XaeroEvent},
        date_time::emit_secs,
    };
    use xaeroflux_core::event::SystemEventKind;
    use super::*;
    use crate::subject::SubjectHash;

    /// Before each test, override CONF.aof.file_path to point at a temporary directory,
    /// so that `emit_control_path_with_subject_hash(...)` always has a valid base path.
    fn init_conf_with_tempdir() -> tempfile::TempDir {
        // 1) create a tempdir to use as the "super-prefix"
        let tmp = tempdir().expect("failed to create tempdir");

        // 2) (re)initialize CONF
        initialize();

        // 3) override the aof.file_path field inside CONF
        let c = CONF.get().expect("CONF must be set in initialize()");
        // We need to mutate a field inside the global `Config`. Since `get()` returns &Config,
        // we use unsafe to obtain a mutable pointer. This is safe here because tests run
        // single-threaded.
        let new_fp = tmp.path().to_path_buf();
        let ptr = c as *const _ as *mut Config;
        unsafe {
            (*ptr).aof.file_path = new_fp;
        }

        tmp
    }

    #[test]
    fn test_aof_actor_initialization() {
        initialize();
        XaeroPoolManager::init();

        // Arrange: set CONF.aof.file_path to a usable tempdir
        let _tmp = init_conf_with_tempdir();

        // Act: construct two pipes (control and a clone) and pass into AOFActor::new
        let subject_hash = SubjectHash([0u8; 32]);
        let control_pipe = Pipe::new(BusKind::Control, None);
        let clone_pipe = control_pipe.clone();

        // This should no longer panic, since `c.aof.file_path` now points at an existing directory
        let _actor1 = AOFActor::new(subject_hash, control_pipe);
        let _actor2: AOFActor = AOFActor::new(subject_hash, clone_pipe);
    }

    #[test]
    fn test_aof_actor_push_xaero_event() {
        initialize();
        XaeroPoolManager::init();

        // Arrange: override CONF.aof.file_path
        let _tmp = init_conf_with_tempdir();

        let subject_hash = SubjectHash([0u8; 32]);
        let control_pipe = Pipe::new(BusKind::Control, None);

        // Construct the actor; internally, it will call `emit_control_path_with_subject_hash`,
        // which now uses the tempdir from CONF.aof.file_path
        let actor = AOFActor::new(subject_hash, control_pipe);

        // Act: create and send a sample XaeroEvent using the ring buffer allocator
        let sample_data = b"hello world from ring buffer";
        let xaero_event = XaeroPoolManager::create_xaero_event(
            sample_data,
            EventType::ApplicationEvent(1).to_u8(),
            None,  // author_id
            None,  // merkle_proof
            None,  // vector_clock
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        actor
            .pipe
            .sink
            .tx
            .send(xaero_event)
            .expect("Failed to send XaeroEvent to AOFActor");

        // Give the background thread a moment to process. It will attempt to write into
        // the LMDB files under `<tempdir>/<subject_hash>/aof/control/...`
        thread::sleep(Duration::from_millis(200));

        // We don't assert on filesystem contents, just that no panic occurred.
    }

    #[test]
    fn test_aof_actor_multiple_events() {
        initialize();
        XaeroPoolManager::init();

        let _tmp = init_conf_with_tempdir();
        let subject_hash = SubjectHash([0u8; 32]);
        let data_pipe = Pipe::new(BusKind::Data, None);
        let actor = AOFActor::new(subject_hash, data_pipe);

        // Send multiple events to test batch processing
        for i in 0..5 {
            let data = format!("test event {}", i);
            let xaero_event = XaeroPoolManager::create_xaero_event(
                data.as_bytes(),
                EventType::ApplicationEvent(i as u8).to_u8(),
                None,
                None,
                None,
                emit_secs(),
            ).unwrap_or_else(|pool_error| {
                tracing::error!("Pool allocation failed for event {}: {:?}", i, pool_error);
                panic!("Cannot create test event {} - ring buffer pool exhausted", i);
            });

            actor
                .pipe
                .sink
                .tx
                .send(xaero_event)
                .expect(&format!("Failed to send XaeroEvent {}", i));
        }

        // Allow time for all events to be processed
        thread::sleep(Duration::from_millis(300));
    }

    #[test]
    fn test_aof_actor_zero_copy_access() {
        initialize();
        XaeroPoolManager::init();

        let _tmp = init_conf_with_tempdir();
        let subject_hash = SubjectHash([0u8; 32]);
        let control_pipe = Pipe::new(BusKind::Control, None);
        let actor = AOFActor::new(subject_hash, control_pipe);

        // Create event with known data for verification
        let test_data = b"zero copy test data";
        let xaero_event = XaeroPoolManager::create_xaero_event(
            test_data,
            EventType::SystemEvent(SystemEventKind::PayloadWritten).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        // Verify zero-copy access works
        assert_eq!(xaero_event.data(), test_data);
        assert_eq!(xaero_event.event_type(), EventType::SystemEvent(SystemEventKind::PayloadWritten).to_u8());

        actor
            .pipe
            .sink
            .tx
            .send(xaero_event)
            .expect("Failed to send XaeroEvent to AOFActor");

        thread::sleep(Duration::from_millis(150));
    }
}