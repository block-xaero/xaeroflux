//! Append-only event log (AOF) actor using LMDB for persistent storage.
//!
//! This module provides:
//! - `AOFActor`: listens for events and writes them durably into LMDB.
//! - `LmdbEnv`: wrapper around the LMDB environment and databases.
//! - Functions to push events, scan event ranges, and manage metadata.

use std::sync::{Arc, Mutex};

use xaeroflux_core::{
    XAERO_DISPATCHER_POOL, event::Event, listeners::EventListener,
    system_paths::emit_control_path_with_subject_hash,
};

use super::storage::lmdb::{LmdbEnv, push_event};
use crate::{
    BusKind,
    BusKind::{Control, Data},
    Pipe,
    core::{CONF, initialize},
    subject::SubjectHash,
};

pub static NAME_PREFIX: &str = "aof";
/// An append-only file (AOF) actor that persists events into LMDB.
///
/// The `AOFActor` encapsulates an LMDB environment and an event listener.
/// It writes incoming events to disk for durable storage and later retrieval.
pub struct AOFActor {
    pub pipe: Arc<Pipe>,
    pub env: Arc<Mutex<LmdbEnv>>,
}

impl AOFActor {
    pub fn new(subject_hash: SubjectHash, pipe: Arc<Pipe>) -> Self {
        initialize();
        let c = CONF.get().expect("failed to unravel");
        let fp = &c.aof.file_path;
        let fpc = fp.clone();
        let control_path = emit_control_path_with_subject_hash(
            fp.to_str().expect("path_invalid_for_aof"),
            subject_hash.0,
            NAME_PREFIX,
        );
        let cpc = control_path.clone();
        let data_path = emit_control_path_with_subject_hash(
            fpc.to_str().expect("path_invalid_for_aof"),
            subject_hash.0,
            NAME_PREFIX,
        );
        let data_path_clone = data_path.clone();
        let (path, env) = if pipe.sink.kind == Control {
            (
                control_path,
                Arc::new(Mutex::new(
                    LmdbEnv::new(&cpc.as_str(), BusKind::Control).expect("failed to unravel"),
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
        let env_c = Arc::clone(&env);
        let h = Arc::new(move |e: Event<Vec<u8>>| {
            tracing::info!("Pushing event to LMDB: {:?}", e);
            push_event(&env_c, &e).expect("failed to unravel");
        });
        let listener = EventListener::new(
            path.as_str(),
            h,
            Some(CONF.get().expect("failed to load config").aof.buffer_size),
            Some(c.aof.threads.num_worker_threads),
        );
        // L3 : We dispatch xaero event to EventListener
        // TODO: we need to do merkle proof handshake here.
        let pipe_clone = pipe.clone();
        XAERO_DISPATCHER_POOL
            .get()
            .expect("xaero pool not initialized")
            .execute(move || {
                while let Ok(event) = pipe_clone.source.rx.recv() {
                    let res = listener.inbox.send(event.evt);
                    match res {
                        Ok(_) => {
                            tracing::debug!("sent Xaero event successfully")
                        }
                        Err(e) => tracing::error!("failed to send Xaero event error: {}", e),
                    }
                }
            });

        Self { pipe, env }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use tempfile::tempdir;
    use xaeroflux_core::{
        config::Config,
        event::{Event, EventType},
    };

    use super::*;
    use crate::{XaeroEvent, subject::SubjectHash};

    /// Before each test, override CONF.aof.file_path to point at a temporary directory,
    /// so that `emit_control_path_with_subject_hash(...)` always has a valid base path.
    fn init_conf_with_tempdir() -> tempfile::TempDir {
        // 1) create a tempdir to use as the “super-prefix”
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
    fn test_aof_actor_push_event() {
        initialize();
        // Arrange: override CONF.aof.file_path
        let _tmp = init_conf_with_tempdir();

        let subject_hash = SubjectHash([0u8; 32]);
        let control_pipe = Pipe::new(BusKind::Control, None);
        // Construct the actor; internally, it will call `emit_control_path_with_subject_hash`,
        // which now uses the tempdir from CONF.aof.file_path
        let actor = AOFActor::new(subject_hash, control_pipe);

        // Act: send a sample application event to the listener’s inbox
        let sample_data = b"hello world".to_vec();
        actor
            .pipe
            .sink
            .tx
            .send(XaeroEvent {
                evt: Event::new(sample_data, EventType::ApplicationEvent(1).to_u8()),
                merkle_proof: None,
            })
            .expect("Failed to send to AOFActor listener inbox");

        // Give the background thread a moment to process. It will attempt to write into
        // the LMDB files under `<tempdir>/<subject_hash>/aof/control/...`
        thread::sleep(Duration::from_millis(100));
        // We don't assert on filesystem contents, just that no panic occurred.
    }
}
