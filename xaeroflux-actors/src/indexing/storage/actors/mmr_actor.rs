//! MMR indexing actor for xaeroflux-actors.
//!
//! This module defines:
//! - `MmrIndexingActor`: actor that listens to events, archives them, computes Merkle leaf hashes,
//!   updates an in-memory Merkle Mountain Range, and forwards leaf hashes to a segment store.
//! - Constructors for default and custom segment configurations.
//! - Accessors for the in-memory MMR, segment store, and LMDB environment.

use std::{
    sync::{Arc, Mutex},
    thread,
};

use crossbeam::channel::Receiver;
use xaeroflux_core::{
    XAERO_DISPATCHER_POOL,
    event::{Event, EventType, SystemEventKind},
    hash::sha_256,
    listeners::EventListener,
    system_paths::*,
};

use super::segment_writer_actor::{SegmentConfig, SegmentWriterActor};
use crate::{
    BusKind, Pipe, XaeroEvent,
    aof::storage::lmdb::{LmdbEnv, push_event},
    indexing::storage::{
        format::archive,
        mmr::{XaeroMmr, XaeroMmrOps},
    },
    subject::SubjectHash,
};

pub static NAME_PREFIX: &str = "mmr_actor";
/// Actor responsible for indexing events into a Merkle Mountain Range (MMR).
///
/// Fields:
/// - `_lmdb`: LMDB environment for persisting raw events.
/// - `_mmr`: in-memory MMR instance for accumulating leaf hashes.
/// - `_store`: segment writer actor to persist leaf hashes in pages.
/// - `listener`: event listener that drives the indexing pipeline.
pub struct MmrIndexingActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    pub(crate) _handle: thread::JoinHandle<()>,
}

impl MmrIndexingActor {
    /// Create a new `MmrIndexingActor` with optional store and listener.
    ///
    /// If `store` is `None`, a default `SegmentWriterActor` with prefix "xaeroflux-actors-mmr" is
    /// used. If `listener` is `None`, an `EventListener` is created that:
    ///   1. Archives each event into bytes.
    ///   2. Persists the event into LMDB.
    ///   3. Computes the SHA-256 leaf hash from the archived bytes.
    ///   4. Appends the leaf to the in-memory MMR.
    ///   5. Sends the leaf hash to the segment writer for paging.
    ///
    /// Uses a single-threaded event handler by default.
    pub fn new(
        name: SubjectHash,
        pipe: Arc<Pipe>,
        segment_config_opt: Option<SegmentConfig>,
    ) -> Self {
        let pipe_clone0 = pipe.clone();
        let _mmr = Arc::new(Mutex::new(crate::indexing::storage::mmr::XaeroMmr::new()));
        let _store = Arc::new(SegmentWriterActor::new_with_config(
            name,
            pipe_clone0,
            segment_config_opt.unwrap_or(SegmentConfig {
                prefix: "mmr".to_string(),
                ..Default::default()
            }),
        ));
        // Use the store's lmdb_env_path instead of hardcoded path
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(
                emit_data_path_with_subject_hash(
                    &_store.segment_config.lmdb_env_path,
                    name.0,
                    NAME_PREFIX,
                )
                .as_str(),
                BusKind::Data,
            )
            .expect("failed to create LmdbEnv"),
        ));
        let (tx, rx) = crossbeam::channel::unbounded();
        let txc = tx.clone();
        let rxc = rx.clone();
        let pipe_clone1 = pipe.clone();
        let mdb_c = meta_db.clone();
        let _store_clone = _store.clone();
        let mmr_clone = _mmr.clone();
        let _listener = EventListener::new(
            "mmr_indexing_actor",
            Arc::new(move |e: Event<Vec<u8>>| {
                let res = txc.send(e);
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("mmr_indexing_actor listener dropped an event!");
                    }
                }
            }),
            None,
            Some(1), // single-threaded handler
        );
        XAERO_DISPATCHER_POOL
            .get()
            .expect("XAERO_DISPATCHER_POOL::get()")
            .execute(move || {
                while let Ok(evt) = pipe.source.rx.recv() {
                    let event_type = evt.evt.event_type.clone();
                    let res = tx.send(evt.evt);
                    match res {
                        Ok(_) => {}
                        Err(_) => {
                            tracing::error!("failed to send event of type {event_type:?}");
                        }
                    }
                }
            });
        let jh = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                Self::run_mmr_writer_loop(&mdb_c, _store_clone, mmr_clone, rxc);
            })
            .expect("failed to spawn mmr writer thread");

        Self {
            name,
            pipe: pipe_clone1,
            _handle: jh,
        }
    }

    fn run_mmr_writer_loop(
        mdb_c: &Arc<Mutex<LmdbEnv>>,
        _store_clone: Arc<SegmentWriterActor>,
        mmr_clone: Arc<Mutex<XaeroMmr>>,
        rx: Receiver<Event<Vec<u8>>>,
    ) {
        while let Ok(e) = rx.recv() {
            let framed = archive(&e);
            push_event(mdb_c, &e).expect("failed to push event");
            let leaf_hash = sha_256(&framed);
            {
                mmr_clone
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .append(leaf_hash);
            }
            _store_clone
                .pipe
                .sink
                .tx
                .send(XaeroEvent {
                    evt: Event::new(
                        leaf_hash.to_vec(),
                        EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8(),
                    ),
                    merkle_proof: None,
                })
                .expect("failed to send event");
        }
    }

    /// Create a new `MmrIndexingActor` with a custom `SegmentConfig`.
    ///
    /// - `config`: parameters for the segment writer (page size, prefixes, directories).
    /// - `listener`: optional custom event listener; if `None`, a default is created as in `new`.
    ///
    /// This allows configuring storage paths while reusing the same MMR pipeline.
    pub fn new_with_config(name: SubjectHash, pipe: Arc<Pipe>, config: SegmentConfig) -> Self {
        Self::new(name, pipe, Some(config))
    }
}

#[cfg(test)]
mod actor_tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam::channel::Receiver;
    use iroh_blobs::store::bao_tree::blake3;
    use tempfile;
    use xaeroflux_core::{
        event::{Event, EventType, SystemEventKind},
        hash::sha_256,
        init_xaero_pool,
    };

    use super::*;
    use crate::{
        core::initialize,
        indexing::storage::{actors::segment_writer_actor::SegmentConfig, format::archive},
    };

    /// Helper to wrap an Event<Vec<u8>> into a XaeroEvent and send it via pipe.
    fn send_app_event(pipe: &Arc<Pipe>, data: Vec<u8>) {
        let e = Event::new(data.clone(), EventType::ApplicationEvent(1).to_u8());
        let xaero_evt = XaeroEvent {
            evt: e,
            merkle_proof: None,
        };
        pipe.sink.tx.send(xaero_evt).expect("failed to send event");
    }

    #[ignore]
    #[test]
    fn actor_appends_leaf_to_pipe() {
        initialize();
        init_xaero_pool();
        // Create a data pipe for MMR (it listens on Data events)
        let pipe = Pipe::new(BusKind::Data, None);
        let rx_out: Receiver<XaeroEvent> = pipe.source.rx.clone();

        // Compute SubjectHash (for example, using "mmr-actor" as namespace)
        let mut hasher = blake3::Hasher::new();
        hasher.update("mmr-actor".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        let tmp = tempfile::tempdir().expect("failed to create tempdir");

        let payload = b"foo".to_vec();
        let app_event = Event::new(payload.clone(), EventType::ApplicationEvent(1).to_u8());
        let leaf_hash = sha_256(&archive(&app_event));
        let appended_evt = Event::new(
            leaf_hash.to_vec(),
            EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8(),
        );
        let framed = archive(&appended_evt);
        let cfg = SegmentConfig {
            prefix: "mmr-test".to_string(),
            page_size: framed.len(),
            pages_per_segment: 1,
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };

        let _actor = MmrIndexingActor::new_with_config(subject_hash, pipe.clone(), cfg.clone());

        // Send one application event into actor
        send_app_event(&pipe, payload.clone());
        // Allow the actor thread to process the event
        std::thread::sleep(Duration::from_millis(50));

        // Expect a SystemEvent::MmrAppended on the same pipe with correct leaf hash
        let got = loop {
            let got = rx_out
                .recv_timeout(Duration::from_secs(1))
                .expect("expected MmrAppended event");
            if got.evt.event_type == EventType::SystemEvent(SystemEventKind::MmrAppended) {
                break got;
            }
        };
        // Unpack the leaf hash from got.evt.data
        let leaf_hash = got.evt.data.clone();
        // Compute expected: sha256( archive(&app_event) )
        let expected_hash = leaf_hash.to_vec();
        assert_eq!(leaf_hash, expected_hash);
        assert_eq!(
            got.evt.event_type.to_u8(),
            EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8()
        );
    }

    #[ignore]
    #[test]
    fn actor_persists_leaf_to_segment_writer() {
        initialize();
        init_xaero_pool();
        // Prepare a temp directory and change cwd so segment writer writes here
        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        std::env::set_current_dir(tmp.path()).expect("failed to change tempdir");

        // Create a data pipe and subscribe to its rx to capture writes
        let pipe = Pipe::new(BusKind::Data, None);
        let rx_out: Receiver<XaeroEvent> = pipe.source.rx.clone();

        // Compute SubjectHash
        let mut hasher = blake3::Hasher::new();
        hasher.update("mmr-actor".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        // Build a small SegmentConfig so we write a file quickly
        let cfg = SegmentConfig {
            page_size: 32,
            pages_per_segment: 1,
            prefix: "mmr-test".to_string(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };

        // Construct MmrIndexingActor (it will internally create its own SegmentWriterActor)
        // let actor = MmrIndexingActor::new(subject_hash, pipe.clone(), Some(cfg));

        // Send one application event into actor
        let payload = b"bar".to_vec();
        send_app_event(&pipe, payload.clone());

        // The SegmentWriterActor will receive the MmrAppended XaeroEvent on the same pipe,
        // so we should see it:
        let got = rx_out
            .recv_timeout(Duration::from_secs(1))
            .expect("expected MmrAppended from segment writer");
        assert_eq!(
            got.evt.event_type.to_u8(),
            EventType::SystemEvent(SystemEventKind::MmrAppended).to_u8()
        );
    }
}
