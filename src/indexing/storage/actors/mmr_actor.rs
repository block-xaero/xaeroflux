use std::sync::{Arc, Mutex};

use super::segment_writer_actor::{SegmentConfig, SegmentWriterActor};
use crate::{
    core::{
        aof::{self, LmdbEnv, push_event},
        event::Event,
        listeners::EventListener,
    },
    indexing::{
        hash::sha_256,
        storage::{format::archive, mmr::XaeroMmrOps},
    },
};

pub struct MmrIndexingActor {
    pub(crate) _lmdb: Arc<Mutex<aof::LmdbEnv>>,
    pub(crate) _mmr: Arc<Mutex<crate::indexing::storage::mmr::XaeroMmr>>,
    pub(crate) _store: Arc<SegmentWriterActor>,
    pub listener: EventListener<Vec<u8>>,
}

impl MmrIndexingActor {
    pub fn new(
        store: Option<SegmentWriterActor>,
        listener: Option<EventListener<Vec<u8>>>,
    ) -> Self {
        let _mmr = Arc::new(Mutex::new(crate::indexing::storage::mmr::XaeroMmr::new()));
        let _store = Arc::new(store.unwrap_or_else(|| {
            SegmentWriterActor::new_with_config(super::segment_writer_actor::SegmentConfig {
                prefix: "mmr".to_string(),
                ..Default::default()
            })
        }));
        // Use the store's lmdb_env_path instead of hardcoded path
        let path = &_store.segment_config.lmdb_env_path;
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(path).expect("failed to create LmdbEnv"),
        ));
        let mdb_c = meta_db.clone();
        let _store_clone = _store.clone();
        let mmr_clone = _mmr.clone();
        let _listener = listener.unwrap_or_else(|| {
            EventListener::new(
                "mmr_indexing_actor",
                Arc::new(move |e: Event<Vec<u8>>| {
                    let framed = archive(&e);
                    // TODO: THIS SHOULD BE NON BLOCKING
                    push_event(&mdb_c, &e).expect("failed to push event");
                    let leaf_hash = sha_256(&framed);
                    {
                        mmr_clone
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .append(leaf_hash);
                    }
                    _store_clone
                        .inbox
                        .send(leaf_hash.to_vec())
                        .expect("failed to send event");
                }),
                None,
                Some(1), // single-threaded handler
            )
        });
        Self {
            _mmr,
            _store,
            listener: _listener,
            _lmdb: meta_db,
        }
    }

    /// Create an MMR actor with a custom SegmentConfig.
    pub fn new_with_config(
        config: SegmentConfig,
        listener: Option<EventListener<Vec<u8>>>,
    ) -> Self {
        // initialize in-memory MMR
        let _mmr = Arc::new(Mutex::new(crate::indexing::storage::mmr::XaeroMmr::new()));
        // create segment writer from config
        let store = Arc::new(SegmentWriterActor::new_with_config(config.clone()));
        // open LMDB using config path
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(&config.lmdb_env_path).expect("failed to create LmdbEnv"),
        ));
        let mdb_c = meta_db.clone();
        let store_clone = store.clone();
        let mmr_clone = _mmr.clone();
        // build listener
        let _listener = listener.unwrap_or_else(|| {
            EventListener::new(
                "mmr_indexing_actor",
                Arc::new(move |e: Event<Vec<u8>>| {
                    let framed = archive(&e);
                    // persist event metadata
                    push_event(&mdb_c, &e).expect("failed to push event");
                    // append to in-memory MMR
                    let leaf_hash = sha_256(&framed);
                    {
                        mmr_clone
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .append(leaf_hash);
                    }
                    // send to segment writer
                    store_clone
                        .inbox
                        .send(leaf_hash.to_vec())
                        .expect("failed to send event");
                }),
                None,
                Some(1),
            )
        });
        Self {
            _mmr,
            _store: store,
            listener: _listener,
            _lmdb: meta_db,
        }
    }

    pub fn mmr(&self) -> Arc<Mutex<crate::indexing::storage::mmr::XaeroMmr>> {
        Arc::clone(&self._mmr)
    }

    pub fn store(&self) -> Arc<SegmentWriterActor> {
        Arc::clone(&self._store)
    }

    pub fn lmdb(&self) -> Arc<Mutex<aof::LmdbEnv>> {
        Arc::clone(&self._lmdb)
    }
}

// at bottom of mmr_indexing_actor.rs

#[cfg(test)]
mod actor_tests {
    use std::time::Duration;

    use crossbeam::channel;
    use tempfile::{TempDir, tempdir};

    use super::*;
    use crate::{
        core::{event::Event, initialize},
        indexing::{
            hash::sha_256,
            storage::{actors::segment_writer_actor::SegmentConfig, format::archive},
        },
    };

    /// Build a simple Event from raw bytes.
    fn make_event(data: Vec<u8>) -> Event<Vec<u8>> {
        Event::new(data, 1)
    }

    /// Helper to push an event into the listener's inbox.
    fn fire_event(listener: &EventListener<Vec<u8>>, evt: Event<Vec<u8>>) {
        listener.inbox.send(evt).expect("failed to send event");
    }

    fn make_test_store(prefix: String) -> (TempDir, SegmentWriterActor) {
        let tmp = tempdir().expect("failed to create tempdir");
        // small page size so tests finish quickly, 1 page per segment to avoid rollover
        let cfg = SegmentConfig {
            page_size: 32,
            pages_per_segment: 1,
            prefix,
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };
        let store = SegmentWriterActor::new_with_config(cfg);
        (tmp, store)
    }

    #[test]
    fn actor_appends_to_in_memory_mmr() {
        initialize();
        // store config uses small pages; we only care about MMR here
        let (_tmp, store) = make_test_store("mmr".to_string());
        let actor = MmrIndexingActor::new(Some(store), None);

        // Fire one event
        let ev = make_event(b"foo".to_vec());
        fire_event(&actor.listener, ev);
        // wait until the listener has processed the event (or timeout)
        use std::time::{Duration, Instant};
        let start = Instant::now();
        while actor
            .listener
            .meta
            .events_processed
            .load(std::sync::atomic::Ordering::SeqCst)
            < 1
        {
            if start.elapsed() > Duration::from_secs(1) {
                panic!("Timed out waiting for listener to process event");
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // The MMR should contain exactly one leaf
        let mmr = actor._mmr.lock().expect("failed to lock MMR");
        assert_eq!(mmr.leaf_count(), 1, "MMR should have one leaf");
    }

    #[test]
    fn actor_persists_exact_leaf_hash_to_store() {
        let tmp = tempdir().expect("failed to create tempdir");
        std::env::set_current_dir(tmp.path()).expect("failed to set cwd");
        initialize();
        // capture what gets sent to disk
        let dir = tempdir().expect("failed to create tempdir");
        let prefix = dir.path().join("mmr").display().to_string();
        let (tx, rx) = channel::bounded::<Vec<u8>>(1);
        let (_tmp, mut store) = make_test_store(prefix.clone());
        store.inbox = tx.clone();

        let actor = MmrIndexingActor::new(Some(store), None);

        // Fire one event
        let ev = make_event(b"bar".to_vec());
        fire_event(&actor.listener, ev.clone());

        // Read the hash from the store's inbox
        let got = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("expected one leaf hash");

        // It must equal sha256( archive(&ev) )
        let expected = sha_256(&archive(&ev)).to_vec();
        assert_eq!(got, expected, "Persisted hash must match expected digest");
    }
}
