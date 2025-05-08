use std::sync::{Arc, Mutex};

use super::segment_writer_actor::SegmentWriterActor;
use crate::{
    core::{event::Event, listeners::EventListener},
    indexing::{
        hash::sha_256,
        storage::{format::archive, mmr::XaeroMmrOps},
    },
};

pub struct MmrIndexingActor {
    _mmr: Arc<Mutex<crate::indexing::storage::mmr::XaeroMmr>>,
    _store: Arc<SegmentWriterActor>,
    _listener: EventListener<Vec<u8>>,
}

impl MmrIndexingActor {
    pub fn new(
        store: Option<SegmentWriterActor>,
        listener: Option<EventListener<Vec<u8>>>,
    ) -> Self {
        let _mmr = Arc::new(Mutex::new(crate::indexing::storage::mmr::XaeroMmr::new()));
        let _store = Arc::new(store.unwrap_or_else(|| {
            SegmentWriterActor::new_with_config(super::segment_writer_actor::SegmentConfig {
                prefix: "xaero_mmr_".to_string(),
                ..Default::default()
            })
        }));
        let _store_clone = _store.clone();
        let mmr_clone = _mmr.clone();
        let _listener = listener.unwrap_or_else(|| {
            EventListener::new(
                "mmr_indexing_actor",
                Arc::new(move |e: Event<Vec<u8>>| {
                    let framed = archive(&e);
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
            _listener,
        }
    }
}

// at bottom of mmr_indexing_actor.rs

#[cfg(test)]
mod actor_tests {
    use std::time::Duration;

    use crossbeam::channel;
    use tempfile::tempdir;

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

    fn make_test_store(prefix: String) -> SegmentWriterActor {
        // small page size so tests finish quickly, 1 page per segment to avoid rollover
        let cfg = SegmentConfig {
            page_size: 32,
            pages_per_segment: 1,
            prefix,
        };
        SegmentWriterActor::new_with_config(cfg)
    }

    #[test]
    fn actor_appends_to_in_memory_mmr() {
        initialize();
        // store config uses small pages; we only care about MMR here
        let dir = tempdir().expect("failed to create tempdir");
        let store = make_test_store(dir.path().join("mmr").display().to_string());
        let actor = MmrIndexingActor::new(Some(store), None);

        // Fire one event
        let ev = make_event(b"foo".to_vec());
        fire_event(&actor._listener, ev);
        // **new**: let the listener thread process it
        std::thread::sleep(std::time::Duration::from_millis(50));

        // The MMR should contain exactly one leaf
        let mmr = actor._mmr.lock().expect("failed to lock MMR");
        assert_eq!(mmr.leaf_count(), 1, "MMR should have one leaf");
    }

    #[test]
    fn actor_persists_exact_leaf_hash_to_store() {
        initialize();
        // capture what gets sent to disk
        let dir = tempdir().expect("failed to create tempdir");
        let prefix = dir.path().join("mmr").display().to_string();
        let (tx, rx) = channel::bounded::<Vec<u8>>(1);
        let mut store = make_test_store(prefix.clone());
        store.inbox = tx.clone();

        let actor = MmrIndexingActor::new(Some(store), None);

        // Fire one event
        let ev = make_event(b"bar".to_vec());
        fire_event(&actor._listener, ev.clone());

        // Read the hash from the store's inbox
        let got = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("expected one leaf hash");

        // It must equal sha256( archive(&ev) )
        let expected = sha_256(&archive(&ev)).to_vec();
        assert_eq!(got, expected, "Persisted hash must match expected digest");
    }
}
