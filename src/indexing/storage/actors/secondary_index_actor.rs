use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    core::{aof::LmdbEnv, event::Event, listeners::EventListener, meta::SegmentMeta, IO_POOL},
    system::control_bus::SystemPayload,
};

pub struct SecondaryIndexActor {
    /// cache for the secondary index.
    pub(crate) cache: HashMap<[u8; 32], (Option<SegmentMeta>, bool, Instant)>,
    pub(crate) lmdb_env: Arc<Mutex<LmdbEnv>>,
    // The TTL for the garbage collection of the secondary index.
    pub(crate) gc_ttl: Duration,
    pub listener: EventListener<SystemPayload>,
}

impl SecondaryIndexActor {
    pub fn new(lmdb_env: Arc<Mutex<LmdbEnv>>, gc_ttl: Duration) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<Event<SystemPayload>>();
        let txc = tx.clone();
        let listener = EventListener::new(
            "secondary_index_actor",
            Arc::new(move |e: Event<SystemPayload>| {
                txc.send(e).expect("failed to send event");
            }),
            None,
            Some(1), // single-threaded handler
        );

        IO_POOL.get()
        .expect("IO_POOL not initialized")
        .execute(move || {
            let env = lmdb_env.lock().expect("Failed to lock LMDB environment");
            env.dbis[1]
        });

        Self {
            cache: HashMap::new(),
            lmdb_env,
            gc_ttl,
            listener,
        }
    }

    /// Checks if the secondary index is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}
