use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// A single cache entry: optional payload meta, MMR-appended flag, and timestamp.
struct CacheEntry {
    meta: Option<SegmentMeta>,
    mmr_appended: bool,
    timestamp: Instant,
}

use crate::{
    core::{
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, put_secondary_index},
        },
        event::SystemErrorCode,
    },
    system::control_bus::{ControlBus, SystemPayload},
};

pub struct SecondaryIndexActor {
    bus: Arc<ControlBus>,
    cache: Mutex<HashMap<[u8; 32], CacheEntry>>,
    lmdb_env: Arc<Mutex<LmdbEnv>>,
    gc_ttl: Duration,
}

impl SecondaryIndexActor {
    /// Process a single SystemPayload synchronously.
    pub fn handle_event(&self, evt: SystemPayload) {
        let now = Instant::now();
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        // Garbage‑collect stale entries
        cache.retain(|_, entry| now.duration_since(entry.timestamp) < self.gc_ttl);

        match evt {
            SystemPayload::PayloadWritten { leaf_hash, meta } => {
                let entry = cache.entry(leaf_hash).or_insert(CacheEntry {
                    meta: None,
                    mmr_appended: false,
                    timestamp: now,
                });
                entry.meta = Some(meta);
                entry.timestamp = now;

                if entry.mmr_appended {
                    drop(cache);
                    let res = put_secondary_index(&self.lmdb_env, &leaf_hash, &meta);
                    let tx = self.bus.sender();
                    if res.is_ok() {
                        tx.send(SystemPayload::SecondaryIndexWritten { leaf_hash })
                            .expect("failed_to_unravel");
                    } else {
                        tx.send(SystemPayload::SecondaryIndexFailed {
                            leaf_hash,
                            error_code: SystemErrorCode::SecondaryIndex as u16,
                        })
                        .expect("failed_to_unravel");
                    }
                }
            }
            SystemPayload::MmrAppended { leaf_hash } => {
                let entry = cache.entry(leaf_hash).or_insert(CacheEntry {
                    meta: None,
                    mmr_appended: false,
                    timestamp: now,
                });
                entry.mmr_appended = true;
                entry.timestamp = now;

                if let Some(meta) = entry.meta {
                    drop(cache);
                    let res = put_secondary_index(&self.lmdb_env, &leaf_hash, &meta);
                    let tx = self.bus.sender();
                    if res.is_ok() {
                        tx.send(SystemPayload::SecondaryIndexWritten { leaf_hash })
                            .expect("failed_to_unravel");
                    } else {
                        tx.send(SystemPayload::SecondaryIndexFailed {
                            leaf_hash,
                            error_code: SystemErrorCode::SecondaryIndex as u16,
                        })
                        .expect("failed_to_unravel");
                    }
                }
            }
            _ => {}
        }
    }

    pub fn new(bus: Arc<ControlBus>, lmdb_env: Arc<Mutex<LmdbEnv>>, gc_ttl: Duration) -> Arc<Self> {
        let actor = Arc::new(SecondaryIndexActor {
            bus: bus.clone(),
            cache: Mutex::new(HashMap::new()),
            lmdb_env: lmdb_env.clone(),
            gc_ttl,
        });

        let rx = bus.subscribe();
        let h = actor.clone();
        std::thread::spawn(move || {
            while let Ok(evt) = rx.recv() {
                h.handle_event(evt);
            }
        });

        actor
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tempfile::tempdir;

    use super::SecondaryIndexActor;
    use crate::{
        core::aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, get_secondary_index},
        },
        system::control_bus::{ControlBus, SystemPayload},
    };

    #[test]
    fn test_secondary_index_actor_write() {
        // Setup LMDB env and actor
        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"))
                .expect("failed_to_unravel"),
        ));
        let bus = Arc::new(ControlBus::new());
        let actor = SecondaryIndexActor::new(bus.clone(), env.clone(), Duration::from_secs(60));

        // Simulate payload written then MMR appended
        let meta = SegmentMeta {
            page_index: 1,
            segment_index: 2,
            write_pos: 10,
            byte_offset: 20,
            latest_segment_id: 3,
            ts_start: 1000,
            ts_end: 2000,
        };
        let leaf_hash = [1u8; 32];
        actor.handle_event(SystemPayload::PayloadWritten { leaf_hash, meta });
        actor.handle_event(SystemPayload::MmrAppended { leaf_hash });

        // Now should be indexed immediately
        let got = get_secondary_index(&env, &leaf_hash)
            .expect("get_secondary_index")
            .expect("meta missing");
        let m_pid = meta.page_index;
        let g_pid = got.page_index;
        assert_eq!(g_pid, m_pid);
    }

    #[test]
    fn test_secondary_index_actor_mmr_first() {
        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"))
                .expect("failed_to_unravel"),
        ));
        let bus = Arc::new(ControlBus::new());
        let actor = SecondaryIndexActor::new(bus.clone(), env.clone(), Duration::from_secs(60));

        let leaf_hash = [2u8; 32];
        actor.handle_event(SystemPayload::MmrAppended { leaf_hash });
        actor.handle_event(SystemPayload::PayloadWritten {
            leaf_hash,
            meta: SegmentMeta {
                page_index: 4,
                segment_index: 5,
                write_pos: 40,
                byte_offset: 80,
                latest_segment_id: 6,
                ts_start: 2000,
                ts_end: 3000,
            },
        });

        let got = get_secondary_index(&env, &leaf_hash)
            .expect("get_secondary_index")
            .expect("meta missing");
        let g_pid = got.page_index;
        assert_eq!(g_pid, 4);
    }
}
