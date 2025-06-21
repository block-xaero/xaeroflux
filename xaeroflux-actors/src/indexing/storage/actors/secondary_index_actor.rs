use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use xaeroflux_core::{
    event::{EventType::SystemEvent, SystemErrorCode, SystemEventKind, XaeroEvent},
    pipe::{BusKind, Pipe},
    date_time::emit_secs,
    XaeroPoolManager,
};

use crate::{
    aof::storage::{
        format::SegmentMeta,
        lmdb::{LmdbEnv, put_secondary_index},
    },
    system_payload::SystemPayload,
};

/// A single cache entry: optional payload meta, MMR-appended flag, and timestamp.
struct CacheEntry {
    meta: Option<SegmentMeta>,
    mmr_appended: bool,
    timestamp: Instant,
}

pub struct SecondaryIndexActor {
    pub pipe: Arc<Pipe>,
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
                    let tx = self.pipe.sink.tx.clone();

                    if res.is_ok() {
                        let success_event = XaeroPoolManager::create_xaero_event(
                            bytemuck::bytes_of(&SystemPayload::MMRLeafAppended { leaf_hash }),
                            SystemEvent(SystemEventKind::MmrAppended).to_u8(),
                            None, None, None,
                            emit_secs(),
                        ).unwrap_or_else(|pool_error| {
                            tracing::error!("Pool allocation failed for MMRLeafAppended: {:?}", pool_error);
                            panic!("Cannot create MMRLeafAppended event - ring buffer pool exhausted");
                        });

                        tx.send(success_event).expect("failed to send MMRLeafAppended event");
                    } else {
                        let failure_event = XaeroPoolManager::create_xaero_event(
                            bytemuck::bytes_of(&SystemPayload::MmrAppendFailed {
                                leaf_hash,
                                error_code: SystemErrorCode::MmrAppend as u16,
                            }),
                            SystemEvent(SystemEventKind::MmrAppendFailed).to_u8(),
                            None, None, None,
                            emit_secs(),
                        ).unwrap_or_else(|pool_error| {
                            tracing::error!("Pool allocation failed for MmrAppendFailed: {:?}", pool_error);
                            panic!("Cannot create MmrAppendFailed event - ring buffer pool exhausted");
                        });

                        tx.send(failure_event).expect("failed to send MmrAppendFailed event");
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
                    let tx = self.pipe.sink.tx.clone();

                    if res.is_ok() {
                        let success_event = XaeroPoolManager::create_xaero_event(
                            bytemuck::bytes_of(&SystemPayload::SecondaryIndexWritten { leaf_hash }),
                            SystemEvent(SystemEventKind::SecondaryIndexWritten).to_u8(),
                            None, None, None,
                            emit_secs(),
                        ).unwrap_or_else(|pool_error| {
                            tracing::error!("Pool allocation failed for SecondaryIndexWritten: {:?}", pool_error);
                            panic!("Cannot create SecondaryIndexWritten event - ring buffer pool exhausted");
                        });

                        tx.send(success_event).expect("failed to send SecondaryIndexWritten event");
                    } else {
                        let failure_event = XaeroPoolManager::create_xaero_event(
                            bytemuck::bytes_of(&SystemPayload::SecondaryIndexFailed {
                                leaf_hash,
                                error_code: SystemErrorCode::SecondaryIndex as u16,
                            }),
                            SystemEvent(SystemEventKind::SecondaryIndexFailed).to_u8(),
                            None, None, None,
                            emit_secs(),
                        ).unwrap_or_else(|pool_error| {
                            tracing::error!("Pool allocation failed for SecondaryIndexFailed: {:?}", pool_error);
                            panic!("Cannot create SecondaryIndexFailed event - ring buffer pool exhausted");
                        });

                        tx.send(failure_event).expect("failed to send SecondaryIndexFailed event");
                    }
                }
            }
            _ => {
                tracing::debug!("SecondaryIndexActor ignoring unhandled event type");
            }
        }
    }

    pub fn new(pipe: Arc<Pipe>, lmdb_env: Arc<Mutex<LmdbEnv>>, gc_ttl: Duration) -> Arc<Self> {
        XaeroPoolManager::init();

        let actor = Arc::new(SecondaryIndexActor {
            pipe: pipe.clone(),
            cache: Mutex::new(HashMap::new()),
            lmdb_env: lmdb_env.clone(),
            gc_ttl,
        });

        // Start event processing thread
        let actor_clone = actor.clone();
        let pipe_clone = pipe.clone();

        std::thread::Builder::new()
            .name("secondary-index-actor".to_string())
            .spawn(move || {
                tracing::info!("SecondaryIndexActor processing thread started");

                while let Ok(xaero_event) = pipe_clone.source.rx.recv() {
                    // Extract SystemPayload from XaeroEvent data
                    let event_data = xaero_event.data();

                    if event_data.len() >= std::mem::size_of::<SystemPayload>() {
                        match bytemuck::try_from_bytes::<SystemPayload>(event_data) {
                            Ok(system_payload) => {
                                tracing::debug!(
                                    "Processing SystemPayload: type={:?}",
                                    std::mem::discriminant(system_payload)
                                );
                                actor_clone.handle_event(*system_payload);
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to parse SystemPayload from XaeroEvent data: {:?}, data_len={}",
                                    e,
                                    event_data.len()
                                );
                            }
                        }
                    } else {
                        tracing::warn!(
                            "XaeroEvent data too small for SystemPayload: {} bytes, expected at least {}",
                            event_data.len(),
                            std::mem::size_of::<SystemPayload>()
                        );
                    }
                }

                tracing::info!("SecondaryIndexActor processing thread terminated");
            })
            .expect("failed to spawn secondary index actor thread");

        actor
    }
}

impl Drop for SecondaryIndexActor {
    fn drop(&mut self) {
        tracing::info!("SecondaryIndexActor dropping - cleaning up cache");
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
        thread::sleep,
    };

    use tempfile::tempdir;
    use xaeroflux_core::{initialize, XaeroPoolManager, date_time::emit_secs};

    use super::SecondaryIndexActor;
    use crate::{
        BusKind, Pipe,
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, get_secondary_index},
        },
        system_payload::SystemPayload,
    };

    #[test]
    fn test_secondary_index_actor_write() {
        initialize();
        XaeroPoolManager::init();

        // Setup LMDB env and actor
        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data)
                .expect("failed_to_unravel"),
        ));
        let pipe = Pipe::new(BusKind::Data, None);
        let actor = SecondaryIndexActor::new(pipe.clone(), env.clone(), Duration::from_secs(60));

        // Create test events using XaeroPoolManager
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

        // Send PayloadWritten event
        let payload_written = SystemPayload::PayloadWritten { leaf_hash, meta };
        let payload_written_event = XaeroPoolManager::create_xaero_event(
            bytemuck::bytes_of(&payload_written),
            0, // Using 0 as placeholder event type
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.source.tx.send(payload_written_event).expect("failed to send PayloadWritten");

        // Send MmrAppended event
        let mmr_appended = SystemPayload::MmrAppended { leaf_hash };
        let mmr_appended_event = XaeroPoolManager::create_xaero_event(
            bytemuck::bytes_of(&mmr_appended),
            0, // Using 0 as placeholder event type
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.source.tx.send(mmr_appended_event).expect("failed to send MmrAppended");

        // Allow processing time
        sleep(Duration::from_millis(100));

        // Verify secondary index was created
        let got = get_secondary_index(&env, &leaf_hash)
            .expect("get_secondary_index")
            .expect("meta missing");

        // Use unaligned access pattern
        let unaligned_meta_page_index = meta.page_index;
        let unaligned_got_page_index = got.page_index;
        assert_eq!(unaligned_got_page_index, unaligned_meta_page_index);
    }

    #[test]
    fn test_secondary_index_actor_mmr_first() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data)
                .expect("failed_to_unravel"),
        ));
        let pipe = Pipe::new(BusKind::Data, None);
        let actor = SecondaryIndexActor::new(pipe.clone(), env.clone(), Duration::from_secs(60));

        let leaf_hash = [2u8; 32];
        let meta = SegmentMeta {
            page_index: 4,
            segment_index: 5,
            write_pos: 40,
            byte_offset: 80,
            latest_segment_id: 6,
            ts_start: 2000,
            ts_end: 3000,
        };

        // Send MmrAppended first
        let mmr_appended = SystemPayload::MmrAppended { leaf_hash };
        let mmr_appended_event = XaeroPoolManager::create_xaero_event(
            bytemuck::bytes_of(&mmr_appended),
            0, // Using 0 as placeholder event type
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.source.tx.send(mmr_appended_event).expect("failed to send MmrAppended");

        // Then send PayloadWritten
        let payload_written = SystemPayload::PayloadWritten { leaf_hash, meta };
        let payload_written_event = XaeroPoolManager::create_xaero_event(
            bytemuck::bytes_of(&payload_written),
            0, // Using 0 as placeholder event type
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        pipe.source.tx.send(payload_written_event).expect("failed to send PayloadWritten");

        // Allow processing time
        sleep(Duration::from_millis(100));

        // Verify secondary index was created
        let got = get_secondary_index(&env, &leaf_hash)
            .expect("get_secondary_index")
            .expect("meta missing");

        // Use unaligned access pattern
        let unaligned_got_page_index = got.page_index;
        assert_eq!(unaligned_got_page_index, 4);
    }

    #[test]
    fn test_secondary_index_actor_zero_copy_access() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data)
                .expect("failed_to_unravel"),
        ));
        let pipe = Pipe::new(BusKind::Data, None);
        let _actor = SecondaryIndexActor::new(pipe.clone(), env.clone(), Duration::from_secs(60));

        // Test zero-copy data access
        let leaf_hash = [3u8; 32];
        let meta = SegmentMeta {
            page_index: 10,
            segment_index: 11,
            write_pos: 100,
            byte_offset: 200,
            latest_segment_id: 12,
            ts_start: 3000,
            ts_end: 4000,
        };
        let payload = SystemPayload::PayloadWritten { leaf_hash, meta };
        let expected_data = bytemuck::bytes_of(&payload);

        let test_event = XaeroPoolManager::create_xaero_event(
            expected_data,
            0, // Using 0 as placeholder event type
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        // Verify zero-copy access works
        assert_eq!(test_event.data(), expected_data);

        pipe.source.tx.send(test_event).expect("failed to send test event");
        sleep(Duration::from_millis(50));
    }
}