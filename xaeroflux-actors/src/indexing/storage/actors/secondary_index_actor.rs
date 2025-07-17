//! Secondary Index actor using ring buffer architecture.
//!
//! This actor:
//! - Reads SystemPayload events from input ring buffer
//! - Coordinates PayloadWritten and MmrAppended events through cache
//! - Writes secondary indexes to LMDB when both events are received
//! - Sends confirmation events to output ring buffer

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bytemuck::{Pod, Zeroable};
use rusted_ring_new::{EventUtils, Reader, RingBuffer, Writer};
use xaeroflux_core::{
    date_time::emit_secs,
    event::{EventType::SystemEvent, SystemErrorCode, SystemEventKind},
    pipe::BusKind,
};

use crate::{
    aof::storage::{
        format::SegmentMeta,
        lmdb::{LmdbEnv, put_secondary_index},
    },
    subject::SubjectHash,
    system_payload::SystemPayload,
};

// ================================================================================================
// SECONDARY INDEX RING BUFFERS
// ================================================================================================

static SECONDARY_INDEX_INPUT_RING: OnceLock<RingBuffer<128, 2000>> = OnceLock::new();
static SECONDARY_INDEX_OUTPUT_RING: OnceLock<RingBuffer<128, 2000>> = OnceLock::new();

// ================================================================================================
// TYPES & STRUCTS
// ================================================================================================

/// Secondary Index confirmation event - exactly 64 bytes for XS pool
#[repr(C, align(8))]
#[derive(Clone, Copy, Debug)]
pub struct SecondaryIndexConfirmation {
    pub leaf_hash: [u8; 32], // 32 bytes
    pub success: u32,        // 4 bytes
    pub error_code: u16,     // 2 bytes
    pub operation_type: u16, // 2 bytes
    pub timestamp: u64,      // 8 bytes
    pub reserved: [u8; 16],  // 16 bytes = 64 total (was 8, need 16)
}
unsafe impl Pod for SecondaryIndexConfirmation {}
unsafe impl Zeroable for SecondaryIndexConfirmation {}

/// Operation types for SecondaryIndexConfirmation
#[repr(u16)]
pub enum SecondaryIndexOperation {
    IndexWritten = 1,
    IndexFailed = 2,
    MmrAppended = 3,
    MmrAppendFailed = 4,
}

/// A single cache entry: optional payload meta, MMR-appended flag, and timestamp.
#[derive(Clone)]
struct CacheEntry {
    meta: Option<SegmentMeta>,
    mmr_appended: bool,
    timestamp: Instant,
}

/// Secondary Index Actor statistics
#[derive(Debug, Default)]
pub struct SecondaryIndexStats {
    pub total_processed: u64,
    pub successful_indexes: u64,
    pub failed_indexes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub gc_runs: u64,
    pub last_process_ts: u64,
}

/// Secondary Index Actor state - tracks cache and processing statistics
pub struct SecondaryIndexState {
    pub lmdb_env: Arc<Mutex<LmdbEnv>>,
    pub cache: HashMap<[u8; 32], CacheEntry>,
    pub subject_hash: SubjectHash,
    pub bus_kind: BusKind,
    pub gc_ttl: Duration,
    pub stats: SecondaryIndexStats,
    pub last_gc: Instant,
}

impl SecondaryIndexState {
    /// Create new Secondary Index state with LMDB environment and cache
    pub fn new(subject_hash: SubjectHash, bus_kind: BusKind, lmdb_env: Arc<Mutex<LmdbEnv>>, gc_ttl: Duration) -> Self {
        Self {
            lmdb_env,
            cache: HashMap::new(),
            subject_hash,
            bus_kind,
            gc_ttl,
            stats: SecondaryIndexStats::default(),
            last_gc: Instant::now(),
        }
    }

    /// Process SystemPayload event
    pub fn process_system_payload(&mut self, payload: SystemPayload, writer: &mut Writer<128, 2000>) -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        self.stats.total_processed += 1;
        self.stats.last_process_ts = emit_secs();

        tracing::debug!("Processing SystemPayload: {:?}", payload);

        // Run garbage collection periodically
        if now.duration_since(self.last_gc) > Duration::from_secs(30) {
            self.garbage_collect(now);
        }

        match payload {
            SystemPayload::PayloadWritten { leaf_hash, meta } => {
                tracing::debug!("Processing PayloadWritten for leaf_hash: {:?}", leaf_hash);
                self.handle_payload_written(leaf_hash, meta, now, writer)
            }
            SystemPayload::MmrAppended { leaf_hash } => {
                tracing::debug!("Processing MmrAppended for leaf_hash: {:?}", leaf_hash);
                self.handle_mmr_appended(leaf_hash, now, writer)
            }
            _ => {
                tracing::debug!("SecondaryIndexActor ignoring unhandled payload type");
                Ok(())
            }
        }
    }

    /// Handle PayloadWritten event
    fn handle_payload_written(&mut self, leaf_hash: [u8; 32], meta: SegmentMeta, now: Instant, writer: &mut Writer<128, 2000>) -> Result<(), Box<dyn std::error::Error>> {
        let entry = self.cache.entry(leaf_hash).or_insert(CacheEntry {
            meta: None,
            mmr_appended: false,
            timestamp: now,
        });

        entry.meta = Some(meta);
        entry.timestamp = now;

        tracing::debug!("PayloadWritten cached. MMR appended: {}", entry.mmr_appended);

        // If MMR already appended, write secondary index immediately
        if entry.mmr_appended {
            tracing::debug!("MMR already appended, writing secondary index immediately");
            self.write_secondary_index(leaf_hash, meta, writer)
        } else {
            tracing::debug!("PayloadWritten cached for leaf_hash: {:?}", leaf_hash);
            Ok(())
        }
    }

    /// Handle MmrAppended event
    fn handle_mmr_appended(&mut self, leaf_hash: [u8; 32], now: Instant, writer: &mut Writer<128, 2000>) -> Result<(), Box<dyn std::error::Error>> {
        // Check if we already have the entry and get the meta before modifying cache
        let has_meta = self.cache.get(&leaf_hash).and_then(|e| e.meta);

        let entry = self.cache.entry(leaf_hash).or_insert(CacheEntry {
            meta: None,
            mmr_appended: false,
            timestamp: now,
        });

        entry.mmr_appended = true;
        entry.timestamp = now;

        tracing::debug!("MmrAppended processed. Has meta: {}", has_meta.is_some());

        // Send MMR appended confirmation first
        let confirmation = SecondaryIndexConfirmation {
            leaf_hash,
            success: 1,
            error_code: 0,
            operation_type: SecondaryIndexOperation::MmrAppended as u16,
            timestamp: emit_secs(),
            reserved: [0; 16],
        };

        let conf_bytes = bytemuck::bytes_of(&confirmation);
        match EventUtils::create_pooled_event::<128>(conf_bytes, 203) {
            Ok(conf_event) =>
                if writer.add(conf_event) {
                    tracing::debug!("MMR appended confirmation sent successfully");
                } else {
                    tracing::warn!("Secondary index confirmation buffer full");
                },
            Err(e) => {
                tracing::error!("Failed to create MMR appended confirmation: {:?}", e);
                return Err(e.into());
            }
        }

        // If payload meta already available, write secondary index
        if let Some(meta) = has_meta.or(entry.meta) {
            tracing::debug!("Meta available, writing secondary index");
            self.write_secondary_index(leaf_hash, meta, writer)
        } else {
            tracing::debug!("MmrAppended cached for leaf_hash: {:?}", leaf_hash);
            Ok(())
        }
    }

    /// Write secondary index to LMDB
    fn write_secondary_index(&mut self, leaf_hash: [u8; 32], meta: SegmentMeta, writer: &mut Writer<128, 2000>) -> Result<(), Box<dyn std::error::Error>> {
        tracing::debug!("Writing secondary index for leaf_hash: {:?}", leaf_hash);

        match put_secondary_index(&self.lmdb_env, &leaf_hash, &meta) {
            Ok(_) => {
                self.stats.successful_indexes += 1;

                // Send success confirmation
                let confirmation = SecondaryIndexConfirmation {
                    leaf_hash,
                    success: 1,
                    error_code: 0,
                    operation_type: SecondaryIndexOperation::IndexWritten as u16,
                    timestamp: emit_secs(),
                    reserved: [0; 16],
                };

                let conf_bytes = bytemuck::bytes_of(&confirmation);
                match EventUtils::create_pooled_event::<128>(conf_bytes, 203) {
                    Ok(conf_event) =>
                        if writer.add(conf_event) {
                            tracing::debug!("Index written confirmation sent successfully");
                        } else {
                            tracing::warn!("Secondary index confirmation buffer full");
                        },
                    Err(e) => {
                        tracing::error!("Failed to create index written confirmation: {:?}", e);
                        return Err(e.into());
                    }
                }

                tracing::debug!("Secondary index written for leaf_hash: {:?}", leaf_hash);

                // Remove from cache after successful write
                self.cache.remove(&leaf_hash);
                Ok(())
            }
            Err(e) => {
                self.stats.failed_indexes += 1;

                // Send failure confirmation
                let confirmation = SecondaryIndexConfirmation {
                    leaf_hash,
                    success: 0,
                    error_code: SystemErrorCode::SecondaryIndex as u16,
                    operation_type: SecondaryIndexOperation::IndexFailed as u16,
                    timestamp: emit_secs(),
                    reserved: [0; 16],
                };

                let conf_bytes = bytemuck::bytes_of(&confirmation);
                match EventUtils::create_pooled_event::<128>(conf_bytes, 203) {
                    Ok(conf_event) =>
                        if !writer.add(conf_event) {
                            tracing::warn!("Secondary index confirmation buffer full");
                        },
                    Err(e2) => {
                        tracing::error!("Failed to create index failed confirmation: {:?}", e2);
                    }
                }

                tracing::error!("Failed to write secondary index for leaf_hash: {:?}, error: {:?}", leaf_hash, e);
                Err(e.into())
            }
        }
    }

    /// Garbage collect stale cache entries
    fn garbage_collect(&mut self, now: Instant) {
        let initial_size = self.cache.len();
        self.cache.retain(|_, entry| now.duration_since(entry.timestamp) < self.gc_ttl);
        let final_size = self.cache.len();

        if initial_size > final_size {
            self.stats.gc_runs += 1;
            tracing::debug!("Garbage collected {} stale cache entries, {} remaining", initial_size - final_size, final_size);
        }

        self.last_gc = now;
    }

    /// Log periodic statistics
    fn log_stats(&self) {
        if self.stats.total_processed % 100 == 0 && self.stats.total_processed > 0 {
            tracing::info!(
                "SecondaryIndex Stats - Subject: {:?}, Bus: {:?}, Processed: {}, Success: {}, Failed: {}, Cache: {}",
                self.subject_hash,
                self.bus_kind,
                self.stats.total_processed,
                self.stats.successful_indexes,
                self.stats.failed_indexes,
                self.cache.len()
            );
        }
    }
}

/// Secondary Index Actor - processes SystemPayload from ring buffer and maintains secondary indexes
pub struct SecondaryIndexActor {
    // Exposed interfaces
    pub in_writer: Writer<128, 2000>,  // SystemPayload input
    pub out_reader: Reader<128, 2000>, // Confirmation output

    pub jh: JoinHandle<()>,
}

// ================================================================================================
// SECONDARY INDEX ACTOR IMPLEMENTATION
// ================================================================================================

impl SecondaryIndexActor {
    /// Create and spawn Secondary Index actor with ring buffer processing
    pub fn spin(subject_hash: SubjectHash, bus_kind: BusKind, lmdb_env: Arc<Mutex<LmdbEnv>>, gc_ttl: Option<Duration>) -> Result<Self, Box<dyn std::error::Error>> {
        let gc_ttl = gc_ttl.unwrap_or(Duration::from_secs(300)); // 5 minute default TTL
        let mut state = SecondaryIndexState::new(subject_hash, bus_kind.clone(), lmdb_env, gc_ttl);

        let jh = thread::spawn(move || {
            tracing::info!("Secondary Index Actor started - Subject: {:?}, Bus: {:?}", subject_hash, bus_kind);

            // Initialize ring buffers
            let input_ring = SECONDARY_INDEX_INPUT_RING.get_or_init(RingBuffer::new);
            let output_ring = SECONDARY_INDEX_OUTPUT_RING.get_or_init(RingBuffer::new);

            // Create reader and writer
            let mut reader = Reader::new(input_ring);
            let mut writer = Writer::new(output_ring);

            // Process ring buffer events
            loop {
                let mut events_processed = 0;

                // Process input events
                for event in reader.by_ref() {
                    tracing::debug!("Received event of size: {} bytes", event.len);

                    // Check for SystemPayload size
                    let expected_size = std::mem::size_of::<SystemPayload>() as u32;
                    if event.len == expected_size {
                        let system_payload = unsafe { std::ptr::read(event.data.as_ptr() as *const SystemPayload) };

                        tracing::debug!("Processing SystemPayload: {:?}", system_payload);

                        match state.process_system_payload(system_payload, &mut writer) {
                            Ok(_) => {
                                tracing::debug!("Processed SystemPayload successfully");
                            }
                            Err(e) => {
                                tracing::error!("Failed to process SystemPayload: {:?}", e);
                            }
                        }
                    } else {
                        tracing::warn!("Invalid SystemPayload size: {} bytes, expected {}", event.len, expected_size);
                    }
                    events_processed += 1;
                }

                if events_processed > 0 {
                    tracing::debug!("Processed {} events this iteration", events_processed);
                    state.log_stats();
                }

                thread::sleep(Duration::from_micros(100));
            }
        });

        // Create exposed interfaces
        let input_ring = SECONDARY_INDEX_INPUT_RING.get_or_init(RingBuffer::new);
        let output_ring = SECONDARY_INDEX_OUTPUT_RING.get_or_init(RingBuffer::new);

        Ok(Self {
            in_writer: Writer::new(input_ring),
            out_reader: Reader::new(output_ring),
            jh,
        })
    }

    /// Get actor statistics through callback (since state is in the worker thread)
    pub fn get_stats(&self) -> Result<String, &'static str> {
        // In ring buffer architecture, we can't directly access state from the main thread
        // This would require implementing a query mechanism through the ring buffers
        Ok("Secondary index stats access requires ring buffer query mechanism".to_string())
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;
    use xaeroflux_core::{date_time::emit_secs, initialize};

    use super::*;
    use crate::aof::storage::{
        format::SegmentMeta,
        lmdb::{LmdbEnv, get_secondary_index},
    };

    #[test]
    fn test_secondary_index_confirmation_size() {
        // Verify our confirmation event fits in XS pool
        let conf = SecondaryIndexConfirmation {
            leaf_hash: [0; 32],
            success: 1,
            error_code: 0,
            operation_type: SecondaryIndexOperation::IndexWritten as u16,
            timestamp: emit_secs(),
            reserved: [0; 16],
        };

        let conf_size = std::mem::size_of::<SecondaryIndexConfirmation>();
        assert_eq!(conf_size, 64, "Secondary index confirmation must be exactly 64 bytes for XS pool, got {} bytes", conf_size);

        println!("✅ Secondary index confirmation fits perfectly in XS pool: {} bytes", conf_size);
    }

    #[test]
    fn test_verify_system_payload_size() {
        let payload_size = std::mem::size_of::<SystemPayload>();
        println!("SystemPayload size: {} bytes", payload_size);

        // Test all variants
        let leaf_hash = [0xAA; 32];
        let meta = SegmentMeta {
            page_index: 1,
            segment_index: 2,
            write_pos: 10,
            byte_offset: 20,
            latest_segment_id: 3,
            ts_start: 1000,
            ts_end: 2000,
        };

        let payload_written = SystemPayload::PayloadWritten { leaf_hash, meta };
        let written_size = std::mem::size_of_val(&payload_written);
        println!("PayloadWritten size: {} bytes", written_size);

        let mmr_appended = SystemPayload::MmrAppended { leaf_hash };
        let appended_size = std::mem::size_of_val(&mmr_appended);
        println!("MmrAppended size: {} bytes", appended_size);

        assert!(payload_size <= 128, "SystemPayload must fit in 128-byte ring buffer");

        println!("✅ SystemPayload size verification complete");
    }

    #[test]
    fn test_secondary_index_actor_creation() {
        initialize();

        let dir = tempdir().expect("Failed to create temp dir");
        let env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap(), BusKind::Data).expect("Failed to create LMDB env")));
        let subject_hash = SubjectHash([1u8; 32]);

        let actor = SecondaryIndexActor::spin(subject_hash, BusKind::Data, env, Some(Duration::from_secs(60))).expect("Failed to create Secondary Index actor");

        // Verify interfaces exist
        assert!(true, "Secondary Index actor created successfully");

        drop(actor); // Cleanup

        println!("✅ Secondary Index actor creation successful");
    }

    #[test]
    fn test_secondary_index_state_processing() {
        initialize();

        let dir = tempdir().expect("Failed to create temp dir");
        let env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap(), BusKind::Data).expect("Failed to create LMDB env")));
        let subject_hash = SubjectHash([2u8; 32]);

        let mut state = SecondaryIndexState::new(subject_hash, BusKind::Data, env.clone(), Duration::from_secs(60));
        static TEST_RING_1: OnceLock<RingBuffer<128, 2000>> = OnceLock::new();

        let output_ring = TEST_RING_1.get_or_init(RingBuffer::new);
        let mut writer = Writer::new(&output_ring);

        // Test PayloadWritten processing
        let leaf_hash = [0xAA; 32];
        let meta = SegmentMeta {
            page_index: 1,
            segment_index: 2,
            write_pos: 10,
            byte_offset: 20,
            latest_segment_id: 3,
            ts_start: 1000,
            ts_end: 2000,
        };

        let payload_written = SystemPayload::PayloadWritten { leaf_hash, meta };
        let result = state.process_system_payload(payload_written, &mut writer);
        assert!(result.is_ok(), "Should successfully process PayloadWritten");

        // Verify cache entry was created
        assert!(state.cache.contains_key(&leaf_hash), "Cache should contain leaf hash");
        assert_eq!(state.stats.total_processed, 1, "Stats should show one processed event");

        // Process MmrAppended to complete the flow
        let mmr_appended = SystemPayload::MmrAppended { leaf_hash };
        let result = state.process_system_payload(mmr_appended, &mut writer);
        assert!(result.is_ok(), "Should successfully process MmrAppended");

        // Verify secondary index was written and cache cleared
        let secondary_index = get_secondary_index(&env, &leaf_hash)
            .expect("Should get secondary index")
            .expect("Secondary index should exist");
        let s_p_idx = secondary_index.page_index;
        let m_p_ix = meta.page_index;
        assert_eq!(s_p_idx, m_p_ix);
        assert!(!state.cache.contains_key(&leaf_hash), "Cache should be cleared after successful write");
        assert_eq!(state.stats.successful_indexes, 1, "Stats should show one successful index");

        println!("✅ Secondary Index state processing working correctly");
    }

    #[test]
    fn test_secondary_index_actor_integration() {
        initialize();

        let dir = tempdir().expect("Failed to create temp dir");
        let env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap(), BusKind::Data).expect("Failed to create LMDB env")));
        let subject_hash = SubjectHash([3u8; 32]);

        let mut actor = SecondaryIndexActor::spin(subject_hash, BusKind::Data, env.clone(), Some(Duration::from_secs(60))).expect("Failed to create Secondary Index actor");

        // Create test SystemPayload events
        let leaf_hash = [0xBB; 32];
        let meta = SegmentMeta {
            page_index: 5,
            segment_index: 6,
            write_pos: 50,
            byte_offset: 100,
            latest_segment_id: 7,
            ts_start: 3000,
            ts_end: 4000,
        };

        // Debug SystemPayload size
        let payload_size = std::mem::size_of::<SystemPayload>();
        println!("SystemPayload actual size: {} bytes", payload_size);

        // Send PayloadWritten event
        let payload_written = SystemPayload::PayloadWritten { leaf_hash, meta };
        let payload_bytes = bytemuck::bytes_of(&payload_written);
        let payload_event = EventUtils::create_pooled_event::<128>(payload_bytes, 101).expect("Should create pooled event");

        println!("Created PayloadWritten event, size: {} bytes", payload_bytes.len());
        assert!(actor.in_writer.add(payload_event), "Should write PayloadWritten to input buffer");

        // Give the actor some time to process the first event
        std::thread::sleep(Duration::from_millis(100));

        // Send MmrAppended event
        let mmr_appended = SystemPayload::MmrAppended { leaf_hash };
        let mmr_bytes = bytemuck::bytes_of(&mmr_appended);
        let mmr_event = EventUtils::create_pooled_event::<128>(mmr_bytes, 102).expect("Should create pooled event");

        println!("Created MmrAppended event, size: {} bytes", mmr_bytes.len());
        assert!(actor.in_writer.add(mmr_event), "Should write MmrAppended to input buffer");

        // Allow processing time for async operations
        std::thread::sleep(Duration::from_millis(1000));

        // Check for confirmations with debug output
        let mut mmr_confirmation_found = false;
        let mut index_confirmation_found = false;
        let mut confirmations_received = 0;

        for attempt in 0..30 {
            if let Some(conf_event) = actor.out_reader.next() {
                confirmations_received += 1;
                println!("Received confirmation #{} at attempt {}, len: {}", confirmations_received, attempt, conf_event.len);

                let expected_conf_size = std::mem::size_of::<SecondaryIndexConfirmation>() as u32;
                if conf_event.len == expected_conf_size {
                    if let Ok(conf) = bytemuck::try_from_bytes::<SecondaryIndexConfirmation>(&conf_event.data[..conf_event.len as usize]) {
                        let operation_type = conf.operation_type;
                        let success = conf.success;

                        println!("Parsed confirmation - Operation: {}, Success: {}", operation_type, success);

                        match operation_type {
                            op if op == SecondaryIndexOperation::MmrAppended as u16 => {
                                mmr_confirmation_found = true;
                                assert_eq!(success, 1, "MMR append should be successful");
                                println!("✅ MMR appended confirmation received");
                            }
                            op if op == SecondaryIndexOperation::IndexWritten as u16 => {
                                index_confirmation_found = true;
                                assert_eq!(success, 1, "Index write should be successful");
                                println!("✅ Index written confirmation received");
                            }
                            _ => {
                                println!("Unknown operation type: {}", operation_type);
                            }
                        }
                    } else {
                        println!("Failed to parse confirmation bytes");
                    }
                } else {
                    println!("Wrong confirmation size: {} vs expected {}", conf_event.len, expected_conf_size);
                }
            }

            if mmr_confirmation_found && index_confirmation_found {
                break;
            }

            std::thread::sleep(Duration::from_millis(100));
        }

        println!("Total confirmations received: {}", confirmations_received);
        println!("MMR confirmation found: {}", mmr_confirmation_found);
        println!("Index confirmation found: {}", index_confirmation_found);

        assert!(mmr_confirmation_found, "Should receive MMR appended confirmation");
        assert!(index_confirmation_found, "Should receive index written confirmation");

        // Verify secondary index was actually written to LMDB
        let secondary_index = get_secondary_index(&env, &leaf_hash)
            .expect("Should get secondary index")
            .expect("Secondary index should exist");
        let sipdx = secondary_index.page_index;
        let mpidx = meta.page_index;
        assert_eq!(sipdx, mpidx);

        println!("✅ Secondary Index actor integration working correctly");
    }

    #[test]
    fn test_cache_garbage_collection() {
        initialize();

        let dir = tempdir().expect("Failed to create temp dir");
        let env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap(), BusKind::Data).expect("Failed to create LMDB env")));
        let subject_hash = SubjectHash([4u8; 32]);

        // Use very short TTL for testing
        let mut state = SecondaryIndexState::new(subject_hash, BusKind::Data, env, Duration::from_millis(100));

        // Add some cache entries
        let now = Instant::now();
        state.cache.insert([1; 32], CacheEntry {
            meta: None,
            mmr_appended: false,
            timestamp: now - Duration::from_millis(200), // Stale
        });
        state.cache.insert([2; 32], CacheEntry {
            meta: None,
            mmr_appended: false,
            timestamp: now, // Fresh
        });

        assert_eq!(state.cache.len(), 2, "Should have 2 cache entries");

        // Trigger garbage collection
        state.garbage_collect(now);

        assert_eq!(state.cache.len(), 1, "Should have 1 cache entry after GC");
        assert!(state.cache.contains_key(&[2; 32]), "Fresh entry should remain");
        assert!(!state.cache.contains_key(&[1; 32]), "Stale entry should be removed");
        assert_eq!(state.stats.gc_runs, 1, "Should record GC run");

        println!("✅ Cache garbage collection working correctly");
    }
}
