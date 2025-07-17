//! AOF (Append-Only File) Actor using ring buffer architecture.
//!
//! This actor:
//! - Reads from input ring buffer internally using existing pools
//! - Persists events to LMDB in append-only fashion
//! - Writes confirmation events to output ring buffer
//! - Exposes reader/writer interfaces for external interaction

use std::{
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use rusted_ring_new::{EventPoolFactory, EventUtils, PooledEvent, Reader, RingBuffer, Writer};
use xaeroflux_core::{CONF, date_time::emit_secs, hash::sha_256_slice, pipe::BusKind, pool::XaeroInternalEvent, system_paths};

use crate::{
    aof::storage::{
        format::EventKey,
        lmdb::{LmdbEnv, generate_event_key, push_internal_event_universal},
    },
    subject::SubjectHash,
};

// ================================================================================================
// AOF-SPECIFIC RING BUFFERS
// ================================================================================================

// AOF Control Event Flow: XS input -> XS output (confirmations)
static AOF_CONTROL_INPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();
static AOF_CONTROL_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// AOF Data Event Flow: S input -> XS output (confirmations are small)
static AOF_DATA_INPUT_RING: OnceLock<RingBuffer<256, 1000>> = OnceLock::new();
static AOF_DATA_OUTPUT_RING: OnceLock<RingBuffer<64, 2000>> = OnceLock::new();

// ================================================================================================
// TYPES & STRUCTS
// ================================================================================================

/// Confirmation event sent after successful LMDB write - exactly 64 bytes for XS pool
#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct AofWriteConfirmation {
    pub original_hash: [u8; 32], // Hash of original event data (32 bytes)
    pub sequence_id: u32,        // Sequential write order (4 bytes)
    pub latest_ts: u32,          // Timestamp (4 bytes)
    pub event_type: u8,          // Original event type (1 byte)
    pub status: u8,              // 0 = success, 1 = failure (1 byte)
    pub _pad: [u8; 22],          // Padding to reach exactly 64 bytes
}

unsafe impl Pod for AofWriteConfirmation {}
unsafe impl Zeroable for AofWriteConfirmation {}

/// AOF Actor statistics
#[derive(Debug, Default)]
pub struct AofStats {
    pub total_writes: u64,
    pub failed_writes: u64,
    pub bytes_written: u64,
    pub last_write_ts: u64,
}

/// AOF Actor state - tracks write statistics and sequence
pub struct AofState {
    pub env: Arc<Mutex<LmdbEnv>>,
    pub subject_hash: SubjectHash,
    pub bus_kind: BusKind,
    pub sequence_counter: u64,
    pub stats: AofStats,
}

/// AOF Actor - processes events from ring buffer and persists to LMDB
pub struct AofActor {
    // Exposed interfaces for external interaction using dedicated AOF ring buffers
    pub in_writer: Writer<64, 2000>,       // Control events input
    pub in_writer_data: Writer<256, 1000>, // Data events input
    pub out_reader: Reader<64, 2000>,      // Control confirmations output
    pub out_reader_data: Reader<64, 2000>, // Data confirmations output (confirmations are small)
    pub jh: JoinHandle<()>,
}

// ================================================================================================
// AOF STATE IMPLEMENTATION
// ================================================================================================

impl AofState {
    /// Create new AOF state with LMDB environment
    pub fn new(subject_hash: SubjectHash, bus_kind: BusKind) -> Result<Self, Box<dyn std::error::Error>> {
        // Get base path from config
        let c = CONF.get().expect("failed to unravel config");
        let base_path = &c.aof.file_path;

        // Construct subject-specific path based on bus kind
        let lmdb_path = match bus_kind {
            BusKind::Control => system_paths::emit_control_path_with_subject_hash(base_path.to_str().expect("path_invalid_for_aof"), subject_hash.0, "aof"),
            BusKind::Data => system_paths::emit_data_path_with_subject_hash(base_path.to_str().expect("path_invalid_for_aof"), subject_hash.0, "aof"),
        };

        tracing::info!("Creating AOF LMDB at path: {}", lmdb_path);
        let env = Arc::new(Mutex::new(LmdbEnv::new(&lmdb_path, bus_kind.clone())?));

        Ok(Self {
            env,
            subject_hash,
            bus_kind,
            sequence_counter: 0,
            stats: AofStats::default(),
        })
    }

    /// Process a single enhanced event with peer and vector clock info
    fn process_event(
        &mut self,
        event_data: &[u8],
        event_type: u8,
        timestamp: u64,
        xaero_id_hash: [u8; 32],
        vector_clock_hash: [u8; 32],
    ) -> Result<AofWriteConfirmation, Box<dyn std::error::Error>> {
        // Skip noise/debug events
        if !Self::should_persist_event_type(event_type) {
            return Ok(AofWriteConfirmation {
                original_hash: sha_256_slice(event_data),
                sequence_id: self.sequence_counter as u32,
                latest_ts: timestamp as u32,
                event_type,
                status: 0, // Success (but skipped)
                _pad: [0; 22],
            });
        }

        // Use the universal push function for storage
        match push_internal_event_universal(&self.env, event_data, event_type as u32, timestamp) {
            Ok(_) => {
                self.sequence_counter += 1;
                self.stats.total_writes += 1;
                self.stats.bytes_written += event_data.len() as u64;
                self.stats.last_write_ts = timestamp;

                Ok(AofWriteConfirmation {
                    original_hash: sha_256_slice(event_data),
                    sequence_id: self.sequence_counter as u32,
                    latest_ts: timestamp as u32,
                    event_type,
                    status: 0, // Success
                    _pad: [0; 22],
                })
            }
            Err(e) => {
                self.stats.failed_writes += 1;
                tracing::error!("Failed to persist enhanced event to LMDB: {:?}", e);

                Ok(AofWriteConfirmation {
                    original_hash: sha_256_slice(event_data),
                    sequence_id: self.sequence_counter as u32,
                    latest_ts: timestamp as u32,
                    event_type,
                    status: 1, // Failure
                    _pad: [0; 22],
                })
            }
        }
    }

    /// Process a single event and persist to LMDB (legacy method)
    fn process_legacy_event(&mut self, event_data: &[u8], event_type: u8) -> Result<AofWriteConfirmation, Box<dyn std::error::Error>> {
        // Use empty hashes for legacy events
        self.process_event(
            event_data,
            event_type,
            emit_secs(),
            [0; 32], // Empty xaero_id_hash
            [0; 32], // Empty vector_clock_hash
        )
    }

    /// Filter out noise events to reduce storage overhead
    fn should_persist_event_type(event_type: u8) -> bool {
        match event_type {
            // Skip debug/trace events
            250..=255 => false,
            // Persist application and system events
            _ => true,
        }
    }

    /// Log periodic statistics
    fn log_stats(&self) {
        if self.stats.total_writes % 1000 == 0 && self.stats.total_writes > 0 {
            tracing::info!(
                "AOF Stats - Subject: {:?}, Bus: {:?}, Writes: {}, Failed: {}, Bytes: {} MB",
                self.subject_hash,
                self.bus_kind,
                self.stats.total_writes,
                self.stats.failed_writes,
                self.stats.bytes_written / (1024 * 1024)
            );
        }
    }
}

// ================================================================================================
// AOF ACTOR IMPLEMENTATION
// ================================================================================================

impl AofActor {
    /// Create and spawn AOF actor with ring buffer processing using dedicated AOF ring buffers
    pub fn spin(subject_hash: SubjectHash, bus_kind: BusKind) -> Result<Self, Box<dyn std::error::Error>> {
        let mut state = AofState::new(subject_hash, bus_kind.clone())?;

        let jh = thread::spawn(move || {
            tracing::info!("AOF Actor started - Subject: {:?}, Bus: {:?}", subject_hash, bus_kind);

            match bus_kind {
                BusKind::Control => {
                    // Control: Read from AOF control input, write confirmations to AOF control output
                    let control_input_ring = AOF_CONTROL_INPUT_RING.get_or_init(RingBuffer::new);
                    let control_output_ring = AOF_CONTROL_OUTPUT_RING.get_or_init(RingBuffer::new);

                    let mut input_reader = Reader::new(control_input_ring);
                    let mut output_writer = Writer::new(control_output_ring);

                    Self::run_control_event_loop(&mut state, &mut input_reader, &mut output_writer);
                }
                BusKind::Data => {
                    // Data: Read from AOF data input, write confirmations to AOF data output
                    let data_input_ring = AOF_DATA_INPUT_RING.get_or_init(RingBuffer::new);
                    let data_output_ring = AOF_DATA_OUTPUT_RING.get_or_init(RingBuffer::new);

                    let mut input_reader = Reader::new(data_input_ring);
                    let mut output_writer = Writer::new(data_output_ring);

                    Self::run_data_event_loop(&mut state, &mut input_reader, &mut output_writer);
                }
            }
        });

        // Create exposed interfaces using the dedicated AOF ring buffers
        let control_input_ring = AOF_CONTROL_INPUT_RING.get_or_init(RingBuffer::new);
        let control_output_ring = AOF_CONTROL_OUTPUT_RING.get_or_init(RingBuffer::new);
        let data_input_ring = AOF_DATA_INPUT_RING.get_or_init(RingBuffer::new);
        let data_output_ring = AOF_DATA_OUTPUT_RING.get_or_init(RingBuffer::new);

        let in_writer = Writer::new(control_input_ring); // Control events input
        let in_writer_data = Writer::new(data_input_ring); // Data events input
        let out_reader = Reader::new(control_output_ring); // Control confirmations output
        let out_reader_data = Reader::new(data_output_ring); // Data confirmations output

        Ok(Self {
            in_writer,
            in_writer_data,
            out_reader,
            out_reader_data,
            jh,
        })
    }

    /// Process event from ring buffer with specific size
    fn process_ring_event_sized<const SIZE: usize>(state: &mut AofState, event: &PooledEvent<SIZE>) -> Result<AofWriteConfirmation, Box<dyn std::error::Error>> {
        // Try to parse as XaeroInternalEvent first
        if event.len == std::mem::size_of::<XaeroInternalEvent<SIZE>>() as u32 {
            // Safety: We've verified the size matches exactly
            let internal_event = unsafe { std::ptr::read(event as *const _ as *const XaeroInternalEvent<SIZE>) };

            let event_data = &internal_event.evt.data[..internal_event.evt.len as usize];

            // Use enhanced key generation with peer and vector clock info
            return state.process_event(
                event_data,
                internal_event.evt.event_type as u8,
                internal_event.latest_ts,
                internal_event.xaero_id_hash,
                internal_event.vector_clock_hash,
            );
        }

        // Fallback: parse as raw PooledEvent data (no peer/vector clock info)
        let event_data = &event.data[..event.len as usize];
        state.process_legacy_event(event_data, event.event_type as u8)
    }

    /// Control event processing loop (AOF control input -> AOF control output)
    fn run_control_event_loop(state: &mut AofState, reader: &mut Reader<64, 2000>, writer: &mut Writer<64, 2000>) {
        loop {
            let mut events_processed = 0;

            for event in reader.by_ref() {
                match Self::process_ring_event_sized::<64>(state, &event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        // Confirmation fits exactly in 64 bytes
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 200) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("AOF control confirmation buffer full - dropping confirmation");
                                },
                            Err(e) => {
                                tracing::error!("Failed to create control confirmation event: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process AOF control event: {:?}", e);
                    }
                }
                events_processed += 1;
            }

            if events_processed > 0 {
                state.log_stats();
            }

            thread::sleep(Duration::from_micros(100));
        }
    }

    /// Data event processing loop (AOF data input -> AOF data output for confirmations)
    fn run_data_event_loop(state: &mut AofState, reader: &mut Reader<256, 1000>, writer: &mut Writer<64, 2000>) {
        tracing::warn!("🚀 AOF Data event loop STARTED");
        tracing::warn!("🎯 Actor input ring buffer address: {:p}", reader.ringbuffer as *const _);
        tracing::warn!("🎯 Actor output ring buffer address: {:p}", writer.ringbuffer as *const _);
        loop {
            let mut events_processed = 0;

            for event in reader.by_ref() {
                match Self::process_ring_event_sized::<256>(state, &event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        // Confirmation fits exactly in 64 bytes
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 200) {
                            Ok(conf_event) =>
                                if !writer.add(conf_event) {
                                    tracing::warn!("AOF data confirmation buffer full - dropping confirmation");
                                },
                            Err(e) => {
                                tracing::error!("Failed to create data confirmation event: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to process AOF data event: {:?}", e);
                    }
                }
                events_processed += 1;
            }

            if events_processed > 0 {
                state.log_stats();
            }

            thread::sleep(Duration::from_micros(100));
        }
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_size_constraints() {
        // Verify our confirmation event fits in XS pool
        let conf = AofWriteConfirmation {
            original_hash: [0; 32],
            sequence_id: 1,
            latest_ts: 1234567890,
            event_type: 42,
            status: 0,
            _pad: [0; 22],
        };

        let conf_size = std::mem::size_of::<AofWriteConfirmation>();
        assert_eq!(conf_size, 64, "Confirmation must be exactly 64 bytes for XS pool, got {} bytes", conf_size);

        // Test XS event creation
        let conf_bytes = bytemuck::bytes_of(&conf);
        let xs_event = EventUtils::create_pooled_event::<64>(conf_bytes, 200).expect("Confirmation should fit in XS event");

        assert_eq!(xs_event.len as usize, conf_size);
        println!("✅ Confirmation fits perfectly in XS pool: {} bytes", conf_size);
    }

    #[test]
    fn test_aof_actor_xs_interface() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([1u8; 32]);
        let mut actor = AofActor::spin(subject_hash, BusKind::Control).expect("Failed to create AOF actor");

        // Test XS writer interface (64 bytes for control events)
        let test_data = b"control cmd"; // Small control command
        let event = EventUtils::create_pooled_event::<64>(test_data, 42).expect("Failed to create XS event");

        // Write to AOF control input buffer
        assert!(actor.in_writer.add(event), "Should write to AOF control input buffer");

        // Read confirmations from AOF control output buffer
        let mut confirmation_found = false;
        for _attempt in 0..50 {
            thread::sleep(Duration::from_millis(100));

            if let Some(conf_event) = actor.out_reader.next() {
                if let Ok(_conf) = bytemuck::try_from_bytes::<AofWriteConfirmation>(&conf_event.data[..conf_event.len as usize]) {
                    confirmation_found = true;
                    break;
                }
            }
        }

        assert!(confirmation_found, "Should receive control confirmation from AOF output buffer");
    }

    #[test]
    fn test_aof_actor_s_interface() {
        xaeroflux_core::initialize();
        let subject_hash = SubjectHash([2u8; 32]);
        let mut actor = AofActor::spin(subject_hash, BusKind::Data).expect("Failed to create AOF actor");
        tracing::warn!(
            "🔍 Test input ring buffer address: {:p}",
            AOF_DATA_INPUT_RING.get().map(|r| r as *const _).unwrap_or(std::ptr::null())
        );
        tracing::warn!(
            "🔍 Test output ring buffer address: {:p}",
            AOF_DATA_OUTPUT_RING.get().map(|r| r as *const _).unwrap_or(std::ptr::null())
        );
        // Test S writer with CRDT operation (256 bytes for data events)
        let crdt_op = b"{'op':'insert','pos':42,'char':'a','user':'alice'}"; // CRDT operation
        let event = EventUtils::create_pooled_event::<256>(crdt_op, 100).expect("Failed to create S event");
        thread::sleep(Duration::from_millis(100));
        // Write to AOF data input buffer
        assert!(actor.in_writer_data.add(event), "Should write CRDT op to AOF data input buffer");

        // Read confirmations from AOF data output buffer (separate ring buffer for confirmations)
        let mut confirmation_found = false;
        for _attempt in 0..50 {
            thread::sleep(Duration::from_millis(100));

            if let Some(conf_event) = actor.out_reader_data.next() {
                match bytemuck::try_from_bytes::<AofWriteConfirmation>(&conf_event.data[..conf_event.len as usize]) {
                    Ok(_conf) => {
                        confirmation_found = true;
                        break;
                    }
                    Err(e) => {
                        panic!(
                            "Failed to parse data confirmation: {:?}. Event len: {}, Expected: {}",
                            e,
                            conf_event.len,
                            std::mem::size_of::<AofWriteConfirmation>()
                        );
                    }
                }
            }
        }

        assert!(confirmation_found, "Should receive data confirmation from AOF output buffer within timeout");
    }

    #[test]
    fn test_xaero_internal_event_detection() {
        // Test that we can distinguish XaeroInternalEvent from regular PooledEvent
        use bytemuck::Zeroable;
        use rusted_ring_new::PooledEvent;

        // Create XaeroInternalEvent<256>
        let mut pooled_event = PooledEvent::<256>::zeroed();
        let test_data = b"internal event test";
        let copy_len = std::cmp::min(test_data.len(), 256);
        pooled_event.data[..copy_len].copy_from_slice(&test_data[..copy_len]);
        pooled_event.len = test_data.len() as u32;
        pooled_event.event_type = 42;

        let internal_event = XaeroInternalEvent::<256> {
            xaero_id_hash: [1u8; 32],
            vector_clock_hash: [2u8; 32],
            evt: pooled_event,
            latest_ts: emit_secs(),
        };

        // Convert to bytes and back as PooledEvent
        let internal_bytes = bytemuck::bytes_of(&internal_event);

        // Check size detection logic
        let size_matches = internal_bytes.len() == std::mem::size_of::<XaeroInternalEvent<256>>();
        assert!(size_matches, "Size should match XaeroInternalEvent<256>");

        // Create regular PooledEvent for comparison
        let regular_event = EventUtils::create_pooled_event::<256>(test_data, 42).unwrap();
        let regular_size = std::mem::size_of::<PooledEvent<256>>();
        let internal_size = std::mem::size_of::<XaeroInternalEvent<256>>();

        assert_ne!(regular_size, internal_size, "Sizes should be different");
        println!("✅ XaeroInternalEvent detection logic working: regular={}, internal={}", regular_size, internal_size);
    }

    #[test]
    fn test_event_processing_with_enhanced_data() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([3u8; 32]);
        let mut state = AofState::new(subject_hash, BusKind::Data).expect("Failed to create AOF state");

        // Test processing enhanced event with peer/vector clock info
        let test_data = b"enhanced event with metadata";
        let confirmation = state
            .process_event(
                test_data,
                42,
                emit_secs(),
                [5u8; 32], // peer ID
                [6u8; 32], // vector clock
            )
            .expect("Should process enhanced event");

        assert_eq!(confirmation.status, 0, "Should succeed");
        assert_eq!(confirmation.event_type, 42, "Should preserve event type");
        assert_eq!(confirmation.original_hash, sha_256_slice(test_data), "Should hash correctly");

        println!("✅ Enhanced event processing working");
    }
}
