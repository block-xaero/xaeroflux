//! AOF (Append-Only File) Actor using ring buffer architecture.
//!
//! This actor:
//! - Reads from input ring buffer internally using existing pools
//! - Persists events to LMDB in append-only fashion
//! - Writes confirmation events to output ring buffer
//! - Exposes reader/writer interfaces for external interaction

use std::{
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use rusted_ring_new::{EventPoolFactory, EventUtils, PooledEvent, Reader, Writer};
use xaeroflux_core::{
    date_time::emit_secs, hash::sha_256_slice, pipe::BusKind, pool::XaeroInternalEvent, system_paths, CONF,
};

use crate::{
    aof::storage::lmdb::{push_internal_event_universal, LmdbEnv},
    subject::SubjectHash,
};

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

/// AOF Actor state - tracks write statistics and sequence
pub struct AofState {
    pub env: Arc<Mutex<LmdbEnv>>,
    pub subject_hash: SubjectHash,
    pub bus_kind: BusKind,
    pub sequence_counter: u64,
    pub stats: AofStats,
}

#[derive(Debug, Default)]
pub struct AofStats {
    pub total_writes: u64,
    pub failed_writes: u64,
    pub bytes_written: u64,
    pub last_write_ts: u64,
}

impl AofState {
    pub fn new(subject_hash: SubjectHash, bus_kind: BusKind) -> Result<Self, Box<dyn std::error::Error>> {
        // Get base path from config
        let c = CONF.get().expect("failed to unravel config");
        let base_path = &c.aof.file_path;

        // Construct subject-specific path based on bus kind
        let lmdb_path = match bus_kind {
            BusKind::Control => system_paths::emit_control_path_with_subject_hash(
                base_path.to_str().expect("path_invalid_for_aof"),
                subject_hash.0,
                "aof",
            ),
            BusKind::Data => system_paths::emit_data_path_with_subject_hash(
                base_path.to_str().expect("path_invalid_for_aof"),
                subject_hash.0,
                "aof",
            ),
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

    /// Process a single event and persist to LMDB
    fn process_event(
        &mut self,
        event_data: &[u8],
        event_type: u8,
    ) -> Result<AofWriteConfirmation, Box<dyn std::error::Error>> {
        let current_ts = emit_secs();

        // Skip noise/debug events
        if !Self::should_persist_event_type(event_type) {
            return Ok(AofWriteConfirmation {
                original_hash: sha_256_slice(event_data),
                sequence_id: self.sequence_counter as u32,
                latest_ts: current_ts as u32,
                event_type,
                status: 0, // Success (but skipped)
                _pad: [0; 22],
            });
        }

        // Persist to LMDB
        match push_internal_event_universal(&self.env, event_data, event_type as u32, current_ts) {
            Ok(_) => {
                self.sequence_counter += 1;
                self.stats.total_writes += 1;
                self.stats.bytes_written += event_data.len() as u64;
                self.stats.last_write_ts = current_ts;

                Ok(AofWriteConfirmation {
                    original_hash: sha_256_slice(event_data),
                    sequence_id: self.sequence_counter as u32,
                    latest_ts: current_ts as u32,
                    event_type,
                    status: 0, // Success
                    _pad: [0; 22],
                })
            }
            Err(e) => {
                self.stats.failed_writes += 1;
                tracing::error!("Failed to persist event to LMDB: {:?}", e);

                Ok(AofWriteConfirmation {
                    original_hash: sha_256_slice(event_data),
                    sequence_id: self.sequence_counter as u32,
                    latest_ts: current_ts as u32,
                    event_type,
                    status: 1, // Failure
                    _pad: [0; 22],
                })
            }
        }
    }

    fn should_persist_event_type(event_type: u8) -> bool {
        match event_type {
            // Skip debug/trace events
            250..=255 => false,
            // Persist application and system events
            _ => true,
        }
    }

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

/// AOF Actor - processes events from ring buffer and persists to LMDB
pub struct AofActor {
    // Exposed interfaces for external interaction using existing pools
    pub in_writer: Writer<64, 2000>,       // XS writer for Control events
    pub in_writer_data: Writer<256, 1000>, // S writer for Data events (CRDT Ops)
    pub out_reader: Reader<64, 2000>,      // XS reader for Control confirmations
    pub out_reader_data: Reader<256, 1000>, // S reader for Data confirmations
    pub jh: JoinHandle<()>,
}

impl AofActor {
    /// Spawn AOF actor with ring buffer processing using existing pools
    pub fn spin(subject_hash: SubjectHash, bus_kind: BusKind) -> Result<Self, Box<dyn std::error::Error>> {
        let mut state = AofState::new(subject_hash, bus_kind.clone())?;

        let jh = thread::spawn(move || {
            tracing::info!("AOF Actor started - Subject: {:?}, Bus: {:?}", subject_hash, bus_kind);

            match bus_kind {
                BusKind::Control => {
                    // Use XS pool for control events (64 bytes, 2000 capacity = 128KB)
                    let mut reader = EventPoolFactory::get_xs_reader();
                    let mut writer = EventPoolFactory::get_xs_writer();
                    Self::run_control_event_loop(&mut state, &mut reader, &mut writer);
                }
                BusKind::Data => {
                    // Use S pool for data events/CRDT ops (256 bytes, 1000 capacity = 256KB)
                    let mut reader = EventPoolFactory::get_s_reader();
                    let mut writer = EventPoolFactory::get_s_writer();
                    Self::run_data_event_loop(&mut state, &mut reader, &mut writer);
                }
            }
        });

        // Create exposed reader/writer interfaces using the same pools
        let in_writer = EventPoolFactory::get_xs_writer();      // Control events input
        let in_writer_data = EventPoolFactory::get_s_writer();  // Data events input
        let out_reader = EventPoolFactory::get_xs_reader();     // Control confirmations output
        let out_reader_data = EventPoolFactory::get_s_reader(); // Data confirmations output

        Ok(Self {
            in_writer,
            in_writer_data,
            out_reader,
            out_reader_data,
            jh,
        })
    }

    /// Control event processing loop (XS pool - 64 bytes)
    fn run_control_event_loop(
        state: &mut AofState,
        reader: &mut Reader<64, 2000>,
        writer: &mut Writer<64, 2000>,
    ) {
        loop {
            let mut events_processed = 0;

            for event in reader.by_ref() {
                match Self::process_ring_event_sized::<64>(state, &event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        // Confirmation fits in 64 bytes (48 bytes + padding)
                        match EventUtils::create_pooled_event::<64>(conf_bytes, 200) {
                            Ok(conf_event) => {
                                if !writer.add(conf_event) {
                                    tracing::warn!("AOF XS confirmation buffer full - dropping confirmation");
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to create XS confirmation event: {:?}", e);
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

    /// Data event processing loop (S pool - 256 bytes)
    fn run_data_event_loop(
        state: &mut AofState,
        reader: &mut Reader<256, 1000>,
        writer: &mut Writer<256, 1000>,
    ) {
        loop {
            let mut events_processed = 0;

            for event in reader.by_ref() {
                match Self::process_ring_event_sized::<256>(state, &event) {
                    Ok(confirmation) => {
                        let conf_bytes = bytemuck::bytes_of(&confirmation);
                        // Confirmation fits in 256 bytes easily
                        match EventUtils::create_pooled_event::<256>(conf_bytes, 200) {
                            Ok(conf_event) => {
                                if !writer.add(conf_event) {
                                    tracing::warn!("AOF S confirmation buffer full - dropping confirmation");
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to create S confirmation event: {:?}", e);
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

    /// Process event from ring buffer with specific size
    fn process_ring_event_sized<const SIZE: usize>(
        state: &mut AofState,
        event: &PooledEvent<SIZE>,
    ) -> Result<AofWriteConfirmation, Box<dyn std::error::Error>> {
        // Try to parse as XaeroInternalEvent
        if event.len == std::mem::size_of::<XaeroInternalEvent<SIZE>>() as u32 {
            let internal_event = unsafe {
                std::ptr::read(event as *const _ as *const XaeroInternalEvent<SIZE>)
            };

            let event_data = &internal_event.evt.data[..internal_event.evt.len as usize];
            return state.process_event(event_data, internal_event.evt.event_type as u8);
        }

        // Fallback: parse as raw data
        let event_data = &event.data[..event.len as usize];
        state.process_event(event_data, event.event_type as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aof_actor_xs_interface() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([1u8; 32]);
        let mut actor = AofActor::spin(subject_hash, BusKind::Control)
            .expect("Failed to create AOF actor");

        // Test XS writer interface (64 bytes for control events)
        let test_data = b"control cmd"; // Small control command
        let event = EventUtils::create_pooled_event::<64>(test_data, 42)
            .expect("Failed to create XS event");

        // Write directly to XS input buffer
        assert!(actor.in_writer.add(event), "Should write to XS input buffer");

        // Allow processing time
        thread::sleep(Duration::from_millis(100));

        // Read confirmations from XS output buffer
        let mut confirmations_found = 0;
        while let Some(conf_event) = actor.out_reader.next() {
            if let Ok(_conf) = bytemuck::try_from_bytes::<AofWriteConfirmation>(
                &conf_event.data[..conf_event.len as usize]
            ) {
                confirmations_found += 1;
                break;
            }
        }

        assert!(confirmations_found > 0, "Should receive XS confirmation");
    }

    #[test]
    fn test_aof_actor_s_interface() {
        xaeroflux_core::initialize();

        let subject_hash = SubjectHash([2u8; 32]);
        let mut actor = AofActor::spin(subject_hash, BusKind::Data)
            .expect("Failed to create AOF actor");

        // Test S writer with CRDT operation (256 bytes for data events)
        let crdt_op = b"{'op':'insert','pos':42,'char':'a','user':'alice'}"; // CRDT operation
        let event = EventUtils::create_pooled_event::<256>(crdt_op, 100)
            .expect("Failed to create S event");

        // Write to S input buffer
        assert!(actor.in_writer_data.add(event), "Should write CRDT op to S buffer");

        thread::sleep(Duration::from_millis(150));

        // Check S output buffer
        if let Some(conf_event) = actor.out_reader_data.next() {
            let _conf = bytemuck::try_from_bytes::<AofWriteConfirmation>(
                &conf_event.data[..conf_event.len as usize]
            ).expect("Should parse S confirmation");
        }
    }

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
        let xs_event = EventUtils::create_pooled_event::<64>(conf_bytes, 200)
            .expect("Confirmation should fit in XS event");

        assert_eq!(xs_event.len as usize, conf_size);
        println!("✅ Confirmation fits perfectly in XS pool: {} bytes", conf_size);
    }
}