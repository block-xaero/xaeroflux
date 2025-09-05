//! AOF (Append-Only File) Actor using ring buffer architecture with MMR indexing.
//!
//! This actor:
//! - Reads from global ring buffers (XS, S, M, L, XL) defined in subject.rs
//! - Persists events to LMDB in append-only fashion
//! - Updates MMR index for each event
//! - Uses simple sequential selection for event processing
//! - Uses hash index for fast leaf hash lookups

use std::{
    ops::Range,
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use parking_lot::{RawRwLock, RwLock, lock_api::RwLockReadGuard};
use rusted_ring::{
    EventPoolFactory, EventUtils, L_CAPACITY, L_TSHIRT_SIZE, M_CAPACITY, M_TSHIRT_SIZE, PooledEvent, Reader, RingBuffer, S_CAPACITY, S_TSHIRT_SIZE, Writer, XL_CAPACITY,
    XL_TSHIRT_SIZE, XS_CAPACITY, XS_TSHIRT_SIZE,
};
use xaeroflux_core::{CONF, date_time::emit_secs, hash::blake_hash_slice, pipe::BusKind, pool::XaeroInternalEvent, system_paths};

// Import global ring buffers from subject.rs
use crate::{L_RING, M_RING, S_RING, XL_RING, XS_RING};
use crate::{
    aof::storage::{
        format::{EventKey, MmrMeta},
        lmdb::{LmdbEnv, generate_event_key, get_event_by_hash, get_mmr_meta, push_internal_event_universal, put_mmr_meta, scan_enhanced_range},
    },
    indexing::mmr::{Peak, XaeroMmr, XaeroMmrOps},
    read_api::PointQuery,
};
// ================================================================================================
// TYPES & STRUCTS
// ================================================================================================

/// AOF Actor state - tracks sequence and MMR index
pub struct AofState {
    pub env: Arc<Mutex<LmdbEnv>>,
    pub sequence_counter: u64,
    pub mmr: RwLock<XaeroMmr>,
}

/// Reader multiplexer for all ring buffer sizes
pub struct ReaderMultiplexer {
    pub xs_reader: Reader<XS_TSHIRT_SIZE, XS_CAPACITY>,
    pub s_reader: Reader<S_TSHIRT_SIZE, S_CAPACITY>,
    pub m_reader: Reader<M_TSHIRT_SIZE, M_CAPACITY>,
    pub l_reader: Reader<L_TSHIRT_SIZE, L_CAPACITY>,
    pub xl_reader: Reader<XL_TSHIRT_SIZE, XL_CAPACITY>,
}

impl ReaderMultiplexer {
    pub fn new() -> Self {
        let xs_ring = XS_RING.get_or_init(RingBuffer::new);
        let s_ring = S_RING.get_or_init(RingBuffer::new);
        let m_ring = M_RING.get_or_init(RingBuffer::new);
        let l_ring = L_RING.get_or_init(RingBuffer::new);
        let xl_ring = XL_RING.get_or_init(RingBuffer::new);

        Self {
            xs_reader: Reader::new(xs_ring),
            s_reader: Reader::new(s_ring),
            m_reader: Reader::new(m_ring),
            l_reader: Reader::new(l_ring),
            xl_reader: Reader::new(xl_ring),
        }
    }

    /// Process events using simple sequential selection (XS -> S -> M -> L -> XL)
    pub fn process_events(&mut self, state: &mut AofState) -> bool {
        // Check XS first (highest priority)
        if let Some(event) = self.xs_reader.next() {
            if AofActor::process_ring_event_sized::<XS_TSHIRT_SIZE>(state, &event).is_ok() {
                tracing::debug!("Processed XS event");
            }
            return true;
        }

        // Check S
        if let Some(event) = self.s_reader.next() {
            if AofActor::process_ring_event_sized::<S_TSHIRT_SIZE>(state, &event).is_ok() {
                tracing::debug!("Processed S event");
            }
            return true;
        }

        // Check M
        if let Some(event) = self.m_reader.next() {
            if AofActor::process_ring_event_sized::<M_TSHIRT_SIZE>(state, &event).is_ok() {
                tracing::debug!("Processed M event");
            }
            return true;
        }

        // Check L
        if let Some(event) = self.l_reader.next() {
            if AofActor::process_ring_event_sized::<L_TSHIRT_SIZE>(state, &event).is_ok() {
                tracing::debug!("Processed L event");
            }
            return true;
        }

        // Check XL
        if let Some(event) = self.xl_reader.next() {
            if AofActor::process_ring_event_sized::<XL_TSHIRT_SIZE>(state, &event).is_ok() {
                tracing::debug!("Processed XL event");
            }
            return true;
        }

        false // No events found
    }
}

/// AOF Actor - processes events from global ring buffers and persists to LMDB with MMR indexing
pub struct AofActor {
    pub jh: JoinHandle<()>,
    pub env: Arc<Mutex<LmdbEnv>>,
}

// ================================================================================================
// AOF STATE IMPLEMENTATION
// ================================================================================================

impl AofState {
    /// Create new AOF state with LMDB environment and recovered MMR
    pub fn new(env: Arc<Mutex<LmdbEnv>>) -> Result<Self, Box<dyn std::error::Error>> {
        // Get base path from config
        let c = CONF.get().expect("failed to unravel config");
        let base_path = &c.aof.file_path;

        let mmr = Self::recover_mmr_from_events(&env)?;
        tracing::info!("Recovered MMR with {} leaves", mmr.leaf_count());
        let mmr_rw = RwLock::new(mmr);
        Ok(Self {
            env,
            sequence_counter: 0,
            mmr: mmr_rw,
        })
    }

    /// Recover MMR state by scanning all existing events in chronological order
    fn recover_mmr_from_events(env: &Arc<Mutex<LmdbEnv>>) -> Result<XaeroMmr, Box<dyn std::error::Error>> {
        let mut mmr = XaeroMmr::new();

        // Check if we have stored MMR metadata first
        if let Ok(Some(mmr_meta)) = get_mmr_meta(env) {
            let mpc = mmr_meta.leaf_count;
            tracing::info!("Found existing MMR metadata: {mpc:?} leaves");
        }

        // Scan all events in chronological order and rebuild MMR
        let mut all_events = Vec::new();

        // Collect events from all possible sizes
        if let Ok(xs_events) = unsafe { scan_enhanced_range::<XS_TSHIRT_SIZE>(env, 0, u64::MAX) } {
            for event in xs_events {
                let event_data = &event.evt.data[..event.evt.len as usize];
                let event_hash = blake_hash_slice(event_data);
                all_events.push((event.latest_ts, event_hash));
            }
        }

        if let Ok(s_events) = unsafe { scan_enhanced_range::<S_TSHIRT_SIZE>(env, 0, u64::MAX) } {
            for event in s_events {
                let event_data = &event.evt.data[..event.evt.len as usize];
                let event_hash = blake_hash_slice(event_data);
                all_events.push((event.latest_ts, event_hash));
            }
        }

        if let Ok(m_events) = unsafe { scan_enhanced_range::<M_TSHIRT_SIZE>(env, 0, u64::MAX) } {
            for event in m_events {
                let event_data = &event.evt.data[..event.evt.len as usize];
                let event_hash = blake_hash_slice(event_data);
                all_events.push((event.latest_ts, event_hash));
            }
        }

        if let Ok(l_events) = unsafe { scan_enhanced_range::<L_TSHIRT_SIZE>(env, 0, u64::MAX) } {
            for event in l_events {
                let event_data = &event.evt.data[..event.evt.len as usize];
                let event_hash = blake_hash_slice(event_data);
                all_events.push((event.latest_ts, event_hash));
            }
        }

        if let Ok(xl_events) = unsafe { scan_enhanced_range::<XL_TSHIRT_SIZE>(env, 0, u64::MAX) } {
            for event in xl_events {
                let event_data = &event.evt.data[..event.evt.len as usize];
                let event_hash = blake_hash_slice(event_data);
                all_events.push((event.latest_ts, event_hash));
            }
        }

        // Sort events by timestamp to rebuild MMR in correct order
        all_events.sort_by_key(|(ts, _)| *ts);

        // Rebuild MMR by appending events in chronological order
        for (_, event_hash) in all_events {
            mmr.append(event_hash);
        }

        tracing::info!("Recovered MMR with {} leaves", mmr.leaf_count());
        Ok(mmr)
    }

    /// Process a single enhanced event with peer and vector clock info, including MMR update
    fn process_event(
        &mut self,
        event_data: &[u8],
        event_type: u32,
        timestamp: u64,
        xaero_id_hash: [u8; 32],
        vector_clock_hash: [u8; 32],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Store event in LMDB (this also updates hash index automatically)
        match push_internal_event_universal(&self.env, event_data, event_type, timestamp) {
            Ok(_) => {
                // 2. Add event hash to MMR index
                let event_hash = blake_hash_slice(event_data);
                let _changed_peaks = self.mmr.write().append(event_hash);

                // 3. Persist MMR metadata to LMDB
                if let Err(e) = self.persist_mmr_metadata() {
                    tracing::warn!("Failed to persist MMR metadata: {:?}", e);
                }

                self.sequence_counter += 1;

                tracing::debug!("Event processed: hash={}, MMR leaves={}", hex::encode(event_hash), self.mmr.read().leaf_count());

                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to persist event to LMDB: {:?}", e);
                Err(e)
            }
        }
    }

    /// Process a single event and persist to LMDB (legacy method)
    fn process_legacy_event(&mut self, event_data: &[u8], event_type: u8) -> Result<(), Box<dyn std::error::Error>> {
        // Use empty hashes for legacy events
        self.process_event(
            event_data,
            event_type as u32,
            emit_secs(),
            [0; 32], // Empty xaero_id_hash
            [0; 32], // Empty vector_clock_hash
        )
    }

    /// Persist MMR metadata to LMDB META database
    fn persist_mmr_metadata(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mmr = self.mmr.read();
        let mmr_meta = MmrMeta {
            root_hash: mmr.root(),
            peaks_count: mmr.peaks().len(),
            leaf_count: mmr.leaf_count(),
        };

        put_mmr_meta(&self.env, &mmr_meta)?;
        Ok(())
    }

    /// Get events for specific leaf hashes (for peer sync) - NOW USING HASH INDEX!
    pub fn get_events_for_leaf_hashes(&self, leaf_hashes: &[[u8; 32]]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut events = Vec::new();

        for &target_hash in leaf_hashes {
            let mut found = false;

            // Try O(1) hash index lookup for each possible size
            // This replaces the old O(N) scan approach!

            // Try XS size first
            if !found && let Ok(Some(event)) = get_event_by_hash::<XS_TSHIRT_SIZE>(&self.env, target_hash) {
                let event_data = &event.evt.data[..event.evt.len as usize];
                events.push(event_data.to_vec());
                found = true;
            }

            // Try S size
            if !found && let Ok(Some(event)) = get_event_by_hash::<S_TSHIRT_SIZE>(&self.env, target_hash) {
                let event_data = &event.evt.data[..event.evt.len as usize];
                events.push(event_data.to_vec());
                found = true;
            }

            // Try M size
            if !found && let Ok(Some(event)) = get_event_by_hash::<M_TSHIRT_SIZE>(&self.env, target_hash) {
                let event_data = &event.evt.data[..event.evt.len as usize];
                events.push(event_data.to_vec());
                found = true;
            }

            // Try L size
            if !found && let Ok(Some(event)) = get_event_by_hash::<L_TSHIRT_SIZE>(&self.env, target_hash) {
                let event_data = &event.evt.data[..event.evt.len as usize];
                events.push(event_data.to_vec());
                found = true;
            }

            // Try XL size
            if !found && let Ok(Some(event)) = get_event_by_hash::<XL_TSHIRT_SIZE>(&self.env, target_hash) {
                let event_data = &event.evt.data[..event.evt.len as usize];
                events.push(event_data.to_vec());
                found = true;
            }

            if !found {
                tracing::warn!("Event not found for leaf hash: {}", hex::encode(target_hash));
            }
        }

        tracing::debug!("Retrieved {} events for {} leaf hashes using hash index", events.len(), leaf_hashes.len());
        Ok(events)
    }

    /// Get events sized by point
    pub fn get_event_sized_by_point<const SIZE: usize>(point_query: PointQuery<SIZE>) -> Result<Option<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>> {
        Ok(None)
    }

    pub fn get_env(&self) -> &Arc<Mutex<LmdbEnv>> {
        &self.env
    }

    /// Get current MMR peaks for sharing with peers
    pub fn get_mmr_peaks(&self) -> Vec<Peak> {
        self.mmr.read().peaks().to_vec()
    }

    pub fn get_mmr_leaf_count(&self) -> usize {
        self.mmr.read().leaf_count
    }

    /// Get MMR root hash
    pub fn get_mmr_root(&self) -> [u8; 32] {
        self.mmr.read().root()
    }

    /// Generate MMR proof for a specific leaf index
    pub fn get_mmr_proof(&self, leaf_index: usize) -> Option<xaeroflux_core::merkle_tree::XaeroMerkleProof> {
        self.mmr.read().proof(leaf_index)
    }

    /// Verify an MMR proof
    pub fn verify_mmr_proof(&self, leaf_hash: [u8; 32], proof: &xaeroflux_core::merkle_tree::XaeroMerkleProof, expected_root: [u8; 32]) -> bool {
        self.mmr.read().verify(leaf_hash, proof, expected_root)
    }

    pub fn get_leaf_hashes_from_range(&self, leaf_indices_range: Range<usize>) -> Vec<[u8; 32]> {
        self.mmr.read().leaf_hashes[leaf_indices_range].to_vec()
    }
}

// ================================================================================================
// AOF ACTOR IMPLEMENTATION
// ================================================================================================

impl AofActor {
    /// Create and spawn AOF actor that reads from global ring buffers
    pub fn spin(lmdb_env: Arc<Mutex<LmdbEnv>>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut state = AofState::new(lmdb_env)?;
        let read_only_clone = state.env.clone();
        let jh = thread::spawn(move || {
            tracing::info!("AOF Actor started ");
            tracing::info!("Reading from global ring buffers: XS, S, M, L, XL");

            // Create reader multiplexer for all global ring buffers
            let mut multiplexer = ReaderMultiplexer::new();

            // Main event processing loop
            loop {
                // Process events with priority (XS first, then S, M, L, XL)
                if !multiplexer.process_events(&mut state) {
                    // No events available, yield CPU briefly
                    thread::sleep(Duration::from_micros(100));
                }
            }
        });

        Ok(Self { jh, env: read_only_clone })
    }

    /// Process event from ring buffer with specific size
    pub fn process_ring_event_sized<const SIZE: usize>(state: &mut AofState, event: &PooledEvent<SIZE>) -> Result<(), Box<dyn std::error::Error>> {
        // Try to parse as XaeroInternalEvent first
        if event.len == std::mem::size_of::<XaeroInternalEvent<SIZE>>() as u32 {
            // Safety: We've verified the size matches exactly
            let internal_event = unsafe { std::ptr::read(event as *const _ as *const XaeroInternalEvent<SIZE>) };

            let event_data = &internal_event.evt.data[..internal_event.evt.len as usize];

            // Use enhanced key generation with peer and vector clock info
            return state.process_event(
                event_data,
                internal_event.evt.event_type,
                internal_event.latest_ts,
                internal_event.xaero_id_hash,
                internal_event.vector_clock_hash,
            );
        }

        // Fallback: parse as raw PooledEvent data (no peer/vector clock info)
        let event_data = &event.data[..event.len as usize];
        state.process_legacy_event(event_data, event.event_type as u8)
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reader_multiplexer() {
        // Test that multiplexer can be created with global rings
        let multiplexer = ReaderMultiplexer::new();

        // Verify all readers are initialized
        assert!(multiplexer.xs_reader.cursor == 0);
        assert!(multiplexer.s_reader.cursor == 0);
        assert!(multiplexer.m_reader.cursor == 0);
        assert!(multiplexer.l_reader.cursor == 0);
        assert!(multiplexer.xl_reader.cursor == 0);

        println!("✅ Reader multiplexer created successfully");
    }

    #[test]
    fn test_mmr_integration() {
        xaeroflux_core::initialize();

        let mut state = AofState::new(Arc::new(Mutex::new(LmdbEnv::new("/tmp/xaero-test-data")
            .unwrap
        ()))).expect
        ("Failed to create \
        AOF \
        state");

        // Test MMR integration with event processing
        let test_events = [b"first event".as_slice(), b"second event".as_slice(), b"third event".as_slice()];
        let initial_leaf_count = state.mmr.read().leaf_count();
        for (i, event_data) in test_events.iter().enumerate() {
            state
                .process_event(
                    event_data,
                    xaeroflux_core::event::make_pinned((5008 + i) as u32),
                    emit_secs() + i as u64,
                    [i as u8; 32],        // Different peer IDs
                    [(i + 10) as u8; 32], // Different vector clocks
                )
                .expect("Should process event with MMR");
        }

        assert_eq!(state.mmr.read().leaf_count(), initial_leaf_count + test_events.len(), "MMR should have correct leaf count");
        assert_ne!(state.mmr.read().root(), [0; 32], "MMR root should be non-zero");

        println!("✅ MMR integration working: {} leaves", state.mmr.read().leaf_count());
    }

    #[ignore]
    #[test]
    fn test_hash_index_lookup_performance() {
        xaeroflux_core::initialize();

        let mut state = AofState::new(Arc::new(Mutex::new(LmdbEnv::new("/tmp/xaero-test-data")
            .unwrap
            ()))).expect
        ("Failed to create \
        AOF \
        state");

        // Store some test events
        let test_events = [b"hash_index_test_1".as_slice(), b"hash_index_test_2".as_slice(), b"hash_index_test_3".as_slice()];

        let mut event_hashes = Vec::new();

        for (i, event_data) in test_events.iter().enumerate() {
            state
                .process_event(event_data, 100 + i as u32, emit_secs() + i as u64, [i as u8; 32], [(i + 20) as u8; 32])
                .expect("Should process event");

            let event_hash = blake_hash_slice(event_data);
            event_hashes.push(event_hash);
        }

        // Test hash index lookup
        let retrieved_events = state.get_events_for_leaf_hashes(&event_hashes).expect("Hash lookup should work");

        assert_eq!(retrieved_events.len(), test_events.len(), "Should retrieve all events");

        for (original, retrieved) in test_events.iter().zip(retrieved_events.iter()) {
            assert_eq!(*original, retrieved.as_slice(), "Event data should match");
        }

        println!("✅ Hash index lookup performance test passed");
    }
}
