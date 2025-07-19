use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering, fence},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use bytemuck::{Pod, Zeroable, bytes_of};
use hnsw_rs::prelude::*;
use rusted_ring::{
    EventUtils, L_CAPACITY, L_TSHIRT_SIZE, M_CAPACITY, M_TSHIRT_SIZE, PooledEvent, Reader, RingBuffer, S_CAPACITY, S_TSHIRT_SIZE, XL_CAPACITY, XL_TSHIRT_SIZE, XS_CAPACITY,
    XS_TSHIRT_SIZE,
};
use xaeroflux_core::{
    event::ScanWindow,
    pipe::{BusKind, GenPipe, SignalPipe, Sink, Source},
    pool::XaeroInternalEvent,
};

// Import global ring buffers from subject.rs
use crate::{L_RING, M_RING, S_RING, XL_RING, XS_RING};
use crate::{
    aof::storage::{
        format::{EventKey, SegmentMeta},
        lmdb::generate_event_key,
    },
    indexing::lmdb::LmdbVectorSearchDb,
};
// ================================================================================================
// VECTOR EXTRACTION & REGISTRY
// ================================================================================================

pub trait VectorExtractor: Send + Sync {
    fn extract_vector(&self, event_data: &[u8]) -> Option<Vec<f32>>;
}

pub struct VectorRegistry {
    pub hnsw: Hnsw<'static, f32, DistCosine>,
    pub node_to_event: HashMap<usize, EventKey>,
    pub event_to_node: HashMap<EventKey, usize>,
    pub vector_dimension: usize,
    pub extractors: HashMap<u32, Box<dyn VectorExtractor>>,
    pub next_node_id: usize,
}

impl VectorRegistry {
    pub fn new(
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
        extractors: HashMap<u32, Box<dyn VectorExtractor>>,
        vector_dimension: usize,
    ) -> Self {
        Self {
            hnsw: Hnsw::new(max_nb_connection, max_elements, max_layer, ef_construction, DistCosine),
            node_to_event: HashMap::new(),
            event_to_node: HashMap::new(),
            vector_dimension,
            extractors,
            next_node_id: 0,
        }
    }
}

// ================================================================================================
// QUERY TYPES (POD for zero-copy)
// ================================================================================================

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct VectorQueryRequest<const VECTOR_SIZE: usize> {
    pub query_id: u64,
    pub requester_id: [u8; 32],
    pub scope: QueryScope,
    pub vector: [f32; VECTOR_SIZE],
    pub k: u32,
    pub similarity_threshold: f32,
    pub time_window: ScanWindow,
    pub flags: QueryFlags,
}
unsafe impl<const VECTOR_SIZE: usize> Pod for VectorQueryRequest<VECTOR_SIZE> {}
unsafe impl<const VECTOR_SIZE: usize> Zeroable for VectorQueryRequest<VECTOR_SIZE> {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct QueryScope {
    pub group_id: Option<[u8; 32]>, // Changed to byte arrays for consistency
    pub workspace_id: Option<[u8; 32]>,
    pub object_id: Option<[u8; 32]>,
}
unsafe impl Pod for QueryScope {}
unsafe impl Zeroable for QueryScope {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct QueryFlags {
    pub include_metadata: bool,
    pub include_operations: bool,
    pub fan_out: bool,
    pub use_lora_bias: bool,
}
unsafe impl Pod for QueryFlags {}
unsafe impl Zeroable for QueryFlags {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct SearchResult {
    pub similarity: f32,
    pub event_key: EventKey,
    pub scope: QueryScope,
    pub has_data: bool,
}
unsafe impl Pod for SearchResult {}
unsafe impl Zeroable for SearchResult {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct VectorQueryResponse<const PAGE_SIZE: usize> {
    pub query_id: u64,
    pub result_count: u32,
    pub status: u8, // 0=processing, 1=complete, 2=error
    pub results: [SearchResult; PAGE_SIZE],
    pub continuation_token: Option<SegmentMeta>,
}
unsafe impl<const PAGE_SIZE: usize> Pod for VectorQueryResponse<PAGE_SIZE> {}
unsafe impl<const PAGE_SIZE: usize> Zeroable for VectorQueryResponse<PAGE_SIZE> {}

pub struct VectorReaderMultiplexer {
    pub xs_reader: Reader<XS_TSHIRT_SIZE, XS_CAPACITY>,
    pub s_reader: Reader<S_TSHIRT_SIZE, S_CAPACITY>,
    pub m_reader: Reader<M_TSHIRT_SIZE, M_CAPACITY>,
    pub l_reader: Reader<L_TSHIRT_SIZE, L_CAPACITY>,
    pub xl_reader: Reader<XL_TSHIRT_SIZE, XL_CAPACITY>,
}

impl Default for VectorReaderMultiplexer {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorReaderMultiplexer {
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

    /// Process events from ring buffers and extract vectors for indexing
    pub fn process_events(&mut self, registry: &mut VectorRegistry) -> bool {
        // Check XS first (highest priority)
        if let Some(event) = self.xs_reader.next() {
            if VectorSearchActor::process_ring_event_sized::<XS_TSHIRT_SIZE>(registry, &event).is_ok() {
                tracing::debug!("Indexed XS event vector");
            }
            return true;
        }

        // Check S
        if let Some(event) = self.s_reader.next() {
            if VectorSearchActor::process_ring_event_sized::<S_TSHIRT_SIZE>(registry, &event).is_ok() {
                tracing::debug!("Indexed S event vector");
            }
            return true;
        }

        // Check M
        if let Some(event) = self.m_reader.next() {
            if VectorSearchActor::process_ring_event_sized::<M_TSHIRT_SIZE>(registry, &event).is_ok() {
                tracing::debug!("Indexed M event vector");
            }
            return true;
        }

        // Check L
        if let Some(event) = self.l_reader.next() {
            if VectorSearchActor::process_ring_event_sized::<L_TSHIRT_SIZE>(registry, &event).is_ok() {
                tracing::debug!("Indexed L event vector");
            }
            return true;
        }

        // Check XL
        if let Some(event) = self.xl_reader.next() {
            if VectorSearchActor::process_ring_event_sized::<XL_TSHIRT_SIZE>(registry, &event).is_ok() {
                tracing::debug!("Indexed XL event vector");
            }
            return true;
        }

        false // No events found
    }
}

// ================================================================================================
// VECTOR SEARCH ACTOR STATE
// ================================================================================================

pub struct VectorSearchState {
    pub registry: VectorRegistry,
    pub lmdb_db: Arc<crate::indexing::lmdb::LmdbVectorSearchDb>,
}

impl VectorSearchState {
    pub fn new(
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
        extractors: HashMap<u32, Box<dyn VectorExtractor>>,
        vector_dimension: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let registry = VectorRegistry::new(max_nb_connection, max_elements, max_layer, ef_construction, extractors, vector_dimension);

        let lmdb_db = Arc::new(LmdbVectorSearchDb::new());

        Ok(Self { registry, lmdb_db })
    }

    /// Perform vector search query
    pub fn search(&self, query: &VectorQueryRequest<256>) -> VectorQueryResponse<5> {
        let mut response: VectorQueryResponse<5> = VectorQueryResponse::zeroed();
        response.query_id = query.query_id;
        response.status = 0; // processing

        // Memory fence - ensure we see all committed writes from indexer
        fence(Ordering::Acquire);

        // Perform HNSW search
        let hnsw_results = self.registry.hnsw.search(&query.vector, query.k as usize, 16);
        let mut valid_results = Vec::new();
        let mut event_keys_to_fetch = Vec::new();

        // Filter results by similarity threshold and collect EventKeys
        for neighbour in hnsw_results.iter() {
            if neighbour.distance >= query.similarity_threshold {
                if let Some(&event_key) = self.registry.node_to_event.get(&neighbour.d_id) {
                    valid_results.push((event_key, neighbour.distance));
                    event_keys_to_fetch.push(event_key);
                }
            }
        }

        // Batch fetch snapshot data from LMDB
        let snapshot_data = self.bulk_search_lmdb(event_keys_to_fetch);

        // Populate response
        let result_count = std::cmp::min(valid_results.len(), 5); // Max 5 results per response

        for (i, (event_key, similarity)) in valid_results.into_iter().take(result_count).enumerate() {
            response.results[i] = SearchResult {
                similarity,
                event_key,
                scope: query.scope,
                has_data: snapshot_data.contains_key(&event_key),
            };
        }

        response.result_count = result_count as u32;
        response.status = 1; // complete

        response
    }

    /// Bulk search LMDB for event data
    fn bulk_search_lmdb(&self, event_keys: Vec<EventKey>) -> HashMap<EventKey, Vec<u8>> {
        // Use the improved bulk search from LmdbVectorSearchDb

        // Call the bulk_search method from your LmdbVectorSearchDb
        // Note: You'll need to make the method compatible with returning Vec<u8> instead of references

        HashMap::new()
    }
}

// ================================================================================================
// VECTOR SEARCH ACTOR
// ================================================================================================

pub struct VectorSearchActor {
    pub indexer_handle: JoinHandle<()>,
    pub query_handler: Arc<Mutex<VectorSearchState>>,
}

impl VectorSearchActor {
    /// Create and spawn vector search actor following AOF pattern
    pub fn spin(
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
        extractors: HashMap<u32, Box<dyn VectorExtractor>>,
        vector_dimension: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let state = VectorSearchState::new(max_nb_connection, max_elements, max_layer, ef_construction, extractors, vector_dimension)?;

        let query_handler = Arc::new(Mutex::new(state));
        let indexer_state = Arc::clone(&query_handler);

        let indexer_handle = thread::spawn(move || {
            tracing::info!("Vector Search Actor started!");
            tracing::info!("Reading from global ring buffers: XS, S, M, L, XL");

            // Create reader multiplexer for all global ring buffers
            let mut multiplexer = VectorReaderMultiplexer::new();

            // Main indexing loop
            loop {
                // Process events with priority (XS first, then S, M, L, XL)
                let processed = {
                    let mut state_guard = indexer_state.lock().expect("failed to unravel");
                    multiplexer.process_events(&mut state_guard.registry)
                };

                if !processed {
                    // No events available, yield CPU briefly
                    thread::sleep(Duration::from_micros(100));
                }
            }
        });

        Ok(Self { indexer_handle, query_handler })
    }

    /// Process event from ring buffer with specific size and extract vectors
    pub fn process_ring_event_sized<const SIZE: usize>(registry: &mut VectorRegistry, event: &PooledEvent<SIZE>) -> Result<(), Box<dyn std::error::Error>> {
        // Check if we have an extractor for this event type
        if let Some(extractor) = registry.extractors.get(&event.event_type) {
            // Try to parse as XaeroInternalEvent first
            if event.len == std::mem::size_of::<XaeroInternalEvent<SIZE>>() as u32 {
                let internal_event = unsafe { std::ptr::read(event as *const _ as *const XaeroInternalEvent<SIZE>) };

                let event_data = &internal_event.evt.data[..internal_event.evt.len as usize];

                // Extract vector using the appropriate extractor
                if let Some(vector) = extractor.extract_vector(event_data) {
                    // Generate event key for indexing
                    let event_key = generate_event_key(
                        event_data,
                        internal_event.evt.event_type,
                        internal_event.latest_ts,
                        internal_event.xaero_id_hash,
                        internal_event.vector_clock_hash,
                    );

                    // Get next node ID
                    let node_id = registry.next_node_id;
                    registry.next_node_id += 1;

                    // Insert into HNSW index
                    registry.hnsw.insert((vector.as_slice(), node_id));

                    // Update mappings
                    registry.node_to_event.insert(node_id, event_key);
                    registry.event_to_node.insert(event_key, node_id);

                    // Memory fence - ensures all writes above are visible to readers
                    fence(Ordering::Release);

                    tracing::debug!("Indexed vector for event: {:?}", event_key);
                }
            } else {
                // Fallback: parse as raw PooledEvent data
                let event_data = &event.data[..event.len as usize];

                if let Some(vector) = extractor.extract_vector(event_data) {
                    // Generate legacy event key (no peer/vector clock info)
                    let event_key = generate_event_key(
                        event_data,
                        event.event_type,
                        xaeroflux_core::date_time::emit_secs(),
                        [0; 32], // Empty xaero_id_hash
                        [0; 32], // Empty vector_clock_hash
                    );

                    let node_id = registry.next_node_id;
                    registry.next_node_id += 1;

                    registry.hnsw.insert((vector.as_slice(), node_id));
                    registry.node_to_event.insert(node_id, event_key);
                    registry.event_to_node.insert(event_key, node_id);

                    fence(Ordering::Release);

                    tracing::debug!("Indexed legacy vector for event: {:?}", event_key);
                }
            }
        }

        Ok(())
    }

    /// Perform a vector search query (thread-safe)
    pub fn search(&self, query: &VectorQueryRequest<256>) -> VectorQueryResponse<5> {
        let state = self.query_handler.lock().expect("failed to unravel");
        state.search(query)
    }

    /// Get current index statistics
    pub fn get_stats(&self) -> (usize, usize) {
        let state = self.query_handler.lock().expect("failed to unravel");
        (state.registry.next_node_id, state.registry.node_to_event.len())
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexing::vec_search_actor::VectorExtractor;

    // Mock extractor for testing
    struct MockExtractor;
    impl VectorExtractor for MockExtractor {
        fn extract_vector(&self, _event_data: &[u8]) -> Option<Vec<f32>> {
            Some(vec![0.1, 0.2, 0.3, 0.4]) // Mock 4D vector
        }
    }

    #[test]
    fn test_vector_reader_multiplexer() {
        // Test that multiplexer can be created with global rings
        let multiplexer = VectorReaderMultiplexer::new();

        // Verify all readers are initialized
        assert!(multiplexer.xs_reader.cursor == 0);
        assert!(multiplexer.s_reader.cursor == 0);
        assert!(multiplexer.m_reader.cursor == 0);
        assert!(multiplexer.l_reader.cursor == 0);
        assert!(multiplexer.xl_reader.cursor == 0);

        println!("✅ Vector reader multiplexer created successfully");
    }

    #[test]
    fn test_vector_search_state_creation() {
        xaeroflux_core::initialize();

        let mut extractors = HashMap::new();
        extractors.insert(1, Box::new(MockExtractor) as Box<dyn VectorExtractor>);

        let state = VectorSearchState::new(16, 1000, 16, 200, extractors, 4).expect("Failed to create vector search state");

        assert_eq!(state.registry.vector_dimension, 4);
        assert_eq!(state.registry.next_node_id, 0);

        println!("✅ Vector search state created successfully");
    }
}
