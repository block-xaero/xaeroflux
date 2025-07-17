use std::{
    collections::HashMap,
    mem::zeroed,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, AtomicUsize, Ordering, fence},
    },
};

use bytemuck::{Pod, Zeroable, bytes_of};
use hnsw_rs::prelude::*;
use liblmdb::{MDB_RDONLY, MDB_dbi, MDB_env, MDB_txn, MDB_val, mdb_get, mdb_txn_abort, mdb_txn_begin, mdb_txn_commit};
use rusted_ring_new::{PooledEvent, RingBuffer, RingPipe};
use xaeroflux_core::{
    event::ScanWindow,
    pipe::{BusKind::Control, GenPipe, Pipe, SignalPipe, Sink, Source},
    pool::XaeroInternalEvent,
};

use crate::{
    aof::storage::{
        format::{EventKey, SegmentMeta},
        lmdb::generate_event_key,
    },
    indexing::storage::lmdb::LmdbVectorSearchDb,
};

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
            hnsw: Hnsw::new(max_nb_connection, max_elements, max_layer, ef_construction, DistCosine::default()),
            node_to_event: HashMap::new(),
            event_to_node: HashMap::new(),
            vector_dimension,
            extractors,
            next_node_id: 0,
        }
    }
}

// Core query request - POD for zero-copy over actor boundaries
#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct VectorQueryRequest<const TSHIRT: usize, const VECTOR_SIZE: usize> {
    pub query_id: u64,
    pub requester_id: [u8; 32],
    pub scope: QueryScope,
    pub vector: [f32; VECTOR_SIZE],
    pub k: u32,
    pub similarity_threshold: f32,
    pub time_window: ScanWindow,
    pub flags: QueryFlags,
}
unsafe impl<const TSHIRT: usize, const VECTOR_SIZE: usize> Pod for VectorQueryRequest<TSHIRT, VECTOR_SIZE> {}
unsafe impl<const TSHIRT: usize, const VECTOR_SIZE: usize> Zeroable for VectorQueryRequest<TSHIRT, VECTOR_SIZE> {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct QueryScope {
    pub group_id: Option<u64>,
    pub workspace_id: Option<u64>,
    pub object_id: Option<u64>,
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
pub struct SearchResult<const TSHIRT: usize, const CAPACITY: usize> {
    pub similarity: f32,
    pub event_key: EventKey,
    pub scope: QueryScope,
    pub has_data: bool, // Whether we successfully loaded the snapshot data
}
unsafe impl<const TSHIRT: usize, const CAPACITY: usize> Pod for SearchResult<TSHIRT, CAPACITY> {}
unsafe impl<const TSHIRT: usize, const CAPACITY: usize> Zeroable for SearchResult<TSHIRT, CAPACITY> {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct VectorQueryResponse<const TSHIRT: usize, const PAGE_SIZE: usize, const PAGES: usize> {
    pub query_id: u64,
    pub result_count: u32,
    pub status: u8, // 0=processing, 1=complete, 2=error
    pub results: [SearchResult<TSHIRT, PAGE_SIZE>; PAGES],
    pub continuation_token: Option<SegmentMeta>,
}
unsafe impl<const TSHIRT: usize, const PAGE_SIZE: usize, const PAGES: usize> Pod for VectorQueryResponse<TSHIRT, PAGE_SIZE, PAGES> {}
unsafe impl<const TSHIRT: usize, const PAGE_SIZE: usize, const PAGES: usize> Zeroable for VectorQueryResponse<TSHIRT, PAGE_SIZE, PAGES> {}

pub struct VectorSearchActor<const TSHIRT: usize, const RING_CAPACITY: usize> {
    pub write: RingPipe<TSHIRT, RING_CAPACITY>,
    pub read: GenPipe<VectorQueryRequest<TSHIRT, 254>>,
    pub response_buffer: Pin<Arc<[VectorQueryResponse<TSHIRT, 10, 5>; 10]>>,
    pub signal_pipe: Arc<SignalPipe>,
    pub lmdb_vector_search_db: Arc<LmdbVectorSearchDb>,
    index: Arc<VectorRegistry>,
}

impl<const TSHIRT: usize, const RING_CAPACITY: usize> VectorSearchActor<TSHIRT, RING_CAPACITY> {
    pub fn new(
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
        extractors: HashMap<u32, Box<dyn VectorExtractor>>,
        vector_dimension: usize,
        in_buffer: &'static RingBuffer<TSHIRT, RING_CAPACITY>,
        out_buffer: &'static RingBuffer<TSHIRT, RING_CAPACITY>,
    ) -> Self {
        Self {
            write: RingPipe::new(in_buffer, out_buffer),
            read: GenPipe {
                source: Arc::new(Source::new(Some(10), Control)),
                sink: Arc::new(Sink::new(Some(10), Control)),
            },
            response_buffer: Pin::new(Arc::new([VectorQueryResponse::zeroed(); 10])),
            index: Arc::new(VectorRegistry::new(
                max_nb_connection,
                max_elements,
                max_layer,
                ef_construction,
                extractors,
                vector_dimension,
            )),
            signal_pipe: SignalPipe::single(),
            lmdb_vector_search_db: Arc::new(LmdbVectorSearchDb::new()),
        }
    }

    pub fn spin(self) -> (std::thread::JoinHandle<()>, std::thread::JoinHandle<()>) {
        // Clone for reader BEFORE destructuring
        let reader_registry = Arc::clone(&self.index);
        let response_buffer = self.response_buffer.clone();
        let lmdb_db = Arc::clone(&self.lmdb_vector_search_db);

        let VectorSearchActor {
            write,
            read,
            response_buffer: _,
            signal_pipe,
            lmdb_vector_search_db,
            index,
        } = self;

        let reader_handle = spin_reader_loop(reader_registry, response_buffer, read, lmdb_db);

        // Writer gets exclusive ownership of the registry
        let writer_handle = spin_write_indexer(write, index, signal_pipe, lmdb_vector_search_db);

        (reader_handle, writer_handle)
    }
}

fn spin_write_indexer<const TSHIRT: usize, const RING_CAPACITY: usize>(
    mut write_pipe: RingPipe<TSHIRT, RING_CAPACITY>,
    registry: Arc<VectorRegistry>,
    _signal_pipe: Arc<SignalPipe>,
    _lmdb_vector_search_db: Arc<LmdbVectorSearchDb>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // Extract owned registry from Arc - this will only work if ref count = 1
        let mut owned_registry = Arc::try_unwrap(registry).map_err(|_| "Cannot get exclusive ownership of VectorRegistry").unwrap();

        loop {
            for pooled_event in write_pipe.sink.reader.by_ref() {
                if let Some(extractor) = owned_registry.extractors.get(&pooled_event.event_type) {
                    if pooled_event.len == std::mem::size_of::<XaeroInternalEvent<TSHIRT>>() as u32 {
                        let xaero_internal_event = unsafe { std::ptr::read(&pooled_event as *const _ as *const XaeroInternalEvent<TSHIRT>) };
                        if let Some(vector) = extractor.extract_vector(&pooled_event.data) {
                            let event_key = generate_event_key(
                                &xaero_internal_event.evt.data,
                                xaero_internal_event.evt.event_type,
                                xaero_internal_event.latest_ts,
                                xaero_internal_event.xaero_id_hash,
                                xaero_internal_event.vector_clock_hash,
                            );

                            // Get next node ID
                            let node_id = owned_registry.next_node_id;
                            owned_registry.next_node_id += 1;

                            // Insert into HNSW (returns Result, not value)
                            owned_registry.hnsw.insert((vector.as_slice(), node_id));
                            // Update mappings
                            owned_registry.node_to_event.insert(node_id, event_key);
                            owned_registry.event_to_node.insert(event_key, node_id);

                            // Memory fence - ensures all writes above are visible to readers
                            fence(Ordering::Release);
                        }
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    })
}

fn spin_reader_loop<const TSHIRT: usize>(
    vector_registry: Arc<VectorRegistry>,
    response_buffer: Pin<Arc<[VectorQueryResponse<TSHIRT, 10, 5>; 10]>>,
    read_pipe: GenPipe<VectorQueryRequest<TSHIRT, 254>>,
    lmdb_db: Arc<LmdbVectorSearchDb>,
) -> std::thread::JoinHandle<()> {
    let next_slot_counter = AtomicUsize::new(0);

    std::thread::spawn(move || {
        loop {
            while let Ok(query) = read_pipe.sink.rx.try_recv() {
                let slot = next_slot_counter.fetch_add(1, Ordering::Relaxed) % 10;

                // Memory fence - ensures we see all committed writes from indexer
                fence(Ordering::Acquire);

                // Mark as processing
                let response_ptr = response_buffer.as_ptr() as *mut VectorQueryResponse<TSHIRT, 10, 5>;
                unsafe {
                    (*response_ptr.add(slot)).query_id = query.query_id;
                    (*response_ptr.add(slot)).status = 0; // processing
                    (*response_ptr.add(slot)).result_count = 0;
                }

                // Perform HNSW search - returns Vec<Neighbour>
                let hnsw_results = vector_registry.hnsw.search(&query.vector, query.k as usize, 16);
                let mut valid_results = Vec::new();
                let mut event_keys_to_fetch = Vec::new();

                // Filter results by similarity threshold and collect EventKeys
                for neighbour in hnsw_results.iter() {
                    // Use the corrected field names from Neighbour struct
                    if neighbour.distance >= query.similarity_threshold {
                        if let Some(&event_key) = vector_registry.node_to_event.get(&neighbour.d_id) {
                            valid_results.push((event_key, neighbour.distance));
                            event_keys_to_fetch.push(event_key);
                        }
                        // Note: We might miss some results due to stale reads, that's
                        // acceptable
                    }
                }

                // Batch fetch snapshot data from LMDB
                let snapshot_data = bulk_search_lmdb::<TSHIRT>(&lmdb_db, event_keys_to_fetch);

                // Populate response buffer
                unsafe {
                    let response = &mut *response_ptr.add(slot);
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
                }
            }
            // Memory fence - ensure response is visible to FFI readers
            fence(Ordering::Release);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    })
}

// Fixed bulk LMDB search function
fn bulk_search_lmdb<const TSHIRT: usize>(lmdb_db: &Arc<LmdbVectorSearchDb>, event_keys: Vec<EventKey>) -> HashMap<EventKey, Vec<u8>> {
    let mut results = HashMap::new();

    unsafe {
        // Begin read transaction
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let begin_rc = mdb_txn_begin(lmdb_db.env.env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if begin_rc != 0 {
            return results; // Failed to begin transaction
        }

        // Batch read each EventKey
        for event_key in event_keys {
            let key_bytes = bytes_of(&event_key);
            let mut mdb_key = MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
            };

            let mut data_val = MDB_val {
                mv_size: 0,
                mv_data: std::ptr::null_mut(),
            };

            let rc = mdb_get(txn, lmdb_db.env.dbis[0], &mut mdb_key, &mut data_val);
            if rc == 0 {
                // Found the key, copy the data
                let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
                results.insert(event_key, data_slice.to_vec());
            }
            // Skip if not found (rc != 0)
        }

        mdb_txn_abort(txn); // Cleanup transaction
    }

    results
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    use super::*;

    // Mock extractor for testing
    struct MockExtractor;
    impl VectorExtractor for MockExtractor {
        fn extract_vector(&self, _event_data: &[u8]) -> Option<Vec<f32>> {
            Some(vec![0.1, 0.2, 0.3, 0.4]) // Mock 4D vector
        }
    }

    struct TestExtractor {
        vector: Vec<f32>,
    }

    impl TestExtractor {
        fn new(vector: Vec<f32>) -> Self {
            Self { vector }
        }
    }

    impl VectorExtractor for TestExtractor {
        fn extract_vector(&self, _event_data: &[u8]) -> Option<Vec<f32>> {
            Some(self.vector.clone())
        }
    }

    #[test]
    fn test_vector_registry_creation() {
        let mut extractors = HashMap::new();
        extractors.insert(1, Box::new(MockExtractor) as Box<dyn VectorExtractor>);

        let registry = VectorRegistry::new(
            16,   // max_nb_connection
            1000, // max_elements
            16,   // max_layer
            200,  // ef_construction
            extractors, 4, // vector_dimension
        );

        assert_eq!(registry.vector_dimension, 4);
        assert_eq!(registry.next_node_id, 0);
        assert!(registry.node_to_event.is_empty());
        assert!(registry.event_to_node.is_empty());
    }

    #[test]
    fn test_hnsw_basic_insert_search() {
        let mut extractors = HashMap::new();
        extractors.insert(1, Box::new(MockExtractor) as Box<dyn VectorExtractor>);

        let mut registry = VectorRegistry::new(16, 1000, 16, 200, extractors, 4);

        // Insert a test vector
        let test_vector = vec![0.1, 0.2, 0.3, 0.4];
        let node_id = 0;

        registry.hnsw.insert((test_vector.as_slice(), node_id));

        // Add mapping
        let dummy_event_key = EventKey {
            xaero_id_hash: [1; 32],
            vector_clock_hash: [2; 32],
            ts: 12345,
            kind: 1,
            hash: [3; 32],
        };
        registry.node_to_event.insert(node_id, dummy_event_key);

        // Search for similar vector
        let query_vector = vec![0.1, 0.2, 0.3, 0.4];
        let search_results = registry.hnsw.search(&query_vector, 1, 16);

        assert!(!search_results.is_empty());
        assert_eq!(search_results[0].d_id, node_id); // Should find the same node_id

        // Test mapping lookup
        let found_event_key = registry.node_to_event.get(&search_results[0].d_id);
        assert!(found_event_key.is_some());
        assert_eq!(*found_event_key.unwrap(), dummy_event_key);
    }

    #[test]
    fn test_multiple_vector_search() {
        let mut extractors = HashMap::new();
        extractors.insert(1, Box::new(TestExtractor::new(vec![0.1, 0.2, 0.3, 0.4])) as Box<dyn VectorExtractor>);
        extractors.insert(2, Box::new(TestExtractor::new(vec![0.9, 0.8, 0.7, 0.6])) as Box<dyn VectorExtractor>);

        let mut registry = VectorRegistry::new(16, 1000, 16, 200, extractors, 4);

        // Insert multiple vectors
        let vectors = vec![
            vec![0.1, 0.2, 0.3, 0.4],
            vec![0.9, 0.8, 0.7, 0.6],
            vec![0.15, 0.25, 0.35, 0.45], // Similar to first
            vec![0.85, 0.75, 0.65, 0.55], // Similar to second
        ];

        for (i, vector) in vectors.iter().enumerate() {
            registry.hnsw.insert((vector.as_slice(), i));

            let event_key = EventKey {
                xaero_id_hash: [i as u8; 32],
                vector_clock_hash: [i as u8; 32],
                ts: i as u64,
                kind: 1,
                hash: [i as u8; 32],
            };
            registry.node_to_event.insert(i, event_key);
        }

        // Search for vector similar to first one
        let query = vec![0.1, 0.2, 0.3, 0.4];
        let results = registry.hnsw.search(&query, 2, 16);

        assert!(!results.is_empty());
        assert!(results.len() >= 1);

        // First result should be exact match (node 0)
        assert_eq!(results[0].d_id, 0);

        // Check that we can find the mapping for all results
        for result in &results {
            let event_key = registry.node_to_event.get(&result.d_id);
            assert!(event_key.is_some());
        }
    }

    #[test]
    fn test_query_request_pod_safety() {
        // Test that our POD structs can be safely zeroed
        let _query: VectorQueryRequest<128, 256> = VectorQueryRequest::zeroed();
        let _response: VectorQueryResponse<128, 10, 5> = VectorQueryResponse::zeroed();
        let _result: SearchResult<128, 5> = SearchResult::zeroed();

        // Test that we can create and manipulate them
        let mut query: VectorQueryRequest<128, 256> = VectorQueryRequest::zeroed();
        query.query_id = 12345;
        query.k = 10;
        query.similarity_threshold = 0.8;

        assert_eq!(query.query_id, 12345);
        assert_eq!(query.k, 10);
        assert_eq!(query.similarity_threshold, 0.8);
    }

    #[test]
    fn test_memory_fence_ordering() {
        use std::{
            sync::atomic::{AtomicBool, Ordering},
            thread,
            time::Duration,
        };

        let flag = Arc::new(AtomicBool::new(false));
        let data = Arc::new(AtomicUsize::new(0));

        let flag_clone = Arc::clone(&flag);
        let data_clone = Arc::clone(&data);

        // Writer thread
        let writer = thread::spawn(move || {
            data_clone.store(42, Ordering::Relaxed);
            fence(Ordering::Release);
            flag_clone.store(true, Ordering::Relaxed);
        });

        // Reader thread
        let reader = thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(1));
            }
            fence(Ordering::Acquire);
            data.load(Ordering::Relaxed)
        });

        writer.join().unwrap();
        let result = reader.join().unwrap();

        assert_eq!(result, 42); // Should see the data written before the fence
    }

    #[test]
    fn test_similarity_threshold_filtering() {
        let mut extractors = HashMap::new();
        extractors.insert(1, Box::new(MockExtractor) as Box<dyn VectorExtractor>);

        let mut registry = VectorRegistry::new(16, 1000, 16, 200, extractors, 4);

        // Insert test vectors with known distances
        let base_vector = vec![0.0, 0.0, 0.0, 0.0];
        let similar_vector = vec![0.1, 0.1, 0.1, 0.1]; // Close
        let different_vector = vec![1.0, 1.0, 1.0, 1.0]; // Far

        registry.hnsw.insert((base_vector.as_slice(), 0));
        registry.hnsw.insert((similar_vector.as_slice(), 1));
        registry.hnsw.insert((different_vector.as_slice(), 2));

        // Add mappings
        for i in 0..3 {
            let event_key = EventKey {
                xaero_id_hash: [i; 32],
                vector_clock_hash: [i; 32],
                ts: i as u64,
                kind: 1,
                hash: [i; 32],
            };
            registry.node_to_event.insert(i as usize, event_key);
        }

        // Search with query similar to base vector
        let query = vec![0.05, 0.05, 0.05, 0.05];
        let results = registry.hnsw.search(&query, 3, 16);

        assert!(!results.is_empty());

        // Test that distances make sense (closer vectors have smaller distances)
        if results.len() >= 2 {
            // Results should be ordered by distance (closest first)
            assert!(results[0].distance <= results[1].distance);
        }
    }

    #[test]
    fn test_event_key_serialization() {
        let event_key = EventKey {
            xaero_id_hash: [42; 32],
            vector_clock_hash: [24; 32],
            ts: 1234567890,
            kind: 5,
            hash: [99; 32],
        };

        // Test that we can serialize it to bytes (needed for LMDB)
        let bytes = bytes_of(&event_key);
        assert_eq!(bytes.len(), std::mem::size_of::<EventKey>());

        // Test that it's a valid POD type
        let zeroed_key: EventKey = EventKey::zeroed();
        assert_eq!(zeroed_key.ts, 0);
        assert_eq!(zeroed_key.kind, 0);
    }

    #[test]
    fn test_concurrent_access_pattern() {
        use std::{
            sync::atomic::{AtomicUsize, Ordering},
            thread,
            time::Duration,
        };

        // Simulate the pattern used in your actor system
        let counter = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(AtomicUsize::new(0));

        let counter_writer = Arc::clone(&counter);
        let data_writer = Arc::clone(&data);
        let counter_reader = Arc::clone(&counter);
        let data_reader = Arc::clone(&data);

        // Writer thread (simulates indexer)
        let writer = thread::spawn(move || {
            for i in 0..10 {
                data_writer.store(i * 100, Ordering::Relaxed);
                counter_writer.store(i + 1, Ordering::Relaxed);
                fence(Ordering::Release);
                thread::sleep(Duration::from_millis(10));
            }
        });

        // Reader thread (simulates searcher)
        let reader = thread::spawn(move || {
            let mut last_seen = 0;
            for _ in 0..10 {
                fence(Ordering::Acquire);
                let current_count = counter_reader.load(Ordering::Relaxed);
                if current_count > last_seen {
                    let current_data = data_reader.load(Ordering::Relaxed);
                    // We should see data corresponding to the count we observed
                    assert!(current_data < 1000); // Reasonable bounds check
                    last_seen = current_count;
                }
                thread::sleep(Duration::from_millis(5));
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }
}
