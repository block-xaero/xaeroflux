use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex, OnceLock},
};

use blake3::hazmat::Mode::Hash;
use bytemuck::{Pod, Zeroable};
use hnsw_rs::{hnsw, prelude::*};
use rkyv::Archive;
use rusted_ring_new::{PooledEvent, Reader, RingBuffer};
use serde::{Deserialize, Serialize};
use xaeroflux_core::event::ScanWindow;

use crate::aof::storage::format::{EventKey, SegmentMeta};

pub trait VectorExtractor: Send + Sync {
    fn extract_vector(&self, event_data: &[u8]) -> Option<Vec<f32>>;
}

// XaeroFlux schema service - application populates it
pub struct SchemaService;

pub struct VectorRegistry {
    pub hnsw: Hnsw<'static, f32, DistCosine>,
    pub node_to_event: HashMap<usize, EventKey>, // Just track ID mappings
    pub event_to_node: HashMap<EventKey, usize>,
    pub vector_dimension: usize,
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
            hnsw: Hnsw::new(
                max_nb_connection,
                max_elements,
                max_layer,
                ef_construction,
                DistCosine::default(),
            ),
            node_to_event: HashMap::new(),
            event_to_node: HashMap::new(),
            vector_dimension,
        }
    }
}

static IN_BUFFER: OnceLock<RingBuffer<128, 1024>> = OnceLock::new();
static OUT_BUFFER: OnceLock<RingBuffer<128, 1024>> = OnceLock::new();
pub struct VectorSearchIndex<const TSHIRT: usize, const CAPACITY: usize> {
    pub event_to_index: PooledEvent<TSHIRT>,
}
pub struct Source {
    pub reader: rusted_ring_new::Reader<128, 1024>,
}
pub struct Sink {
    pub writer: rusted_ring_new::Writer<128, 1024>,
}
pub struct Pipe {
    pub source: Source,
    pub sink: Sink,
}
// Core query request - POD for zero-copy over actor boundaries
#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct VectorQueryRequest<const TSHIRT: usize, const VECTOR_SIZE: usize> {
    pub query_id: u64,
    pub requester_id: [u8; 32],
    pub scope: QueryScope,
    pub vector: [f32; VECTOR_SIZE], // Query vector (fixed size for POD)
    pub k: u32,                     // Top-k results
    pub similarity_threshold: f32,  // Min similarity cutoff
    pub time_window: ScanWindow,    // Temporal constraints
    pub flags: QueryFlags,          // Search modifiers
}
unsafe impl<const TSHIRT: usize, const VECTOR_SIZE: usize> Pod for VectorQueryRequest<TSHIRT, VECTOR_SIZE> {}
unsafe impl<const TSHIRT: usize, const VECTOR_SIZE: usize> Zeroable for VectorQueryRequest<TSHIRT, VECTOR_SIZE> {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct QueryScope {
    pub group_id: Option<u64>,     // None = all accessible groups
    pub workspace_id: Option<u64>, // None = all workspaces in group
    pub object_id: Option<u64>,    // None = all objects in workspace
}

unsafe impl Pod for QueryScope {}
unsafe impl Zeroable for QueryScope {}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct QueryFlags {
    pub include_metadata: bool,   // Return checkpoint metadata
    pub include_operations: bool, // Return CRDT ops that created snapshot
    pub fan_out: bool,            // Search all levels vs. scoped only
    pub use_lora_bias: bool,      // Apply personal LoRA weights
}
unsafe impl Pod for QueryFlags {}
unsafe impl Zeroable for QueryFlags {}
pub struct VectorQueryResponse<const TSHIRT: usize, const VECTOR_SIZE: usize> {
    pub query_id: u64,
    pub results: [SearchResult<TSHIRT, VECTOR_SIZE>; 100],
    // give byte offset and segment meta to look for next
    pub continuation_token: Option<SegmentMeta>,
}
#[repr(C)]
#[derive(Copy, Clone)]
pub struct SearchResult<const TSHIRT: usize, const VECTOR_SIZE: usize> {
    pub similarity: f32,
    pub scope: QueryScope,           // Which level found this
    pub events: PooledEvent<TSHIRT>, // If requested
}

pub struct VectorSearchIndexer {
    pub pipe: Arc<Pipe>,
    index: VectorRegistry,
}

impl VectorSearchIndexer {
    pub fn new(
        pipe: Arc<Pipe>,
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
        extractors: HashMap<u32, Box<dyn VectorExtractor>>,
        vector_dimension: usize,
    ) -> Self {
        Self {
            pipe,
            index: VectorRegistry::new(
                max_nb_connection,
                max_elements,
                max_layer,
                ef_construction,
                extractors,
                vector_dimension,
            ),
        }
    }
}
