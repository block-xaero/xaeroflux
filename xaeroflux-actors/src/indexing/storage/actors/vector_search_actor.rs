use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use blake3::hazmat::Mode::Hash;
use hnsw_rs::{hnsw, prelude::*};
use rusted_ring_new::{PooledEvent, RingBuffer};

use crate::aof::storage::format::EventKey;

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

pub struct VectorRegistryBuilder {
    pub index: Option<Arc<Mutex<VectorRegistry>>>,
    pub extractors: Arc<Mutex<HashMap<u32, Box<dyn VectorExtractor>>>>,
    pub vector_dimension: usize,
}

impl VectorRegistryBuilder {
    pub fn new() -> Self {
        Self {
            index: None,
            extractors: Arc::new(Mutex::new(HashMap::new())),
            vector_dimension: 0,
        }
    }

    pub fn with_extractor(&mut self, extractor_id: u32, extractor: Box<dyn VectorExtractor>) -> &mut Self {
        let res = self
            .extractors
            .lock()
            .expect("can't lock for extractors")
            .insert(extractor_id, extractor);
        match res {
            None => tracing::error!("extractor already registered"),
            Some(k) => tracing::info!("xtractor registered"),
        }
        self
    }

    pub fn with_vector_dimension(&mut self, vector_dimension: usize) -> &mut Self {
        self.vector_dimension = vector_dimension;
        self
    }

    pub fn with_index_configuration(
        &mut self,
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
    ) -> &mut Self {
        self.index = Some(Arc::new(Mutex::new(VectorRegistry {
            hnsw: Hnsw::new(
                max_nb_connection,
                max_elements,
                max_layer,
                ef_construction,
                DistCosine::default(),
            ),
            node_to_event: HashMap::new(),
            event_to_node: HashMap::new(),
            vector_dimension: 0,
        })));
        self
    }

    pub fn build(self) -> Option<Arc<Mutex<VectorRegistry>>> {
        self.index
    }
}
static IN_BUFFER : OnceLock<RingBuffer<128, 1024>> = OnceLock::new();
static OUT_BUFFER : OnceLock<RingBuffer<128, 1024>> = OnceLock::new();
pub struct VectorSearchIndex<const TSHIRT: usize, const CAPACITY: usize> {
    pub event_to_index: PooledEvent<TSHIRT>,

}
pub struct VectorSearchActor{

}