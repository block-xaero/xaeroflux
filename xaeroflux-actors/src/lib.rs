pub mod aof;
pub mod indexing;
mod networking;

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
    thread::JoinHandle,
};

use rusted_ring_new::{
    PooledEvent, RingBuffer, Writer, L_CAPACITY, L_TSHIRT_SIZE, M_CAPACITY, M_TSHIRT_SIZE, S_CAPACITY, S_TSHIRT_SIZE, XL_CAPACITY, XL_TSHIRT_SIZE, XS_CAPACITY, XS_TSHIRT_SIZE,
};

use crate::aof::ring_buffer_actor::AofActor;
// ================================================================================================
// GLOBAL RING BUFFERS - MAIN (EventBus writes to these, AOF/VectorSearch read from these)
// ================================================================================================

pub static XS_RING: OnceLock<RingBuffer<XS_TSHIRT_SIZE, XS_CAPACITY>> = OnceLock::new();
pub static S_RING: OnceLock<RingBuffer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();
pub static M_RING: OnceLock<RingBuffer<M_TSHIRT_SIZE, M_CAPACITY>> = OnceLock::new();
pub static L_RING: OnceLock<RingBuffer<L_TSHIRT_SIZE, L_CAPACITY>> = OnceLock::new();
pub static XL_RING: OnceLock<RingBuffer<XL_TSHIRT_SIZE, XL_CAPACITY>> = OnceLock::new();

// ================================================================================================
// GLOBAL RING BUFFERS - P2P (P2P actors write to these - for future use)
// ================================================================================================

pub static P2P_XS_RING: OnceLock<RingBuffer<XS_TSHIRT_SIZE, XS_CAPACITY>> = OnceLock::new();
pub static P2P_S_RING: OnceLock<RingBuffer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();
pub static P2P_M_RING: OnceLock<RingBuffer<M_TSHIRT_SIZE, M_CAPACITY>> = OnceLock::new();
pub static P2P_L_RING: OnceLock<RingBuffer<L_TSHIRT_SIZE, L_CAPACITY>> = OnceLock::new();
pub static P2P_XL_RING: OnceLock<RingBuffer<XL_TSHIRT_SIZE, XL_CAPACITY>> = OnceLock::new();

// ================================================================================================
// EVENT BUS - JUST HOUSES WRITERS
// ================================================================================================

pub struct EventBus {
    xs_writer: Writer<XS_TSHIRT_SIZE, XS_CAPACITY>,
    s_writer: Writer<S_TSHIRT_SIZE, S_CAPACITY>,
    m_writer: Writer<M_TSHIRT_SIZE, M_CAPACITY>,
    l_writer: Writer<L_TSHIRT_SIZE, L_CAPACITY>,
    xl_writer: Writer<XL_TSHIRT_SIZE, XL_CAPACITY>,
}

impl EventBus {
    /// Create new EventBus with writers to main ring buffers
    pub fn new() -> Self {
        let xs_ring = XS_RING.get_or_init(|| RingBuffer::new());
        let s_ring = S_RING.get_or_init(|| RingBuffer::new());
        let m_ring = M_RING.get_or_init(|| RingBuffer::new());
        let l_ring = L_RING.get_or_init(|| RingBuffer::new());
        let xl_ring = XL_RING.get_or_init(|| RingBuffer::new());

        Self {
            xs_writer: Writer::new(xs_ring),
            s_writer: Writer::new(s_ring),
            m_writer: Writer::new(m_ring),
            l_writer: Writer::new(l_ring),
            xl_writer: Writer::new(xl_ring),
        }
    }

    /// Write XS event
    pub fn write_xs(&mut self, event: PooledEvent<XS_TSHIRT_SIZE>) {
        self.xs_writer.add(event);
    }

    /// Write S event
    pub fn write_s(&mut self, event: PooledEvent<S_TSHIRT_SIZE>) {
        self.s_writer.add(event);
    }

    /// Write M event
    pub fn write_m(&mut self, event: PooledEvent<M_TSHIRT_SIZE>) {
        self.m_writer.add(event);
    }

    /// Write L event
    pub fn write_l(&mut self, event: PooledEvent<L_TSHIRT_SIZE>) {
        self.l_writer.add(event);
    }

    /// Write XL event
    pub fn write_xl(&mut self, event: PooledEvent<XL_TSHIRT_SIZE>) {
        self.xl_writer.add(event);
    }

    /// Helper to write data to optimal ring buffer
    pub fn write_optimal(&mut self, data: &[u8], event_type: u32) -> Result<(), XaeroFluxError> {
        let data_len = data.len();

        if data_len <= XS_TSHIRT_SIZE {
            let event = Self::create_pooled_event::<XS_TSHIRT_SIZE>(data, event_type)?;
            self.write_xs(event);
        } else if data_len <= S_TSHIRT_SIZE {
            let event = Self::create_pooled_event::<S_TSHIRT_SIZE>(data, event_type)?;
            self.write_s(event);
        } else if data_len <= M_TSHIRT_SIZE {
            let event = Self::create_pooled_event::<M_TSHIRT_SIZE>(data, event_type)?;
            self.write_m(event);
        } else if data_len <= L_TSHIRT_SIZE {
            let event = Self::create_pooled_event::<L_TSHIRT_SIZE>(data, event_type)?;
            self.write_l(event);
        } else if data_len <= XL_TSHIRT_SIZE {
            let event = Self::create_pooled_event::<XL_TSHIRT_SIZE>(data, event_type)?;
            self.write_xl(event);
        } else {
            return Err(XaeroFluxError::DataTooLarge(data_len));
        }

        Ok(())
    }

    /// Helper to create PooledEvent
    fn create_pooled_event<const SIZE: usize>(data: &[u8], event_type: u32) -> Result<PooledEvent<SIZE>, XaeroFluxError> {
        if data.len() > SIZE {
            return Err(XaeroFluxError::DataTooLarge(data.len()));
        }

        let mut event_data = [0u8; SIZE];
        event_data[..data.len()].copy_from_slice(data);

        Ok(PooledEvent {
            data: event_data,
            len: data.len() as u32,
            event_type,
        })
    }
}

pub struct P2PRingAccess;

impl P2PRingAccess {
    /// Get writer for P2P XS ring
    pub fn xs_writer() -> Writer<XS_TSHIRT_SIZE, XS_CAPACITY> {
        let ring = P2P_XS_RING.get_or_init(|| RingBuffer::new());
        Writer::new(ring)
    }

    /// Get writer for P2P S ring
    pub fn s_writer() -> Writer<S_TSHIRT_SIZE, S_CAPACITY> {
        let ring = P2P_S_RING.get_or_init(|| RingBuffer::new());
        Writer::new(ring)
    }

    /// Get writer for P2P M ring
    pub fn m_writer() -> Writer<M_TSHIRT_SIZE, M_CAPACITY> {
        let ring = P2P_M_RING.get_or_init(|| RingBuffer::new());
        Writer::new(ring)
    }

    /// Get writer for P2P L ring
    pub fn l_writer() -> Writer<L_TSHIRT_SIZE, L_CAPACITY> {
        let ring = P2P_L_RING.get_or_init(|| RingBuffer::new());
        Writer::new(ring)
    }

    /// Get writer for P2P XL ring
    pub fn xl_writer() -> Writer<XL_TSHIRT_SIZE, XL_CAPACITY> {
        let ring = P2P_XL_RING.get_or_init(|| RingBuffer::new());
        Writer::new(ring)
    }
}

// ================================================================================================
// VECTOR SEARCH TYPES
// ================================================================================================

pub trait VectorExtractor: Send + Sync {
    fn extract_vector(&self, event_data: &[u8]) -> Option<Vec<f32>>;
}

#[derive(Debug, Clone)]
pub struct VectorSearchStats {
    pub total_indexed: usize,
    pub active_nodes: usize,
}

use crate::indexing::vec_search_actor::{VectorQueryRequest, VectorQueryResponse, VectorSearchActor};

pub struct XaeroFlux {
    pub event_bus: EventBus,
    pub vector_search: Option<Arc<VectorSearchActor>>,
    pub aof_handle: Option<JoinHandle<()>>,
}

impl XaeroFlux {
    /// Create a new XaeroFlux instance
    pub fn new() -> Self {
        Self {
            event_bus: EventBus::new(),
            vector_search: None,
            aof_handle: None,
        }
    }

    /// Start the AOF actor
    pub fn start_aof(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let aof_actor = AofActor::spin()?;
        self.aof_handle = Some(aof_actor.jh);
        Ok(())
    }

    /// Start vector search with extractors
    pub fn start_vector_search(
        &mut self,
        extractors: HashMap<u32, Box<dyn crate::indexing::vec_search_actor::VectorExtractor>>,
        vector_dimension: usize,
        max_nb_connection: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let actor = VectorSearchActor::spin(max_nb_connection, max_elements, max_layer, ef_construction, extractors, vector_dimension)?;

        self.vector_search = Some(Arc::new(actor));
        Ok(())
    }

    /// Write event data to optimal ring buffer
    pub fn write_event(&mut self, data: &[u8], event_type: u32) -> Result<(), XaeroFluxError> {
        self.event_bus.write_optimal(data, event_type)
    }

    // ================================================================================================
    // VECTOR SEARCH API
    // ================================================================================================

    /// Search using a single vector
    pub fn search_vector(&self, vector: Vec<f32>, k: u32, similarity_threshold: f32) -> Result<VectorQueryResponse<5>, XaeroFluxError> {
        let vector_search = self.vector_search.as_ref().ok_or(XaeroFluxError::VectorSearchNotStarted)?;

        let mut query_vector = [0.0f32; 256];
        let copy_len = std::cmp::min(vector.len(), 256);
        query_vector[..copy_len].copy_from_slice(&vector[..copy_len]);

        let query = VectorQueryRequest {
            query_id: xaeroflux_core::date_time::emit_secs(),
            requester_id: [0; 32],
            scope: crate::indexing::vec_search_actor::QueryScope {
                group_id: None,
                workspace_id: None,
                object_id: None,
            },
            vector: query_vector,
            k,
            similarity_threshold,
            time_window: xaeroflux_core::event::ScanWindow { start: 0, end: u64::MAX },
            flags: crate::indexing::vec_search_actor::QueryFlags {
                include_metadata: true,
                include_operations: false,
                fan_out: false,
                use_lora_bias: false,
            },
        };

        Ok(vector_search.search(&query))
    }

    /// Search using multiple vectors
    pub fn search_vectors(&self, vectors: Vec<Vec<f32>>, k: u32, similarity_threshold: f32) -> Result<Vec<VectorQueryResponse<5>>, XaeroFluxError> {
        let mut results = Vec::new();

        for vector in vectors {
            let result = self.search_vector(vector, k, similarity_threshold)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Get vector search statistics
    pub fn vector_search_stats(&self) -> Result<VectorSearchStats, XaeroFluxError> {
        let vector_search = self.vector_search.as_ref().ok_or(XaeroFluxError::VectorSearchNotStarted)?;

        let (total_indexed, active_nodes) = vector_search.get_stats();
        Ok(VectorSearchStats { total_indexed, active_nodes })
    }
}

// ================================================================================================
// ERROR TYPES
// ================================================================================================

#[derive(Debug, Clone)]
pub enum XaeroFluxError {
    VectorSearchNotStarted,
    DataTooLarge(usize),
    InvalidData,
    ActorError(String),
}

impl std::fmt::Display for XaeroFluxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            XaeroFluxError::VectorSearchNotStarted => write!(f, "Vector search not started"),
            XaeroFluxError::DataTooLarge(size) => write!(f, "Data too large: {} bytes", size),
            XaeroFluxError::InvalidData => write!(f, "Invalid data"),
            XaeroFluxError::ActorError(msg) => write!(f, "Actor error: {}", msg),
        }
    }
}

impl std::error::Error for XaeroFluxError {}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_bus_creation() {
        let bus = EventBus::new();
        // Basic creation test - writers should be ready
        println!("✅ EventBus created with writers");
    }

    #[test]
    fn test_event_bus_write_optimal() {
        let mut bus = EventBus::new();

        let test_data = b"hello world";
        let result = bus.write_optimal(test_data, 42);
        assert!(result.is_ok());

        println!("✅ EventBus write_optimal works");
    }

    #[test]
    fn test_p2p_ring_access() {
        // Test that P2P actors can get writers
        let _xs_writer = P2PRingAccess::xs_writer();
        let _s_writer = P2PRingAccess::s_writer();
        let _m_writer = P2PRingAccess::m_writer();
        let _l_writer = P2PRingAccess::l_writer();
        let _xl_writer = P2PRingAccess::xl_writer();

        println!("✅ P2P ring access works");
    }

    #[test]
    fn test_xaeroflux_creation() {
        let xf = XaeroFlux::new();
        assert!(xf.vector_search.is_none());
        assert!(xf.aof_handle.is_none());

        println!("✅ XaeroFlux created successfully");
    }

    #[test]
    fn test_xaeroflux_write_event() {
        let mut xf = XaeroFlux::new();

        let test_data = b"test event data";
        let result = xf.write_event(test_data, 42);
        assert!(result.is_ok());

        println!("✅ XaeroFlux write_event works");
    }

    #[test]
    fn test_oversized_data() {
        let mut xf = XaeroFlux::new();

        let oversized_data = vec![0u8; 20000]; // Larger than XL
        let result = xf.write_event(&oversized_data, 42);
        assert!(matches!(result, Err(XaeroFluxError::DataTooLarge(_))));

        println!("✅ Oversized data properly rejected");
    }
}
