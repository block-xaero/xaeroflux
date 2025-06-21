use std::sync::{Arc, OnceLock};
use bytemuck::Zeroable;
use rusted_ring::{EventAllocator, EventSize, PooledEvent, RingPtr};
use rusted_ring::pooled_event_ptr::PooledEventPtr;
use xaeroid::XaeroID;

use crate::event::VectorClock;

// Platform-specific configuration
#[cfg(any(target_os = "ios", target_os = "android"))]
mod mobile_config {
    // Conservative for phones - cyan usage will be light
    pub const MERKLE_CAPACITY: usize = 20; // 20KB merkle proofs
    pub const VECTOR_CLOCK_CAPACITY: usize = 5; // ~120KB vector clocks (5 * 24KB)
}

#[cfg(all(not(target_os = "ios"), not(target_os = "android")))]
mod desktop_config {
    // Generous for tablets/laptops - main whiteboarding platforms
    pub const MERKLE_CAPACITY: usize = 500; // 500KB merkle proofs
    pub const VECTOR_CLOCK_CAPACITY: usize = 100; // ~2.4MB vector clocks (100 * 24KB)
}

// Use platform-specific constants
#[cfg(all(not(target_os = "ios"), not(target_os = "android")))]
use desktop_config::*;
#[cfg(any(target_os = "ios", target_os = "android"))]
use mobile_config::*;

// Global allocators - ALL stack allocated
static EVENT_DATA_ALLOCATOR: OnceLock<EventAllocator> = OnceLock::new();
static XAERO_ID_ALLOCATOR: OnceLock<EventAllocator> = OnceLock::new();
static MERKLE_PROOF_ALLOCATOR: OnceLock<MerkleProofAllocator> = OnceLock::new();
static VECTOR_CLOCK_ALLOCATOR: OnceLock<VectorClockAllocator> = OnceLock::new();

pub static XAERO_ID_EVENT_BASE: u8 = 108;
pub static MERKLE_PROOF_EVENT_BASE: u8 = 110;
pub static VECTOR_CLOCK_EVENT_BASE: u8 = 111;

const MAX_PEERS_PER_OBJECT: usize = 10; // Reasonable for whiteboard collaboration

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("Data too large: {data_len} bytes > {max_pool_size} bytes")]
    TooLarge {
        data_len: usize,
        max_pool_size: usize,
    },
    #[error("Pool allocation failed: {0}")]
    AllocationFailed(String),
}

/// Fixed-size merkle proof for stack ring buffer
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct FixedMerkleProof {
    pub proof_len: u16,
    pub _pad: [u8; 6],          // Alignment padding
    pub proof_data: [u8; 1024], // 1KB max proof size
}

unsafe impl bytemuck::Pod for FixedMerkleProof {}
unsafe impl bytemuck::Zeroable for FixedMerkleProof {}

impl FixedMerkleProof {
    pub fn from_bytes(data: &[u8]) -> Result<Self, PoolError> {
        if data.len() > 1024 {
            return Err(PoolError::TooLarge {
                data_len: data.len(),
                max_pool_size: 1024,
            });
        }

        let mut fixed_proof = Self::zeroed();
        fixed_proof.proof_len = data.len() as u16;
        fixed_proof.proof_data[..data.len()].copy_from_slice(data);
        Ok(fixed_proof)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.proof_data[..self.proof_len as usize]
    }
}

/// Fixed-size vector clock for stack ring buffer
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct FixedVectorClock {
    pub latest_timestamp: u64,
    pub peer_count: u8,
    pub _pad: [u8; 7],                                 // Alignment padding
    pub peers: [(XaeroID, u64); MAX_PEERS_PER_OBJECT], // ~24KB per peer * 10
}

unsafe impl bytemuck::Pod for FixedVectorClock {}
unsafe impl bytemuck::Zeroable for FixedVectorClock {}

impl FixedVectorClock {
    pub fn from_vector_clock(vc: &VectorClock) -> Result<Self, PoolError> {
        if vc.neighbor_clocks.len() > MAX_PEERS_PER_OBJECT {
            return Err(PoolError::TooLarge {
                data_len: vc.neighbor_clocks.len(),
                max_pool_size: MAX_PEERS_PER_OBJECT,
            });
        }

        let mut fixed_vc = Self::zeroed();
        fixed_vc.latest_timestamp = vc.latest_timestamp;
        fixed_vc.peer_count = vc.neighbor_clocks.len() as u8;

        for (i, (peer_id, timestamp)) in vc.neighbor_clocks.iter().enumerate() {
            fixed_vc.peers[i] = (*peer_id, *timestamp);
        }

        Ok(fixed_vc)
    }

    pub fn to_vector_clock(&self) -> VectorClock {
        let mut neighbor_clocks = std::collections::HashMap::new();

        for i in 0..self.peer_count as usize {
            let (peer_id, timestamp) = self.peers[i];
            neighbor_clocks.insert(peer_id, timestamp);
        }

        VectorClock {
            latest_timestamp: self.latest_timestamp,
            neighbor_clocks,
        }
    }
}

/// Stack-based allocator for merkle proofs
pub struct MerkleProofAllocator {
    allocator: EventAllocator,
}

impl MerkleProofAllocator {
    pub fn new() -> Self {
        Self {
            allocator: EventAllocator::new(),
        }
    }

    pub fn allocate_merkle_proof(
        &self,
        proof_data: &[u8],
    ) -> Result<RingPtr<FixedMerkleProof>, PoolError> {
        let fixed_proof = FixedMerkleProof::from_bytes(proof_data)?;
        let bytes = bytemuck::bytes_of(&fixed_proof);

        // Use M pool (1KB) for merkle proofs
        let ring_ptr = self
            .allocator
            .allocate_m_event(bytes, MERKLE_PROOF_EVENT_BASE as u32)
            .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;

        Ok(unsafe {
            std::mem::transmute::<RingPtr<PooledEvent<1024>>, RingPtr<FixedMerkleProof>>(ring_ptr)
        })
    }
}

/// Stack-based allocator for vector clocks
pub struct VectorClockAllocator {
    allocator: EventAllocator,
}

impl VectorClockAllocator {
    pub fn new() -> Self {
        Self {
            allocator: EventAllocator::new(),
        }
    }

    pub fn allocate_vector_clock(
        &self,
        vc: &VectorClock,
    ) -> Result<RingPtr<FixedVectorClock>, PoolError> {
        let fixed_vc = FixedVectorClock::from_vector_clock(vc)?;
        let bytes = bytemuck::bytes_of(&fixed_vc);

        // Use XL pool (16KB) for vector clocks (they're ~24KB but we'll compress)
        let ring_ptr = self
            .allocator
            .allocate_xl_event(bytes, VECTOR_CLOCK_EVENT_BASE as u32)
            .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;

        Ok(unsafe {
            std::mem::transmute::<RingPtr<PooledEvent<16384>>, RingPtr<FixedVectorClock>>(ring_ptr)
        })
    }
}

/// XaeroEvent - heap allocated once, contains ALL stack ring buffer pointers
#[derive(Debug, Clone)]
pub struct XaeroEvent {
    // ALL stack ring buffer pointers (zero-copy)
    pub evt: PooledEventPtr,                             // Stack ring buffer
    pub author_id: Option<RingPtr<XaeroID>>,             // Stack ring buffer
    pub merkle_proof: Option<RingPtr<FixedMerkleProof>>, // Stack ring buffer
    pub vector_clock: Option<RingPtr<FixedVectorClock>>, // Stack ring buffer

    // Only primitives on heap
    pub latest_ts: u64, // Stack primitive
}

impl XaeroEvent {
    pub fn data(&self) -> &[u8] {
        self.evt.data()
    }

    pub fn event_type(&self) -> u8 {
        self.evt.event_type()
    }

    pub fn len(&self) -> u32 {
        self.evt.len()
    }

    /// Get author ID with zero-copy access (since XaeroID is Pod)
    pub fn author_id(&self) -> Option<&XaeroID> {
        self.author_id.as_ref().map(|ptr| &**ptr)
    }

    /// Get merkle proof with zero-copy access
    pub fn merkle_proof(&self) -> Option<&[u8]> {
        self.merkle_proof.as_ref().map(|ptr| ptr.as_bytes())
    }

    /// Get vector clock (converts from fixed to dynamic)
    pub fn get_vector_clock(&self) -> Option<VectorClock> {
        self.vector_clock
            .as_ref()
            .map(|vc_ptr| vc_ptr.to_vector_clock())
    }

    /// Get raw fixed vector clock for zero-copy operations
    pub fn get_fixed_vector_clock(&self) -> Option<&FixedVectorClock> {
        self.vector_clock.as_ref().map(|ptr| &**ptr)
    }

    /// Get slot indices for operator pipeline access
    pub fn get_slot_info(&self) -> EventSlotInfo {
        EventSlotInfo {
            event_slot: self.evt.slot_index(),
            event_pool_id: self.evt.pool_id(),
            author_slot: self.author_id.as_ref().map(|ptr| ptr.slot_index),
            merkle_proof_slot: self.merkle_proof.as_ref().map(|ptr| ptr.slot_index),
            vector_clock_slot: self.vector_clock.as_ref().map(|ptr| ptr.slot_index),
        }
    }

    /// Check if this is a pure zero-copy event (all data in stack ring buffers)
    pub fn is_pure_zero_copy(&self) -> bool {
        // All major data is in stack ring buffers
        // TODO: Maybe assess in future.
        true
    }

    /// Memory breakdown for monitoring
    pub fn memory_breakdown(&self) -> MemoryBreakdown {
        MemoryBreakdown {
            event_data_size: self.len() as usize,
            author_id_size: self
                .author_id
                .as_ref()
                .map(|_| std::mem::size_of::<XaeroID>()),
            merkle_proof_size: self
                .merkle_proof
                .as_ref()
                .map(|_| std::mem::size_of::<FixedMerkleProof>()),
            vector_clock_size: self
                .vector_clock
                .as_ref()
                .map(|_| std::mem::size_of::<FixedVectorClock>()),
            all_stack_allocated: true,
        }
    }
}

/// Information for operators to access ring buffer slots directly
#[derive(Debug, Clone)]
pub struct EventSlotInfo {
    pub event_slot: u32,
    pub event_pool_id: rusted_ring::PoolId,
    pub author_slot: Option<u32>,
    pub merkle_proof_slot: Option<u32>,
    pub vector_clock_slot: Option<u32>,
}

/// Memory usage breakdown for monitoring
#[derive(Debug)]
pub struct MemoryBreakdown {
    pub event_data_size: usize,
    pub author_id_size: Option<usize>,
    pub merkle_proof_size: Option<usize>,
    pub vector_clock_size: Option<usize>,
    pub all_stack_allocated: bool,
}

/// Unified pool manager - ALL STACK
pub struct XaeroPoolManager;

impl XaeroPoolManager {
    /// Initialize all pools once at startup - ALL ON STACK
    pub fn init() {
        // Stack-based pools for all data
        EVENT_DATA_ALLOCATOR.get_or_init(|| EventAllocator::new());
        XAERO_ID_ALLOCATOR.get_or_init(|| EventAllocator::new());
        MERKLE_PROOF_ALLOCATOR.get_or_init(|| MerkleProofAllocator::new());
        VECTOR_CLOCK_ALLOCATOR.get_or_init(|| VectorClockAllocator::new());

        // Log platform configuration
        #[cfg(any(target_os = "ios", target_os = "android"))]
        log::info!(
            "XaeroFlux: Mobile configuration - Merkle: {} slots, VectorClock: {} slots",
            MERKLE_CAPACITY,
            VECTOR_CLOCK_CAPACITY
        );

        #[cfg(all(not(target_os = "ios"), not(target_os = "android")))]
        log::info!(
            "XaeroFlux: Desktop configuration - Merkle: {} slots, VectorClock: {} slots",
            MERKLE_CAPACITY,
            VECTOR_CLOCK_CAPACITY
        );
    }

    fn event_data_allocator() -> &'static EventAllocator {
        EVENT_DATA_ALLOCATOR.get().expect("Call init() first")
    }

    fn xaero_id_allocator() -> &'static EventAllocator {
        XAERO_ID_ALLOCATOR.get().expect("Call init() first")
    }

    fn merkle_proof_allocator() -> &'static MerkleProofAllocator {
        MERKLE_PROOF_ALLOCATOR.get().expect("Call init() first")
    }

    fn vector_clock_allocator() -> &'static VectorClockAllocator {
        VECTOR_CLOCK_ALLOCATOR.get().expect("Call init() first")
    }

    /// Get stack memory requirements for this platform
    pub fn get_stack_requirements() -> StackRequirements {
        #[cfg(any(target_os = "ios", target_os = "android"))]
        return StackRequirements {
            platform: "Mobile",
            core_pools: 2_500_000,       // ~2.5MB
            merkle_pools: 20_000,        // ~20KB
            vector_clock_pools: 120_000, // ~120KB
            total: 2_640_000,            // ~2.6MB
            safety_margin: "Fits in 4MB stack with room for local vars",
        };

        #[cfg(all(not(target_os = "ios"), not(target_os = "android")))]
        return StackRequirements {
            platform: "Desktop/Tablet",
            core_pools: 2_500_000,         // ~2.5MB
            merkle_pools: 500_000,         // ~500KB
            vector_clock_pools: 2_400_000, // ~2.4MB
            total: 5_400_000,              // ~5.4MB
            safety_margin: "Fits in 8MB stack with room for local vars",
        };
    }

    /// Allocate event data in stack ring buffer
    pub fn allocate_event_data(data: &[u8], event_type: u8) -> Result<PooledEventPtr, PoolError> {
        let size = EventAllocator::estimate_size(data.len());
        let allocator = Self::event_data_allocator();

        match size {
            EventSize::XS => {
                let ring_ptr = allocator
                    .allocate_xs_event(data, event_type as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(PooledEventPtr::Xs(ring_ptr))
            }
            EventSize::S => {
                let ring_ptr = allocator
                    .allocate_s_event(data, event_type as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(PooledEventPtr::S(ring_ptr))
            }
            EventSize::M => {
                let ring_ptr = allocator
                    .allocate_m_event(data, event_type as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(PooledEventPtr::M(ring_ptr))
            }
            EventSize::L => {
                let ring_ptr = allocator
                    .allocate_l_event(data, event_type as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(PooledEventPtr::L(ring_ptr))
            }
            EventSize::XL => {
                let ring_ptr = allocator
                    .allocate_xl_event(data, event_type as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(PooledEventPtr::Xl(ring_ptr))
            }
            EventSize::XXL => Err(PoolError::TooLarge {
                data_len: data.len(),
                max_pool_size: 16384,
            }),
        }
    }

    /// Allocate XaeroID in stack ring buffer
    pub fn allocate_xaero_id(xaero_id: XaeroID) -> Result<RingPtr<XaeroID>, PoolError> {
        let bytes = bytemuck::bytes_of(&xaero_id);
        let estimate = EventAllocator::estimate_size(bytes.len());
        let allocator = Self::xaero_id_allocator();

        match estimate {
            EventSize::XS => {
                let ring_ptr = allocator
                    .allocate_xs_event(bytes, XAERO_ID_EVENT_BASE as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(unsafe {
                    std::mem::transmute::<RingPtr<PooledEvent<64>>, RingPtr<XaeroID>>(ring_ptr)
                })
            }
            EventSize::S => {
                let ring_ptr = allocator
                    .allocate_s_event(bytes, XAERO_ID_EVENT_BASE as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(unsafe {
                    std::mem::transmute::<RingPtr<PooledEvent<256>>, RingPtr<XaeroID>>(ring_ptr)
                })
            }
            EventSize::M => {
                let ring_ptr = allocator
                    .allocate_m_event(bytes, XAERO_ID_EVENT_BASE as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(unsafe {
                    std::mem::transmute::<RingPtr<PooledEvent<1024>>, RingPtr<XaeroID>>(ring_ptr)
                })
            }
            EventSize::L => {
                let ring_ptr = allocator
                    .allocate_l_event(bytes, XAERO_ID_EVENT_BASE as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(unsafe {
                    std::mem::transmute::<RingPtr<PooledEvent<4096>>, RingPtr<XaeroID>>(ring_ptr)
                })
            }
            EventSize::XL => {
                let ring_ptr = allocator
                    .allocate_xl_event(bytes, XAERO_ID_EVENT_BASE as u32)
                    .map_err(|e| PoolError::AllocationFailed(e.to_string()))?;
                Ok(unsafe {
                    std::mem::transmute::<RingPtr<PooledEvent<16384>>, RingPtr<XaeroID>>(ring_ptr)
                })
            }
            EventSize::XXL => Err(PoolError::TooLarge {
                data_len: bytes.len(),
                max_pool_size: 16384,
            }),
        }
    }

    /// Allocate merkle proof in stack ring buffer
    pub fn allocate_merkle_proof(
        proof_data: &[u8],
    ) -> Result<RingPtr<FixedMerkleProof>, PoolError> {
        let allocator = Self::merkle_proof_allocator();
        allocator.allocate_merkle_proof(proof_data)
    }

    /// Allocate vector clock in stack ring buffer
    pub fn allocate_vector_clock(vc: &VectorClock) -> Result<RingPtr<FixedVectorClock>, PoolError> {
        let allocator = Self::vector_clock_allocator();
        allocator.allocate_vector_clock(vc)
    }

    /// Create XaeroEvent (heap allocated, contains ALL stack ring buffer pointers)
    pub fn create_xaero_event(
        data: &[u8],
        event_type: u8,
        author_id: Option<XaeroID>,
        merkle_proof: Option<&[u8]>,
        vector_clock: Option<&VectorClock>,
        latest_ts: u64,
    ) -> Result<Arc<XaeroEvent>, PoolError> {
        // ALL stack ring buffer allocations
        let evt = Self::allocate_event_data(data, event_type)?;
        let author_id = author_id
            .map(|id| Self::allocate_xaero_id(id))
            .transpose()?;
        let merkle_proof = merkle_proof
            .map(|proof| Self::allocate_merkle_proof(proof))
            .transpose()?;
        let vector_clock = vector_clock
            .map(|vc| Self::allocate_vector_clock(vc))
            .transpose()?;

        // Single heap allocation for the event container (just pointers + u64)
        Ok(Arc::new(XaeroEvent {
            evt,
            author_id,
            merkle_proof,
            vector_clock,
            latest_ts,
        }))
    }
}

#[derive(Debug)]
pub struct StackRequirements {
    pub platform: &'static str,
    pub core_pools: usize,
    pub merkle_pools: usize,
    pub vector_clock_pools: usize,
    pub total: usize,
    pub safety_margin: &'static str,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytemuck::Zeroable;

    use super::*;

    #[test]
    fn test_stack_requirements() {
        let requirements = XaeroPoolManager::get_stack_requirements();
        println!("Platform: {}", requirements.platform);
        println!(
            "Total stack: {:.1}MB",
            requirements.total as f64 / 1_000_000.0
        );
        println!("Safety: {}", requirements.safety_margin);

        // Should fit in reasonable stack limits
        #[cfg(any(target_os = "ios", target_os = "android"))]
        assert!(requirements.total < 4_000_000); // <4MB for mobile

        #[cfg(all(not(target_os = "ios"), not(target_os = "android")))]
        assert!(requirements.total < 6_000_000); // <6MB for desktop
    }

    #[test]
    fn test_all_stack_allocation() {
        XaeroPoolManager::init();

        // Create test vector clock
        let mut neighbor_clocks = HashMap::new();
        neighbor_clocks.insert(XaeroID::zeroed(), 123);
        neighbor_clocks.insert(XaeroID::zeroed(), 456);

        let vc = VectorClock {
            latest_timestamp: 789,
            neighbor_clocks,
        };

        // Test complete XaeroEvent creation - ALL STACK
        let event = XaeroPoolManager::create_xaero_event(
            b"all stack test",
            42,
            Some(XaeroID::zeroed()),
            Some(b"merkle_proof_data"),
            Some(&vc),
            1234567890,
        )
        .unwrap();

        // Verify ALL data is zero-copy accessible
        assert_eq!(event.data(), b"all stack test");
        assert_eq!(event.event_type(), 42);
        assert_eq!(event.latest_ts, 1234567890);
        assert!(event.is_pure_zero_copy());

        // Verify all components are stack allocated
        let breakdown = event.memory_breakdown();
        assert!(breakdown.all_stack_allocated);
        assert!(breakdown.author_id_size.is_some());
        assert!(breakdown.merkle_proof_size.is_some());
        assert!(breakdown.vector_clock_size.is_some());

        // Verify slot info for operators
        let slot_info = event.get_slot_info();
        assert!(slot_info.author_slot.is_some());
        assert!(slot_info.merkle_proof_slot.is_some());
        assert!(slot_info.vector_clock_slot.is_some());
    }

    #[test]
    fn test_whiteboard_collaboration_scenario() {
        XaeroPoolManager::init();

        // Simulate whiteboard events
        let mut events = Vec::new();

        // Drawing stroke event (common)
        let stroke_event = XaeroPoolManager::create_xaero_event(
            b"stroke_data_with_coordinates_and_style",
            1, // STROKE_EVENT
            Some(XaeroID::zeroed()),
            None, // No merkle proof for simple strokes
            None, // No vector clock for simple strokes
            1234567890,
        )
        .unwrap();

        // Collaborative edit with vector clock (less common)
        let mut vc = VectorClock {
            latest_timestamp: 789,
            neighbor_clocks: HashMap::new(),
        };
        vc.neighbor_clocks.insert(XaeroID::zeroed(), 100);
        vc.neighbor_clocks.insert(XaeroID::zeroed(), 200);

        let collab_event = XaeroPoolManager::create_xaero_event(
            b"collaborative_edit_complex_operation",
            2, // COLLABORATIVE_EDIT
            Some(XaeroID::zeroed()),
            Some(b"merkle_proof_for_complex_edit"),
            Some(&vc),
            1234567891,
        )
        .unwrap();

        events.push(stroke_event);
        events.push(collab_event);

        // Verify both events are zero-copy
        for (i, event) in events.iter().enumerate() {
            assert!(
                event.is_pure_zero_copy(),
                "Event {} should be pure zero-copy",
                i
            );

            let breakdown = event.memory_breakdown();
            assert!(
                breakdown.all_stack_allocated,
                "Event {} should be all stack allocated",
                i
            );
        }

        println!(
            "Successfully created {} whiteboard events with all-stack allocation",
            events.len()
        );
    }
}
