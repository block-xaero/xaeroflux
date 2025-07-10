use std::sync::{Arc, OnceLock};

use bytemuck::{Pod, Zeroable};
use rusted_ring::{
    EventAllocator, EventSize, PooledEvent, RingPtr, pooled_event_ptr::PooledEventPtr,
};
use xaeroid::{XaeroID, XaeroProof};

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

use crate::vector_clock_actor::XaeroVectorClock;

pub enum XaeroEvent {
    XS(XaeroEventSized<64>),
    S(XaeroEventSized<256>),
    M(XaeroEventSized<1024>),
    L(XaeroEventSized<4096>),
    XL(XaeroEventSized<16384>),
}

#[derive(Debug, Clone, Copy)]
pub struct XaeroEventSized<const SIZE: usize> {
    pub evt: PooledEvent<SIZE>,
    pub author_id: [u8; 32],
    pub merkle_proof: Option<[u8; 32]>,
    pub vector_clock: Option<[u8; 32]>,
    pub latest_ts: u64,
}
unsafe impl<const SIZE: usize> Zeroable for XaeroEventSized<SIZE> {}
unsafe impl<const SIZE: usize> Pod for XaeroEventSized<SIZE> {}

#[derive(Debug, Clone, Copy)]
pub struct XaeroInternalEvent<const TSHIRT_SIZE: usize> {
    pub xaero_id_hash: [u8; 32],
    pub vector_clock_hash: [u8; 32],
    pub evt: rusted_ring_new::PooledEvent<TSHIRT_SIZE>,
    pub latest_ts: u64,
}

unsafe impl<const TSHIRT_SIZE: usize> Pod for XaeroInternalEvent<TSHIRT_SIZE> {}
unsafe impl<const TSHIRT_SIZE: usize> Zeroable for XaeroInternalEvent<TSHIRT_SIZE> {}

#[derive(Clone, Copy)]
pub struct XaeroPeerEvent<const TSHIRT_SIZE: usize> {
    pub evt: rusted_ring_new::PooledEvent<TSHIRT_SIZE>,
    pub author_id: Option<XaeroID>,
    pub merkle_proof: Option<XaeroProof>,
    pub vector_clock: XaeroVectorClock,
    pub latest_ts: u64,
}

unsafe impl<const TSHIRT_SIZE: usize> Pod for XaeroPeerEvent<TSHIRT_SIZE> {}
unsafe impl<const TSHIRT_SIZE: usize> Zeroable for XaeroPeerEvent<TSHIRT_SIZE> {}
