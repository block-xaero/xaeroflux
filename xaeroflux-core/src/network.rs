#[allow(dead_code)]
use std::sync::{Arc, Mutex};

use xaeroid::XaeroID;

/// Allows you to discover peers or actors that emit events for:
/// Either `Workspace` or `Object` or
pub trait PeerDiscovery {
    type Conduits;
    fn conduits(&self) -> &Self::Conduits;
    fn spin(&mut self);
}
pub static MAX_PEERS_PER_GROUP: i32 = 100_000;

pub struct PeerAllocator {}
impl Default for PeerAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerAllocator {
    pub fn new() -> Self {
        PeerAllocator {}
    }
}
pub struct GroupPeerDiscovery {
    pub peer_cache: Arc<Mutex<[XaeroID; 100_000]>>,
}
