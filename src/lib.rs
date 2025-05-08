use crossbeam::channel::Receiver;
use indexing::merkle_tree::XaeroMerkleProof;
use rkyv::{Archive, Deserialize, Serialize};

pub mod core;
pub mod indexing;
pub mod logs;
pub mod networking;
pub mod sys;
/// Top-level engine for XFlux
pub struct XFlux {}

impl XFlux {
    /// Construct the engine; doesn't start any loops yet.
    pub fn new(_cfg: &crate::core::config::Config) -> Self {
        XFlux {}
    }

    /// Kick off all background tasks (AOF/LMDB, paging, MMR, P2P).
    /// Returns a handle you use to publish & subscribe.
    pub fn start(self) -> XFluxHandle {
        XFluxHandle {}
    }
}

/// Once you call `.start()`, this is your API surface:
#[derive(Clone)]
pub struct XFluxHandle {
    // internal channels, etc…
}

impl XFluxHandle {
    /// Publish a raw event into a named topic (channel).
    /// Under the hood it's append-only, paged, MMR'd, and gossiped.
    pub fn publish(&self, _topic: &str, _payload: Vec<u8>) -> Result<EventId, XFluxError> {
        todo!()
    }

    /// Subscribe to a topic. Receives every past & future event (with proofs).
    pub fn subscribe(&self, _topic: &str) -> Receiver<XFluxEvent> {
        todo!()
    }

    /// Discover other peers also interested in this topic.
    /// Emits `PeerInfo` any time someone appears or disappears.
    pub fn discover(&self, _topic: &str) -> Receiver<PeerInfo> {
        todo!()
    }

    /// (Optional) Shutdown the engine gracefully.
    pub fn shutdown(self) -> Result<(), String> {
        todo!()
    }
}

/// Every event you see on `subscribe()`:
#[repr(C)]
#[derive(Clone, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct XFluxEvent {
    pub id: EventId,             // unique, monotonic or random
    pub payload: Vec<u8>,        // your opaque app data
    pub timestamp: u64,          // unix millis when we wrote it
    pub proof: XaeroMerkleProof, // inclusion proof in our MMR root
}

#[repr(C)]
#[derive(Clone, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct PeerInfo {
    pub peer_id: String, // our opaque handle for them
}

pub type EventId = String;
pub type PeerId = String;
pub type XFluxError = Box<dyn std::error::Error + Send + Sync>;

#[cfg(test)]
mod xflux_smoke_tests {
    use crossbeam::channel::Receiver;

    use super::*;
    // Make sure you have a Config type in core::config.
    // If it doesn't impl Default yet, just construct it however you need.
    use crate::core::{CONF, initialize};

    #[test]
    fn new_and_start_smoke() {
        initialize();
        let cfg = CONF.get().expect("failed to get config");
        // new() must not panic
        let xf = XFlux::new(cfg);
        // start() must not panic and must return a handle
        let handle: XFluxHandle = xf.start();
        // we don't call any other methods yet
    }

    #[test]
    fn handle_is_cloneable() {
        initialize();
        let cfg = CONF.get().expect("failed to get config");
        let handle1 = XFlux::new(cfg).start();
        // Clone must work
        let _handle2 = handle1.clone();
    }

    // Sanity-check that subscribe & discover return the expected channel type,
    // but don't actually call .recv() since it's still unimplemented.
    #[test]
    fn subscribe_and_discover_signature() {
        initialize();
        let cfg = CONF.get().expect("failed to get config");
        let handle = XFlux::new(cfg).start();

        // just check the returned type, don't recv from it
        let _rx: Receiver<XFluxEvent> = handle.subscribe("some_topic");
        let _dx: Receiver<PeerInfo> = handle.discover("some_topic");

        // shutdown signature
        let _ = handle.shutdown();
    }
}
