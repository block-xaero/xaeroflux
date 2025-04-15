use core::{event, event_buffer::RawEvent};
use std::sync::Arc;

use logs::diagnostics::Diagnostics;

pub mod core;
pub mod engine;
pub mod indexing;
pub mod logs;
pub mod sys;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum XaeroEngineMode {
    Normal,
    Debug,
    Test,
    PerformanceTesting,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum XaeroEngineState {
    Running(XaeroEngineMode),
    Stopped(XaeroEngineMode),
    Shutdown(XaeroEngineMode),
}

#[derive(Clone, Debug)]

/// Represents the configuration for the XaeroFlux engine
/// # Fields
/// * `f_path` - Path to the storage
/// * `io_threads` - Number of IO threads
/// * `cpu_threads` - Number of CPU threads
/// * `max_connections` - Maximum number of connections
/// * `peer_sync_interval` - Peer sync interval
/// * `root_zero_id` - Root zero ID which is did:peer + zk_proofs that can aid verify if user is
///   legit
pub struct XaeroFluxConfig {
    pub f_path: String,
    pub io_threads: usize,
    pub cpu_threads: usize,
    pub max_connections: usize,
    pub peer_sync_interval: usize,
    pub root_zero_id: [u8; 32],
}

/// Represents the result of the XaeroFlux engine
pub type XaeroResult<T> = std::result::Result<T, anyhow::Error>;
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Represents a key in the database
/// # Fields
/// * `timestamp` - Timestamp of the event
/// * `hash` - Hash of the event
/// # Example
/// ```
/// use xaeroflux::XaeroFluxKey;
/// let key = XaeroFluxKey {
///     timestamp: [0; 8],
///     hash: [0; 32],
/// };
/// ```
/// # Note / TODO: Document better.
pub struct XaeroFluxKey {
    pub timestamp: [u8; 8],
    pub hash: [u8; 32],
}

/// Represents the XaeroFlux database
pub struct XaeroFluxDB {
    pub config: Arc<XaeroFluxConfig>,
    pub storage_path: String,
}
/// XaeroFluxDB is a trait that defines the interface for the database
pub trait XaeroFluxDBOps {
    fn new(conf: Arc<XaeroFluxConfig>) -> Self;
    fn open(&self, storage_path: &str) -> XaeroResult<XaeroFluxDB>;
    /// Truncates the entire database, THIS IS NOT RECOVERABLE!
    /// # Arguments
    /// * `storage_path` - Path to the storage
    /// # Returns true if the database was truncated successfully
    /// # Returns false if the database was not truncated successfully
    /// TODO: NOTE This function would require a zeroId to be implemented.
    fn truncate(&self /* zeroId: */) -> XaeroResult<()>;
    /// Closes
    fn close(&self) -> XaeroResult<()>;
    /// Get range of events from the database
    /// # Arguments
    /// * `t1` - Start time.
    /// * `t2` - End time.
    /// # Returns vector of raw events found
    fn range(&self, t1: u64, t2: u64) -> XaeroResult<Vec<RawEvent>>;
    /// Append an event to the database
    /// # Arguments
    /// * `key` - Key of the event
    /// * `value` - Raw event to be appended
    /// # Returns true if the event was appended successfully
    /// # Returns false if the event was not appended successfully
    fn append(&self, key: XaeroFluxKey, value: RawEvent) -> XaeroResult<()>;
    /// Verify the proof of the event in the database
    /// # Arguments
    /// * `event` - Raw event to be verified
    /// * `proof` - Proof of the event
    /// * # Returns true if the proof is valid
    fn verify_proof(&self, event: RawEvent) -> XaeroResult<()>;
}

/// Meta db trait for advanced operations, such as diagnostics and rebuilding the merkle index
pub trait XaeroFluxMetaDBOps: XaeroFluxDBOps {
    fn enable_diagnostics(&self, zero_id: &str) -> XaeroResult<()>;

    fn diagnostics_report(&self, zero_id: &str) -> XaeroResult<Diagnostics>;

    /// Rebuild the merkle index using the given timestamps
    /// # Arguments
    /// * `ts1` - Start timestamp
    /// * `ts2` - End timestamp
    /// # Returns vector of merkle index paths
    /// # Returns error if the operation fails
    /// # Returns empty vector if no paths are found
    fn rebuild_merkle_index(&self, ts1: u64, ts2: u64) -> XaeroResult<Vec<String>>;
}
