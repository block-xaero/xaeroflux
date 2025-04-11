/// Diagnostics related to the network sync subsystem.
#[derive(Debug, Clone)]
pub struct NetworkDiagnostics {
    /// Number of peers currently connected.
    pub connected_peers: usize,
    /// The peer ID (or identifier) with which the last successful sync occurred.
    pub last_sync_peer: Option<String>,
    /// Current bandwidth utilization in bytes per second.
    pub bandwidth_bytes_per_sec: u64,
}

/// Diagnostics related to the hosting device.
#[derive(Debug, Clone)]
pub struct SourceDeviceDiagnostics {
    /// Number of CPU cores available.
    pub cpu_cores: usize,
    /// Current active thread count.
    pub active_threads: usize,
    /// Current memory usage in bytes.
    pub memory_usage_bytes: u64,
    /// Remaining disk space in bytes.
    pub disk_free_bytes: u64,
}

/// Diagnostics for the Merkle tree indexing subsystem.
#[derive(Debug, Clone)]
pub struct MerkleIndexDiagnostics {
    /// Average latency to update the Merkle tree index in milliseconds.
    pub indexing_latency_ms: u64,
    /// Average latency to flush the Merkle tree index to disk, in milliseconds.
    pub flush_latency_ms: u64,
    /// Average latency per page write in the Merkle tree, in milliseconds.
    pub page_write_latency_ms: u64,
    /// Total count of proofs generated since start.
    pub proofs_generated: u64,
    /// The timestamp (epoch millis) of the last proof verified.
    pub last_proof_verified: Option<u64>,
}

/// Diagnostics for RocksDB storage latencies.
#[derive(Debug, Clone)]
pub struct RocksDBDiagnostics {
    /// Average latency for RocksDB write operations, in milliseconds.
    pub storage_latency_ms: u64,
    /// Number of write batches executed.
    pub write_batches: u64,
}

/// Diagnostics for the Write-Ahead Log (WAL) subsystem.
#[derive(Debug, Clone)]
pub struct WalDiagnostics {
    /// Average latency for writing to the WAL, in milliseconds.
    pub wal_write_latency_ms: u64,
    /// Average latency for flushing the WAL, in milliseconds.
    pub wal_flush_latency_ms: u64,
}

/// The top-level diagnostics structure that aggregates various subsystem metrics.
#[derive(Debug, Clone)]
pub struct Diagnostics {
    pub network: NetworkDiagnostics,
    pub source_device: SourceDeviceDiagnostics,
    pub merkle_index: MerkleIndexDiagnostics,
    pub rocksdb: RocksDBDiagnostics,
    pub wal: WalDiagnostics,
}
