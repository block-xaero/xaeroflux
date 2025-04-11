# XaeroFlux Diagnostics

XaeroFlux is a P2P, cloudless storage engine built for mobile-first platforms like iOS and Android. It stores append-only (AO) raw event data as `Vec<u8>` received via crossbeam channels, decodes them into typed events, and persists them using RocksDB. All data is indexed using a Merkle tree structure to enable fast diffing and merging among peers. Peers discover and sync via a distributed DHT.

This README documents the **Diagnostics Module**, which provides subsystem-level performance metrics. These diagnostics are collected locally and optionally **probed remotely by a privileged identity (`zero_id`)**, then **advertised to the DHT** for distributed performance visibility.

---

## 🔍 Purpose

The diagnostics module gives a comprehensive view of system health by collecting metrics across five core subsystems:

1. **Network Sync**
2. **Source Device (Mobile Hardware)**
3. **Merkle Indexing**
4. **RocksDB Storage**
5. **Write-Ahead Logging (WAL)**

These diagnostics support:

- Peer performance analysis
- Mobile resource usage reporting
- Merkle tree latency and proof tracking
- Storage layer profiling
- System-wide distributed telemetry

---

## 📦 Diagnostics Structure

### `Diagnostics`

Aggregates all subsystem metrics:

```rust
pub struct Diagnostics {
    pub network: NetworkDiagnostics,
    pub source_device: SourceDeviceDiagnostics,
    pub merkle_index: MerkleIndexDiagnostics,
    pub rocksdb: RocksDBDiagnostics,
    pub wal: WalDiagnostics,
}
```

### Network Sync

```rust
pub struct NetworkDiagnostics {
    pub connected_peers: usize,
    pub last_sync_peer: Option<String>,
    pub bandwidth_bytes_per_sec: u64,
}
```

- Number of peers currently connected
- Last peer with a successful sync
- Bandwidth usage (bytes per second)

## Source Device

```rust
pub struct SourceDeviceDiagnostics {
    pub cpu_cores: usize,
    pub active_threads: usize,
    pub memory_usage_bytes: u64,
    pub disk_free_bytes: u64,
}
```

- CPU core count
- Thread count
- Memory and disk stats
- Essential for mobile diagnostics

## Merkle Index

```rust
pub struct MerkleIndexDiagnostics {
    pub indexing_latency_ms: u64,
    pub flush_latency_ms: u64,
    pub page_write_latency_ms: u64,
    pub proofs_generated: u64,
    pub last_proof_verified: Option<u64>,
}
```

- Merkle tree update and flush latencies
- Page write latency
- Proof generation count
- Last proof verification timestamp

## RocksDB Storage

```rust

pub struct RocksDBDiagnostics {
    pub storage_latency_ms: u64,
    pub write_batches: u64,
}

```

- Average write latency
- Number of write batches committed

## Write-Ahead Log (WAL)

```rust
pub struct WalDiagnostics {
    pub wal_write_latency_ms: u64,
    pub wal_flush_latency_ms: u64,
}
```

- WAL write latency
- WAL flush latency

## 🛠️ License

This module is part of the XaeroFlux engine and is released under the MPL-2.0 license.

⸻

## 👥 Contributing

Contributions to enhance observability, metrics granularity, or probing security are welcome. Please open an issue or PR.

⸻

## 🌐 Maintainers

This module is maintained by the BlockXaero organization.
