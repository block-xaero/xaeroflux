//! Application configuration definitions for xaeroflux.
//!
//! This module defines the configuration structures for:
//! - Global `Config`
//! - Append-only file settings (`AofConfig`)
//! - Storage engine parameters (`StorageConfig`)
//! - Merkle index tuning (`MerkleConfig`)
//! - Peer-to-peer networking (`P2PConfig` and `DiscoveryConfig`)
//! - Buffering strategies (`BufferConfig`)
//! - Thread pool sizing (`ThreadConfig`)
//! - Logging preferences (`LoggingConfig`)

use std::{collections::HashMap, path::PathBuf};

use serde::Deserialize;

/// Top-level application configuration.
///
/// Aggregates all sub-configuration for various components:
/// general metadata, AOF, storage, Merkle, P2P, buffers, threading, and logging.
#[derive(Deserialize, Debug, Default)]
pub struct Config {
    pub name: String,
    pub version: u64,
    pub description: String,
    pub aof: AofConfig,
    pub storage: StorageConfig,
    pub merkle: MerkleConfig,
    pub p2p: P2PConfig,
    pub event_buffers: HashMap<String, BufferConfig>,
    pub threads: ThreadConfig,
    pub logging: LoggingConfig,
}

/// Configuration for the append-only file (AOF) subsystem.
///
/// Controls file paths, flush intervals, size limits, compression, retention,
/// threading, and buffer sizing for durable event storage.
#[derive(Deserialize, Debug, Default)]
pub struct AofConfig {
    pub enabled: bool,
    pub file_path: PathBuf,
    pub flush_interval_ms: u64,
    pub max_size_bytes: usize,
    pub compression: String,
    pub retention_policy: String,
    pub threads: ThreadConfig,
    pub buffer_size: usize,
}

/// Settings for the storage engine directories and file handle limits.
///
/// Includes data, write-ahead log, and Merkle index directories,
/// and options for creation and open file limits.
#[derive(Deserialize, Debug, Default)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub merkle_index_dir: PathBuf,
    pub create_if_missing: bool,
    pub max_open_files: u32,
}

/// Tuning parameters for the Merkle mountain range index.
///
/// Defines page size, flush periodicity, and maximum nodes per page.
#[derive(Deserialize, Debug, Default)]
pub struct MerkleConfig {
    pub page_size: usize,
    pub flush_interval_ms: u64,
    pub max_nodes_per_page: usize,
}

/// Peer-to-peer networking configuration.
///
/// Specifies listen address, bootstrap nodes, mDNS usage,
/// CRDT synchronization strategy, message size limits, and discovery options.
#[derive(Deserialize, Debug, Default)]
pub struct P2PConfig {
    pub listen_address: String,
    pub bootstrap_nodes: Vec<String>,
    pub enable_mdns: bool,
    pub crdt_strategy: String,
    pub max_msg_size_bytes: usize,
    pub discovery_config: DiscoveryConfig,
}
/// Settings for peer discovery mechanisms.
///
/// Toggles for WiFi (mDNS), Bluetooth, and geolocation-based discovery.
#[derive(Deserialize, Debug, Default)]
pub struct DiscoveryConfig {
    pub wifi: bool,
    pub bluetooth: bool,
    pub geolocate: bool,
}

/// Configuration for event buffer behavior.
///
/// Includes capacity, batch size, and timeout for buffered operations.
#[derive(Deserialize, Debug, Default)]
pub struct BufferConfig {
    pub capacity: usize,
    pub batch_size: usize,
    pub timeout_ms: u64,
}

/// Thread pool sizing and queue limits.
///
/// Controls number of worker and I/O threads, max queue size, and thread pinning.
#[derive(Deserialize, Debug, Default)]
pub struct ThreadConfig {
    pub num_worker_threads: usize,
    pub num_io_threads: usize,
    pub max_queue_size: usize,
    pub pin_threads: bool,
}

/// Logging preferences including verbosity level and output file path.
#[derive(Deserialize, Debug, Default)]
pub struct LoggingConfig {
    pub level: String,
    pub file: PathBuf,
}
