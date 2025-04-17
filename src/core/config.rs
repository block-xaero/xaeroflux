use std::{collections::HashMap, path::PathBuf};

use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub name: String,
    pub version: u64,
    pub description: String,
    pub storage: StorageConfig,
    pub merkle: MerkleConfig,
    pub p2p: P2PConfig,
    pub event_buffers: HashMap<String, BufferConfig>,
    pub threads: ThreadConfig,
    pub logging: LoggingConfig,
}

#[derive(Deserialize, Debug)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub merkle_index_dir: PathBuf,
    pub create_if_missing: bool,
    pub max_open_files: u32,
}

#[derive(Deserialize, Debug)]
pub struct MerkleConfig {
    pub page_size: usize,
    pub flush_interval_ms: u64,
    pub max_nodes_per_page: usize,
}

#[derive(Deserialize, Debug)]
pub struct P2PConfig {
    pub listen_address: String,
    pub bootstrap_nodes: Vec<String>,
    pub enable_mdns: bool,
    pub crdt_strategy: String,
    pub max_msg_size_bytes: usize,
}

#[derive(Deserialize, Debug)]
pub struct BufferConfig {
    pub capacity: usize,
    pub batch_size: usize,
    pub timeout_ms: u64,
}

#[derive(Deserialize, Debug)]
pub struct ThreadConfig {
    pub num_worker_threads: usize,
    pub pin_threads: bool,
}

#[derive(Deserialize, Debug)]
pub struct LoggingConfig {
    pub level: String,
    pub file: PathBuf,
}
