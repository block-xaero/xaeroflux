# Xaeroflux

Xaeroflux is a decentralized storage and indexing engine designed for cloudless, peer-to-peer social networks. It leverages RocksDB for efficient event storage, a Merkle tree-based indexing mechanism for data integrity and diffing, and uses asynchronous, minimal-threaded designs for mobile-friendly performance on platforms like iOS and Android.

> **Note:** This is an evolving codebase currently under active development. It includes experimental features and a developing test suite. See the [issues](https://github.com/block-xaero/xaeroflux/issues) section for known limitations and planned improvements.

## Features

- **Decentralized P2P Sync:**  
  Supports cloudless peer-to-peer communication where each group maintains its own DHT for peer and content discovery.

- **Efficient Event Storage:**  
  Utilizes RocksDB with a dedicated column family for event logs. Events are stored as raw `u8` bytes, then decoded and indexed.

- **Merkle Tree Indexing:**  
  Events are indexed via a Merkle tree, where each page (e.g., 16 KB pages) holds 512 nodes. This allows for fast and secure diffing between peers.

- **Concurrency with Crossbeam Channels:**  
  An internal pipeline is implemented to handle raw event ingestion, decoding, indexing, and advertisement using lightweight worker threads.

- **Write-Ahead Logging (WAL):**  
  Maintains data integrity via a WAL file, updated through memory-mapped I/O and Mio for non-blocking operations.

- **Modular & Extensible Architecture:**  
  Components are separated into clear modules for core logic, indexing, storage metadata, and logging, ensuring ease of development and testing.

## Project Structure

- **`src/core/`**  
  Contains the central components:
  - `mod.rs`: Core logic and application state management.
  - `storage_meta.rs`: Definitions and traits for storage metadata, including header and footer structures.
  - `swimming.rs`: Experimental or utility modules (naming hinting at dynamic behaviors in data processing).

- **`src/indexing/`**  
  Contains the indexing logic for the Merkle tree:
  - `merkle_tree.rs`: Implements the Merkle tree data structure, including node and tree operations.
  - `storage.rs`: Handles storage-specific functions and page management (e.g., mmapped file segmentation).

- **`src/logs/`**  
  Contains logging and diagnostic helpers:
  - `mod.rs`: Central logging configuration.
  - `probe.rs`: Tools for performance probing and debugging.

## Installation

### Prerequisites

- Rust (stable channel recommended)
- [RocksDB](https://github.com/facebook/rocksdb) library (for local development and testing)
- A POSIX-compliant operating system (for mmap and Mio usage; adaptations might be needed for Windows)

### Build

Clone the repository:

```bash
git clone https://github.com/block-xaero/xaeroflux.git
cd xaeroflux
