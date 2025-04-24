# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

Xaeroflux is a decentralized, append-only storage and indexing engine for cloudless, peer-to-peer social networks. It runs on iOS, Android, and desktop, using minimal threads and zero-copy I/O for mobile-friendly performance.

## Table of Contents

- [Key Features](#key-features)
- [Repository Layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the Daemon](#running-the-daemon)
  - [Library Usage](#library-usage)
- [Architecture Overview](#architecture-overview)
- [Data Formats](#data-formats)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Key Features

- **Append-only Page Store (16 KB pages)**
  - Fixed-size, C-layout pages (`PAGE_SIZE = 16_384` bytes) of exactly **408** `XaeroOnDiskNode` entries (40 bytes each) plus a 36‑byte header.
  - Zero-copy I/O via **bytemuck** (`Pod`+`Zeroable`) for in-place `mmap` reads/writes and **rkyv** (`Archive`) for zero-copy in-memory views.

- **Merkle Tree Indexing**
  - One leaf per page (the page-subtree root). Global index maintained by `XaeroMerkleTree` with **O(log N)** inserts/updates.
  - Supports diffs, inclusion proofs, and reconciliation via Merkle-broadcast gossip.

- **Efficient Event Pipeline**
  - **Reader**, **PageStore**, **MerkleIndexer**, and **GossipSync** actors connected by `crossbeam::channel`.
  - `EventListener` abstraction with bounded inbox, configurable thread-pool, and panic-catch guard.
  - Platform-specific thread-count override (single-threaded on iOS/Android via `cfg!(target_os)`).

- **Persistent Write-Ahead Log (WAL)**
  - RocksDB for raw event persistence (column families, batched writes).
  - `merkle_index.bin` for page storage via `mmap` + `mlock` + `madvise` (or `posix_fadvise`).

- **Decentralized P2P Sync**
  - Gossip protocol over a DHT: exchange Merkle roots, compute missing page indices, fetch only needed pages from peers, and verify with inclusion proofs.
  - Append-only, immutable pages ensure monotonic convergence across peers.

---

## Repository Layout

```
core : Event buffering, event listeners, append only file event log.
indexing: merkle tree, merkle tree on-disk storage, diffing capability - page store for prefetching pages and page index offsets.
networking: libp2p network behaviors, DHT peer discovery, QUIC support.
logs: WAL logging, diagnostics
```

---

## Prerequisites

- **Rust** (stable channel, 1.70+ recommended)
- **RocksDB** system library (e.g. `librocksdb-dev` on Debian/Ubuntu)
- **POSIX-compatible OS** for `mmap`/`madvise` (Linux, macOS). Windows support is planned.

---

## Installation

```bash
git clone https://github.com/block-xaero/xaeroflux.git
cd xaeroflux
# Run tests (requires local RocksDB)
cargo test
# Build release binary
cargo build --release
```

---

## Configuration

Configuration is driven by environment variables or a config file under `~/.config/xaeroflux/config.toml` (TBD). Key settings:

- `XAERO_THREADS`: override default worker thread count
- `XAERO_PAGE_STORE`: path to `merkle_index.bin`
- `RUST_LOG`: logging level (e.g. `info`, `debug`)

---

## Usage

### Running the Daemon

```bash
RUST_LOG=info cargo run --bin xaeroflux-daemon -- \
  --db-path ./data/events.db \
  --page-path ./data/merkle_index.bin
```

This starts the Reader, PageStore, MerkleIndexer, and GossipSync services in one process.

### Library Usage

You can embed Xaeroflux in your own Rust app:

```rust
use xaeroflux::core::Event;
use xaeroflux::storage::PageStore;
use xaeroflux::indexing::merkle_tree::XaeroMerkleTree;

// Initialize storage
let ps = PageStore::new("pages.bin").unwrap();

// Write a user post as a page
let page_data: Vec<[u8;32]> = ...; // Merkle-subtree hashes
ps.write_page(page_index, page_data);

// Insert root into global tree
let mut tree = XaeroMerkleTree::neo(vec![]);
tree.insert_root(page_root_hash);
```

---

## Architecture Overview

1. **EventListener**: generic event dispatcher with bounded inbox, thread-pool, and panic isolation.
2. **Reader Actor**: deserializes raw event pages from RocksDB, emits `PageData` events.
3. **PageStore Actor**: single-threaded, owns `MmapMut`, LRU cache, writes pages to disk and caches them.
4. **MerkleIndexer Actor**: listens for page-root events, updates global `XaeroMerkleTree`, emits new root for gossip.
5. **GossipSync Actor**: DHT-backed gossip of Merkle roots, computes diff with peers, and requests missing pages.

---

## Data Formats

### On-disk page (16 KB)

```c
struct XaeroOnDiskPage {
  char marker[4];      // "XAER"
  uint8_t event_type;
  uint8_t _pad1[3];
  uint64_t version;
  uint64_t leaf_start;
  uint64_t total_nodes;
  XaeroOnDiskNode nodes[408];
  uint8_t _pad2[32];
};
```

- `XAER` magic, 1‑byte tag, 36‑byte header, 408 × 40B nodes, 32B padding.
- Zero‐copy via `bytemuck` & `rkyv`.

### Archived types

- **`XaeroOnDiskPage`** derives `#[derive(Archive)]` for zero-copy in-memory reads with `archived_root`.
- **`XaeroMerkleNode`**, **`XaeroMerkleProof`**, **`XaeroMerkleTree`** derive `Archive` for serializing dynamic structures.

---

## Testing

```bash
# Unit and integration tests
cargo test --all
# Lint
cargo clippy --all -- -D warnings
# Format
cargo fmt --all
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repo
2. Create a feature branch
3. Open a PR with tests and documentation

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Mozilla Public License Version 2.0 See [LICENSE](LICENSE) for details.

