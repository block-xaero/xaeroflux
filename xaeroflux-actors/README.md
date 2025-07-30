# XaeroFlux

Offline-first P2P storage engine in Rust.

## Architecture

- **Ring Buffers**: Lock-free, T-shirt sized (XS/S/M/L/XL), cache-aligned
- **Storage**: LMDB + Merkle trees, content-addressed with Blake3
- **Identity**: XaeroID (Falcon-512 + ZK proofs)
- **Networking**: Iroh (QUIC + gossipsub), DHT discovery
- **Vector Search**: HNSW indexing with configurable extractors

## Design Principles

- Zero allocation hot paths
- POD-first with bytemuck
- `#[repr(C, align(64))]` structs
- Actor model with bounded collections

## Usage

```rust
use xaeroflux::XaeroFlux;
use xaeroid::XaeroID;

let mut xf = XaeroFlux::new();

// Start AOF persistence
xf.start_aof()?;

// Start P2P networking  
xf.start_p2p(my_xaero_id)?;

// Send data
xf.send_text("Hello world")?;
xf.send_file_data(&file_bytes)?;
```

## Ring Buffer Sizes

| Size | Capacity | Event Size |
|------|----------|------------|
| XS   | 1024     | 64 bytes   |
| S    | 1024     | 256 bytes  |
| M    | 512      | 1024 bytes |
| L    | 256      | 4096 bytes |
| XL   | 128      | 16384 bytes|

## P2P Protocol

1. Vector clock synchronization
2. Event discovery and exchange
3. File transfer
4. Audio/video streaming

## Storage Format

- Events: Append-only log in LMDB
- Indexing: Hash tables + vector search
- Merkle trees: Content verification
- Vector clocks: Causal ordering

## Build

```bash
cargo build --release
```

## Test

```bash
cargo test
```

## License

MPL-2.0