# Xaeroflux

 ⚠️ **Work in progress - NOT READY FOR PRODUCTION USE** ⚠️

## Introduction

Xaeroflux is a decentralized, append-only storage and indexing engine optimized for cloud-less, peer-to-peer social networks. It runs on iOS, Android, and desktop, using minimal threads and zero-copy I/O for mobile-friendly performance. Key features:

1. **Privacy-focused** storage engine  
2. **Pure P2P** architecture with a pluggable networking layer  
3. **Event-centric** design for managing all interactions  
4. **Merkle-indexed** data for efficient exchange  
5. **Mobile-tuned**, lightweight library

## Basic Architecture

### 1. Serialization & Deserialization

- **Zero-copy reads** with [rkyv]  
- **Zero-copy writes** with [bytemuck] when possible  
- **De-serialize only when absolutely necessary**

### 2. Storage Layer

- **LMDB** (liblmdb) for append-only event storage  
  - Simple mmap-paged file I/O  
  - Extremely lightweight and fast  
- **Two-tier Merkle indexing**  
  - Global Merkle tree over page-root hashes  
  - Fixed-size pages (16 KiB) serialized on disk  
- **Fast, archived-only reads** via `rkyv` or zero-copy with `bytemuck`

### 3. Networking Layer

- **P2P sync** with libp2p  
- **Merkle diffing** for minimal data exchange  
- **Resilient** — “never-say-die” networking  
- **Privacy-first**: QUIC + TLS, fallback to Tor  
- **Anonymous mode** over Tor

### 4. Plumbing

- Intra-process comms via **crossbeam** channels  
- **Native threads** (thread-pool)  
- Event-driven I/O with **MIO** + **mmap**

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