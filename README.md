# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

## Introduction

Xaeroflux is a decentralized, append-only storage and indexing engine for cloudless, peer-to-peer social networks. It runs on iOS, Android, and desktop, using minimal threads and zero-copy I/O for mobile-friendly performance. We are:

1. Privacy focussed storage engine
2. Pure Peer to Peer with pluggable networking layer.
3. De-Centralized social network friendly architecture with Events as
first class citizens for managing everything.
4. Merkle Indexed setup for efficient data exchange.
5. Tuned for Mobile and very tight library (aspire to).

## Basic Architecture

1. *Serialization  and deserialization*:
  a. Zero Copy `rkyv` library oriented reads
  b. Zero Copy `bytemuck` based writes when possible.
  c. Never de-serialize until absolutely necessary.

2. Storage Layer:

  a. LMDB (liblmdb) for append only event store, simple mmap paged file disk io, extremely lightweight and fast.
  b. Two Tier merkle indexes: Merkle Index with leaves consisting of page root hashes
  and each page fitting fixed nodes (16 KB page size assuming iOS or Android) as pages serialized on disk.
  c. Fast reads in either case with archived only reads (rkyv) or bytemuck when possible.

3. Networking Layer:

  a. P2P sync with libp2p.
  b. Merkle Indexing used for efficient diffing and exchanging data between peers.
  c. Resilient - never say die networks.
  d. Privacy focussed: QUIC with TLS or IPv6 - if either fail, uses Tor.
  e. Anonymous mode for Tor based storage and networking.

4.Plumbing:
  a. Most of plumbing is using crossbeam channels,
  b. native threads
  c. MIO + mmap

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