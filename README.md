# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

## Introduction

Xaeroflux is a mobile-friendly, decentralized append-only storage and indexing engine powering truly cloud-less, peer-to-peer applications (e.g. social networks, marketplaces, identity systems). It combines:

1. **Event-first design**: every change is an append-only Event
2. **Two-tier Merkle-indexing**:
   * **Per-page Merkle trees** for efficient bulk page proofs
   * **Merkle Mountain Range (MMR)** for instant per-event proofs and low-latency diffs
3. **Plug-and-play networking**: transport-agnostic sync protocol over Libp2p/QUIC/WebRTC/Tor
4. **Mobile-optimized I/O**: zero-copy serialization, mmap-backed pages, minimal threads
5. **Privacy & resilience**: end-to-end encryption, fallback to Tor, never-say-die mesh sync

---

## Quickstart

Open and use Xaeroflux just like a local embedded database—background sync happens automatically via **iroh**, **iroh-blob**, and **iroh-gossip**:

```rust
use xaeroflux::Core;

// NOTE THIS CODE IS A POSSIBLE EXAMPLE OF HOW IT MAY FUNCTION
// THIS IS STILL EVOLVING!
fn main() -> anyhow::Result<()> {
    // initialize and open database (background sync starts automatically)
    let mut core = Core::builder()
        .with_page_size(16 * 1024)
        .with_segment_pages(1024)
        .with_sync_backends(&["iroh", "iroh-blob", "iroh-gossip"] )
        .open("/path/to/xaeroflux.db")?;

    // append a new Event payload
    let ev = core.new_event(b"Hello, Xaeroflux!");
    core.append(ev);

    // read back all events like a local DB
    for event in core.iter_events() {
        println!("Event: {:?}", event.data);
    }

    // get current MMR root for diffing or audit
    let root = core.mmr_root();
    println!("MMR root: {}", hex::encode(root));

    // Core runs sync in background; just exit when done
    Ok(())
}
```

---

## Architecture Overview

### 1. Serialization

* **Zero-copy reads** via \[rkyv]
* **Zero-copy writes** via \[bytemuck] where possible
* Deserialize only on demand

### 2. Storage Layer

* **Append-Only Event Log (AOF)**

  * Backed by **LMDB**: fast, B+-tree mmap pages for raw Event bytes
  * Each `Event` is serialized with a small header and archived bytes

* **Segment Writer Actor**

  * Buffers Events in a thread-safe channel (crossbeam)
  * Packs into a fixed-size page buffer (e.g. 16 KiB)
  * `mmap.flush_range` per-page, rolls over after *N* pages into new `.seg` file

* **Per-Page Merkle Trees**

  * On each full page: build a Merkle tree over its event-hashes
  * Store page’s nodes in a `XaeroOnDiskMerklePage` blob alongside raw bytes
  * Enables O(log P) proofs within any page

* **Merkle Mountain Range (MMR)**

  * Immediate per-event root updates
  * Append: push leaf, carry-merge equal-height peaks → update in-memory peaks + `root`
  * Persist peaks/root metadata so you can resume on restart
  * Enables instant O(log E) inclusion proofs and minimal diffing

### 3. Networking Layer

* **Xaeroflux Sync Protocol (XSP)** over any bidirectional byte stream

  1. **HELLO** handshake (capabilities: SYNC, CRDT, MCP…)
  2. **PEAKS ↔ DIFF**: peer exchanges MMR peak-roots
  3. **PAGE\_REQUEST / PAGE\_RESPONSE** for missing pages
  4. **SYNC\_DONE**
* Built on **Iroh** which is based on **Libp2p** (QUIC, TCP, WebRTC)
* Uses Arti Tor for completely anonymous mode!
* Optional **CRDT** and **MCP (Model Context Protocol)** integrations

### 4. Plumbing & Runtime

* **Intra-process comms**: crossbeam channels, thread-pool handlers
* **I/O**: MIO + `mmap` for non-blocking page watches and efficient flushes
* **Threads**: native OS threads (no async/await baggage)

---

## Testing & Quality

```bash
# Run all tests
cargo test --all

# Enforce code style
cargo fmt --all
cargo clippy --all -- -D warnings
```

---

## Contributing

1. Fork the repo
2. Create a feature branch
3. Add tests & documentation
4. Open a PR and iterate

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

Mozilla Public License Version 2.0
See [LICENSE](LICENSE) for full text.
