# XaeroFlux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

XaeroFlux is an offline-first peer-to-peer storage engine built in Rust with lock-free ring buffers, cryptographic identity management, and content-addressed storage. It enables building distributed applications that work seamlessly offline and sync when peers connect.

## Table of Contents

1. [Introduction](#introduction)
2. [Core Architecture](#core-architecture)
3. [Memory Architecture](#memory-architecture)
4. [Getting Started](#getting-started)
5. [Event Bus API](#event-bus-api)
6. [P2P Networking](#p2p-networking)
7. [Identity & Security](#identity--security)
8. [Storage & Persistence](#storage--persistence)
9. [Vector Search](#vector-search)
10. [System Integration](#system-integration)
11. [Testing](#testing)
12. [Contributing](#contributing)
13. [License](#license)

## Introduction

### What is XaeroFlux?

XaeroFlux is a **distributed storage engine** that enables building offline-first applications where multiple peers can store and sync data without requiring central servers. Think distributed databases, but optimized for peer-to-peer networks with cryptographic security.

### Key Features

- **🔄 Lock-Free Ring Buffers**: Zero-allocation hot paths with cache-aligned structures
- **🌐 P2P Native**: Direct peer discovery and sync via Iroh QUIC networking
- **🔐 Cryptographically Secure**: Falcon-512 post-quantum signatures with ZK proofs
- **💾 Content-Addressed Storage**: LMDB + Merkle trees with Blake3 hashing
- **📱 Offline-First**: Works without internet, syncs when peers reconnect
- **⚡ High Performance**: T-shirt sized event pools (XS/S/M/L/XL) for optimal memory usage
- **🔍 Vector Search**: Built-in HNSW indexing with custom extractors
- **🧠 Memory Efficient**: Stack-based allocation with predictable memory patterns
- **🔗 MMR Indexing**: Merkle Mountain Range for efficient sync and proofs

## Core Architecture

XaeroFlux is built around five main concepts:

### 1. Event Bus - Lock-Free Event Distribution
A lock-free event distribution system using ring buffers:

```rust
use xaeroflux_actors::XaeroFlux;

let mut xf = XaeroFlux::new();
xf.start_aof()?;

// Write events to optimal ring buffer
xf.write_event(b"my data", 42)?;
```

### 2. Ring Buffer Pools - T-Shirt Sized Memory
Events are allocated into size-optimized ring buffers:

```rust
// XS (64B), S (256B), M (1KB), L (4KB), XL (16KB)
// Data automatically routed to optimal size bucket
```

### 3. P2P Networking - Direct Peer Sync
Cryptographically verified peer-to-peer synchronization:

```rust
use xaeroflux_actors::networking::p2p::P2PActor;

let p2p = P2PActor::spawn(topic, our_xaero_id).await?;
```

### 4. Content-Addressed Storage - Immutable Events
All events are content-addressed with Blake3 hashing:

```rust
// Events stored by hash for deduplication and integrity
// LMDB provides fast O(1) lookups by content hash
```

### 5. Vector Search - Semantic Indexing
Built-in HNSW vector search with custom extractors:

```rust
let mut extractors = HashMap::new();
extractors.insert(42, Box::new(MyVectorExtractor));

xf.start_vector_search(extractors, 128, 16, 1000, 16, 200)?;
let results = xf.search_vector(vec![0.1, 0.2, 0.3], 10, 0.8)?;
```

## Memory Architecture

XaeroFlux features a sophisticated lock-free memory architecture built on ring buffer pools:

### Ring Buffer Architecture

- **T-Shirt Sized Pools**: XS (64B), S (256B), M (1KB), L (4KB), XL (16KB)
- **Zero-Copy Access**: Events remain in ring buffers throughout processing
- **Cache-Aligned**: `#[repr(C, align(64))]` structs for optimal CPU cache usage
- **Lock-Free**: Single writer, multiple readers with atomic operations
- **Bounded Collections**: Predictable memory usage for embedded systems

### Event Structure

```rust
pub struct PooledEvent<const SIZE: usize> {
    pub data: [u8; SIZE],           // Fixed-size data buffer
    pub len: u32,                   // Actual data length
    pub event_type: u32,            // Event type identifier
}

// Zero-copy access methods
impl<const SIZE: usize> PooledEvent<SIZE> {
    pub fn data(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}
```

### Memory Benefits

- **Zero Allocations**: Events use pre-allocated ring buffer slots
- **Cache Friendly**: Sequential memory layout improves performance
- **Thread Safe**: Lock-free single-writer, multiple-reader design
- **Bounded Memory**: Predictable memory usage for resource-constrained environments
- **Fast Serialization**: Direct memory mapping to storage

### Ring Buffer Management

```rust
// EventBus automatically routes to optimal ring buffer
let mut xf = XaeroFlux::new();

// Small data -> XS ring buffer
xf.write_event(b"small", 1)?;

// Large data -> appropriate sized ring buffer  
xf.write_event(&vec![0u8; 2048], 2)?; // -> L ring buffer
```

## Getting Started

### Basic Usage

```rust
use xaeroflux_actors::XaeroFlux;

// 1. Initialize XaeroFlux
let mut xf = XaeroFlux::new();

// 2. Start storage engine
xf.start_aof()?;

// 3. Write events - that's it! P2P sync happens automatically
xf.write_event(b"hello world", 42)?;
xf.write_event(b"user logged in", 1)?;
xf.write_event(&document_data, 100)?;

// 4. Optional: Start vector search for semantic queries
let mut extractors = HashMap::new();
extractors.insert(42, Box::new(TextVectorExtractor));
xf.start_vector_search(extractors, 128, 16, 1000, 16, 200)?;

// 5. Search semantically similar content
let results = xf.search_vector(vec![0.1, 0.2, 0.3], 10, 0.8)?;
```

### That's It!

XaeroFlux automatically handles:
- ✅ **Storage**: Events persisted to LMDB
- ✅ **Indexing**: MMR and vector search indexing
- ✅ **P2P Sync**: Background synchronization with peers
- ✅ **Offline Support**: Works without network, syncs when available

## Event Bus API

The EventBus provides lock-free event distribution using ring buffer pools:

### Writing Events

```rust
let mut xf = XaeroFlux::new();

// EventBus automatically selects optimal ring buffer size
xf.write_event(b"xs data", 1)?;        // -> XS ring (64B)
xf.write_event(&vec![0u8; 500], 2)?;   // -> S ring (256B)  
xf.write_event(&vec![0u8; 2000], 3)?;  // -> M ring (1KB)
```

### Direct Ring Buffer Access

```rust
use xaeroflux_actors::{EventBus, P2PRingAccess};

// Local event bus (EventBus writes, AOF/VectorSearch read)
let mut bus = EventBus::new();
bus.write_optimal(b"my data", 42)?;

// P2P ring buffers (P2P actors write, EventBus reads)
let mut p2p_writer = P2PRingAccess::m_writer();
p2p_writer.add(my_pooled_event);
```

### Event Types

```rust
// Define your event types
const USER_LOGIN: u32 = 1;
const USER_LOGOUT: u32 = 2;
const DOCUMENT_EDIT: u32 = 3;
const CHAT_MESSAGE: u32 = 4;

// Write typed events
xf.write_event(user_data, USER_LOGIN)?;
xf.write_event(edit_data, DOCUMENT_EDIT)?;
```

## P2P Networking

XaeroFlux automatically handles peer-to-peer synchronization in the background. Users don't need to manage P2P connections directly.

### Transparent Background Sync

```rust
use xaeroflux_actors::XaeroFlux;

// Just write events normally
let mut xf = XaeroFlux::new();
xf.start_aof()?;

xf.write_event(b"user message", CHAT_MESSAGE)?;
xf.write_event(b"document edit", DOCUMENT_EDIT)?;

// XaeroFlux automatically:
// ✅ Stores events locally in LMDB
// ✅ Discovers peers on your network
// ✅ Syncs events with other XaeroFlux instances
// ✅ Handles identity verification
// ✅ Resolves conflicts via MMR proofs
```

### How P2P Works Behind the Scenes

XaeroFlux uses a sophisticated background system:

1. **Automatic Peer Discovery**: Finds other XaeroFlux instances via topics
2. **Identity Verification**: Uses XaeroID for cryptographic peer authentication
3. **Event Synchronization**: Exchanges XaeroPeerEvent between verified peers
4. **Conflict Resolution**: MMR (Merkle Mountain Range) ensures consistency
5. **Offline Resilience**: Queues events when offline, syncs when reconnected

### Network Architecture

```
Your App
    ↓ xf.write_event()
XaeroFlux EventBus
    ↓ (automatic background processing)
┌─────────────────────────────────────┐
│ P2P Actor (transparent)             │
│ - Peer discovery via topics         │  
│ - XaeroID identity verification     │
│ - Event sync via QUIC               │
│ - Conflict resolution via MMR       │
└─────────────────────────────────────┘
    ↓ ↑ (network sync)
Other XaeroFlux Peers
```

### Topic-Based Discovery

Events are automatically categorized by topics for efficient peer discovery:

```rust
// Events naturally group by application context
xf.write_event(chat_data, CHAT_MESSAGE)?;     // Auto-syncs with chat peers
xf.write_event(doc_data, DOCUMENT_EDIT)?;     // Auto-syncs with doc peers
xf.write_event(game_data, GAME_STATE)?;       // Auto-syncs with game peers

// No manual peer management required!
```

## Identity & Security

XaeroFlux uses post-quantum cryptographic identities with ZK proof capabilities:

### XaeroID Structure

```rust
use xaeroid::XaeroID;

// Create new identity
let identity = XaeroID::new()?;

// Post-quantum signatures (Falcon-512)
let signature = identity.sign(b"data to sign")?;
let is_valid = identity.verify(b"data to sign", &signature)?;

// Zero-knowledge proofs (for future credential systems)
let proof = identity.prove_identity(&challenge)?;
```

### Identity in P2P

```rust
// P2P actors verify peer identities before accepting events
impl P2PActorState {
    pub fn verify_peer_response(&self, peer_id: NodeId, xaero_id: XaeroID, signature: &[u8]) -> Result<bool> {
        let pubkey = &xaero_id.did_peer[..xaero_id.did_peer_len as usize];
        let is_valid = XaeroProofs::verify_identity(pubkey, &challenge, signature);
        // Only verified peers can send/receive events
    }
}
```

### Security Features

- **Post-Quantum Safe**: Falcon-512 signatures resist quantum attacks
- **Identity-Based**: All events cryptographically attributed to authors
- **Challenge-Response**: Peers must prove identity ownership
- **Zero-Knowledge Ready**: Framework for privacy-preserving credentials
- **Content Addressing**: Blake3 hashing ensures data integrity

## Storage & Persistence

XaeroFlux provides high-performance persistent storage with cryptographic integrity:

### LMDB Storage

```rust
// All events automatically persisted to LMDB
// Content-addressed with Blake3 hashing
// Hash index for O(1) lookups

// Events stored in size-specific databases:
// - XS events -> xs_events table
// - S events -> s_events table  
// - etc.
```

### MMR Indexing

```rust
// Merkle Mountain Range for efficient sync
// Each event added to MMR for cryptographic proofs
// Enables efficient peer synchronization

impl AofState {
    pub fn get_mmr_root(&self) -> [u8; 32];
    pub fn get_mmr_proof(&self, leaf_index: usize) -> Option<MerkleProof>;
    pub fn verify_mmr_proof(&self, leaf_hash: [u8; 32], proof: &MerkleProof) -> bool;
}
```

### Hash Index Performance

```rust
// Fast event lookup by content hash
// O(1) retrieval vs O(N) scan
let events = aof_state.get_events_for_leaf_hashes(&target_hashes)?;

// Before: Full table scan for each hash
// After: Direct hash index lookup
```

### Storage Features

- **Content-Addressed**: Automatic deduplication via Blake3 hashing
- **ACID Transactions**: LMDB provides transaction safety
- **Memory-Mapped**: Efficient access to large datasets
- **Crash Recovery**: Automatic recovery from incomplete writes
- **Incremental Sync**: MMR enables efficient peer synchronization

## Vector Search

Built-in semantic search using HNSW (Hierarchical Navigable Small World) indexing:

### Custom Vector Extractors

```rust
use xaeroflux_actors::VectorExtractor;

struct TextVectorExtractor;

impl VectorExtractor for TextVectorExtractor {
    fn extract_vector(&self, event_data: &[u8]) -> Option<Vec<f32>> {
        // Convert text to embeddings
        let text = String::from_utf8_lossy(event_data);
        Some(text_to_embeddings(&text))
    }
}

// Register extractor for specific event types
let mut extractors = HashMap::new();
extractors.insert(CHAT_MESSAGE, Box::new(TextVectorExtractor));
extractors.insert(DOCUMENT_EDIT, Box::new(DocumentVectorExtractor));
```

### Vector Search API

```rust
// Start vector search with extractors
xf.start_vector_search(
    extractors,     // Custom extractors by event type
    128,            // Vector dimension
    16,             // Max connections per node
    1000,           // Max elements in index
    16,             // Max layer
    200,            // EF construction
)?;

// Search semantically similar events
let query_vector = vec![0.1, 0.2, 0.3, /* ... 128 dimensions */];
let results = xf.search_vector(
    query_vector,
    10,     // k=10 results
    0.8,    // similarity threshold
)?;

// Search with multiple vectors
let vectors = vec![vector1, vector2, vector3];
let all_results = xf.search_vectors(vectors, 5, 0.7)?;
```

### Search Features

- **Real-Time Indexing**: Events automatically indexed as they arrive
- **Custom Extractors**: Define how to convert events to vectors
- **High Performance**: HNSW provides sub-linear search time
- **Similarity Filtering**: Configurable similarity thresholds
- **Batch Search**: Search multiple vectors efficiently

## System Integration

### Complete System Startup

```rust
use xaeroflux_actors::XaeroFlux;
use std::collections::HashMap;

// 1. Initialize XaeroFlux
let mut xf = XaeroFlux::new();

// 2. Start storage engine (includes automatic P2P sync)
xf.start_aof()?;

// 3. Setup vector search with custom extractors (optional)
let mut extractors = HashMap::new();
extractors.insert(CHAT_MESSAGE, Box::new(ChatExtractor));
extractors.insert(DOCUMENT_TEXT, Box::new(DocumentExtractor));
extractors.insert(USER_PROFILE, Box::new(ProfileExtractor));

xf.start_vector_search(extractors, 256, 16, 10000, 16, 200)?;

// 4. Your application is ready!
// Just write events - everything else is automatic:

xf.write_event(b"user joined", USER_LOGIN)?;
xf.write_event(b"message: hello", CHAT_MESSAGE)?;
xf.write_event(&document_bytes, DOCUMENT_EDIT)?;

// XaeroFlux automatically handles:
// ✅ Local storage in LMDB
// ✅ MMR indexing for integrity
// ✅ Vector search indexing
// ✅ P2P peer discovery 
// ✅ Background sync with peers
// ✅ Conflict resolution
// ✅ Offline queue and reconnection
```

### Event Flow Architecture

```
Application
    ↓ write_event() - Just write events!
EventBus (lock-free ring buffers)
    ↓ Automatic background processing
┌─────────────────────────────────────┐
│ AOF Actor: persist to LMDB          │
│ MMR Actor: cryptographic proofs     │
│ Vector Search: semantic indexing    │
│ P2P Actor: peer sync (transparent)  │  ← Users never see this
└─────────────────────────────────────┘
    ↓ ↑ (automatic network sync)
Other XaeroFlux Instances
```

### No P2P Management Required

Unlike traditional P2P systems, XaeroFlux requires zero network configuration:

```rust
// ❌ Traditional P2P: Complex setup
// peer.connect(address)?;
// peer.authenticate(credentials)?;
// peer.subscribe(topic)?;
// peer.handle_conflicts()?;

// ✅ XaeroFlux: Just write data
xf.write_event(data, event_type)?;
// Everything else happens automatically!
```

### Actor Responsibilities

- **EventBus**: Lock-free event distribution to ring buffers
- **AOF Actor**: Durable persistence to LMDB with content addressing
- **MMR Actor**: Cryptographic integrity proofs and sync optimization
- **Vector Search Actor**: Real-time semantic indexing with custom extractors
- **P2P Actor**: Transparent peer discovery, identity verification, and event sync

### Transparent Operation

Users interact only with the simple XaeroFlux API:

```rust
// This is all users need to know:
let mut xf = XaeroFlux::new();
xf.start_aof()?;                          // Start everything
xf.write_event(data, event_type)?;        // Write events
let results = xf.search_vector(...)?;     // Search (optional)

// Everything else happens automatically in the background:
// - P2P peer discovery
// - Event synchronization
// - Conflict resolution  
// - Offline handling
// - Identity verification
```

### Memory Layout

```
Ring Buffer Pools (Stack Allocated):
┌─────────────────────────────────────┐
│ XS Ring: [64B events] × capacity    │
│ S Ring:  [256B events] × capacity   │  
│ M Ring:  [1KB events] × capacity    │
│ L Ring:  [4KB events] × capacity    │
│ XL Ring: [16KB events] × capacity   │
└─────────────────────────────────────┘
         ↓ (zero-copy access)
┌─────────────────────────────────────┐
│ Actors read via lock-free iterators │
│ - AOF: persist to LMDB              │
│ - Vector Search: extract + index    │
│ - P2P: broadcast to peers           │
└─────────────────────────────────────┘
```

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Test specific components
cargo test -p xaeroflux-actors aof::
cargo test -p xaeroflux-actors networking::p2p::
cargo test -p xaeroflux-actors indexing::vec_search::

# Test with thread sanitizer
RUSTFLAGS="-Z sanitizer=thread" cargo +nightly test

# Test with address sanitizer  
RUSTFLAGS="-Z sanitizer=address" cargo +nightly test
```

### Test Categories

1. **Unit Tests**: Individual component functionality
2. **Ring Buffer Tests**: Lock-free access and memory safety
3. **Integration Tests**: Complete system workflows
4. **P2P Tests**: Peer discovery and event synchronization
5. **Storage Tests**: LMDB persistence and recovery
6. **Vector Search Tests**: Indexing and similarity search
7. **Performance Tests**: Throughput and latency benchmarks

### Example Integration Test

```rust
#[tokio::test]
async fn test_complete_system_integration() {
    xaeroflux_core::initialize();
    
    // Start complete system
    let mut xf = XaeroFlux::new();
    xf.start_aof().unwrap();
    
    let mut extractors = HashMap::new();
    extractors.insert(42, Box::new(TestVectorExtractor));
    xf.start_vector_search(extractors, 64, 8, 100, 8, 50).unwrap();
    
    // Write test events
    xf.write_event(b"test data 1", 42).unwrap();
    xf.write_event(b"test data 2", 42).unwrap();
    
    // Allow processing time
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify storage
    let stats = xf.vector_search_stats().unwrap();
    assert!(stats.total_indexed > 0);
    
    // Verify vector search
    let results = xf.search_vector(vec![0.1; 64], 5, 0.5).unwrap();
    assert!(!results.is_empty());
    
    println!("✅ Complete system integration working");
}
```

## Contributing

We welcome contributions! Here's how to get involved:

### Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork**: `git clone https://github.com/yourusername/xaeroflux.git`
3. **Create a branch**: `git checkout -b feature/amazing-feature`
4. **Install dependencies**: `cargo build`
5. **Run tests**: `cargo test`

### Development Guidelines

- **Write tests** for new functionality
- **Maintain lock-free design** for ring buffer operations
- **Update documentation** including examples
- **Follow Rust conventions** and run `cargo fmt` and `cargo clippy`
- **Test memory safety** when working with ring buffers
- **Consider performance** impact of changes
- **Maintain backward compatibility** with existing APIs

### Areas We Need Help

- **More vector extractors**: Image, audio, document embeddings
- **P2P optimization**: Better peer discovery and connection management
- **Storage features**: Compression, encryption at rest
- **Mobile support**: iOS/Android optimizations
- **Documentation**: More examples and tutorials
- **Benchmarking**: Performance testing and optimization
- **Language bindings**: Python, JavaScript, Go, etc.

### Ring Buffer Development

When working with ring buffers:

- **Maintain lock-free design**: Single writer, multiple readers
- **Use proper alignment**: `#[repr(C, align(64))]` for cache optimization
- **Test memory safety**: Ensure no data races or memory leaks
- **Handle capacity limits**: Graceful handling of ring buffer exhaustion
- **Optimize for cache**: Consider CPU cache line optimization

### Submitting Changes

1. **Push your changes**: `git push origin feature/amazing-feature`
2. **Open a Pull Request** with clear description
3. **Respond to feedback** from maintainers
4. **Celebrate** when your PR is merged! 🎉

## License

This project is licensed under the **Mozilla Public License 2.0**.

**What this means:**
- ✅ **Commercial use** - Build commercial products with XaeroFlux
- ✅ **Modification** - Change the code to fit your needs
- ✅ **Distribution** - Share your applications
- ✅ **Patent use** - Protection from patent claims
- ⚠️ **Copyleft** - Changes to XaeroFlux itself must be shared back

See [LICENSE](LICENSE) for the complete license text.

---

**Ready to build offline-first distributed applications?** Start with our [Getting Started](#getting-started) guide and join the decentralized revolution! 🚀