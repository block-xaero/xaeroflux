# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

Xaeroflux is an Rx-like distributed, decentralized, peer-to-peer event streaming engine with built-in CRDT support for conflict-free collaborative applications. It combines reactive programming patterns with automatic conflict resolution to enable truly decentralized, offline-first applications.

## Table of Contents

1. [Introduction](#introduction)
2. [Core Architecture](#core-architecture)
3. [Memory Architecture](#memory-architecture)
4. [Getting Started](#getting-started)
5. [Subject API](#subject-api)
6. [Pipeline Processing](#pipeline-processing)
7. [CRDT Operations](#crdt-operations)
8. [Collaborative Examples](#collaborative-examples)
9. [System Integration](#system-integration)
10. [Testing](#testing)
11. [Contributing](#contributing)
12. [License](#license)

## Introduction

### What is Xaeroflux?

Xaeroflux enables you to build **collaborative, decentralized applications** where multiple users can edit shared data simultaneously without conflicts. Think Google Docs, but without Google's servers - everything runs peer-to-peer.

### Key Features

- **🔄 Reactive Streams**: Rx-style event processing with operators like `map`, `filter`, `scan`
- **🤝 Conflict-Free**: Built-in CRDT support automatically resolves concurrent edits
- **📱 Offline-First**: Works without internet, syncs when reconnected
- **🔐 Cryptographically Secure**: Events signed with quantum-resistant signatures
- **⚡ High Performance**: Zero-copy ring buffer architecture with predictable memory usage
- **🌐 P2P Native**: Direct peer-to-peer sync via Iroh networking
- **🔄 Dual Processing**: Parallel streaming and batch event processing pipelines
- **⚙️ Signal Control**: Advanced flow control with kill and blackhole signals
- **🧠 Memory Efficient**: Stack-based allocation with ring buffer pools

## Core Architecture

Xaeroflux is built around four main concepts that work together:

### 1. Subjects - Event Streams
A `Subject` is a named event stream that multiple participants can write to and read from:

```rust
use xaeroflux_macros::subject;

let chat = subject!("workspace/team-alpha/object/general-chat");
```

### 2. Streaming Operators - Real-time Processing
Transform and filter events as they flow through the system in real-time:

```rust
let filtered_chat = chat
    .filter(|msg| !msg.data().is_empty())
    .map(|msg| add_timestamp(msg));
```

### 3. Pipeline Processing - Batch Operations
Sophisticated event processing pipelines with parallel streaming and batch loops:

```rust
let collaborative_doc = subject
    .buffer(
        Duration::from_millis(50), 
        Some(20), 
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::ORSet.to_operator()),
            Operator::Reduce(Reduce::SetContents.to_operator()),
            Operator::TransitionTo(SubjectExecutionMode::Buffer, SubjectExecutionMode::Streaming),
        ],
        Arc::new(is_crdt_operation),
    );
```

### 4. Event Routing - Intelligent Distribution
Events are automatically routed between streaming and batch processing based on predicates, with efficient backpressure management and signal control.

## Memory Architecture

**NEW in v0.7.0-m5**: Xaeroflux now features a sophisticated zero-copy memory architecture built on ring buffer pools:

### PooledEventPtr Architecture

- **Ring Buffer Pools**: Stack-allocated pools for different event sizes (64B, 256B, 1KB, 4KB, 16KB)
- **Zero-Copy Access**: Events remain in ring buffers throughout the pipeline
- **Reference Counting**: `Arc<XaeroEvent>` provides safe sharing across threads
- **Predictable Memory**: Fixed-size pools eliminate heap fragmentation
- **Stack-Based**: All event data lives on the stack for better cache performance

### Event Structure

```rust
pub struct XaeroEvent {
    pub evt: PooledEventPtr,                     // Ring buffer pointer (zero-copy)
    pub author_id: Option<RingPtr<XaeroID>>,     // Stack-allocated author
    pub merkle_proof: Option<RingPtr<FixedMerkleProof>>, // Stack-allocated proof
    pub vector_clock: Option<RingPtr<FixedVectorClock>>, // Stack-allocated clock
    pub latest_ts: u64,                          // Timestamp
}

impl XaeroEvent {
    pub fn data(&self) -> &[u8] {               // Zero-copy data access
        self.evt.data()
    }
    
    pub fn event_type(&self) -> u8 {            // Zero-copy type access
        self.evt.event_type()
    }
}
```

### Memory Benefits

- **Zero Allocations**: Events use pre-allocated ring buffer slots
- **Cache Friendly**: Sequential memory layout improves performance
- **Thread Safe**: Arc<XaeroEvent> enables safe concurrent access
- **Bounded Memory**: Predictable memory usage for embedded systems
- **Fast Serialization**: Direct memory mapping to storage

### Pool Management

```rust
// Initialize ring buffer pools
XaeroPoolManager::init();

// Create events from pool
let event = XaeroPoolManager::create_xaero_event(
    data_slice,           // &[u8] - zero-copy data
    event_type,           // u8 - event type  
    None,                 // author_id (optional)
    None,                 // merkle_proof (optional)
    None,                 // vector_clock (optional)
    timestamp,            // u64 - timestamp
)?;
```

## Getting Started

### Basic Subject Usage

```rust
use xaeroflux_macros::subject;
use xaeroflux_core::{XaeroPoolManager, event::*};
use xaeroid::XaeroID;
use std::sync::Arc;

// Initialize ring buffer pools
XaeroPoolManager::init();

// 1. Create a subject for your data
let likes = subject!("workspace/blog/object/post-123-likes");

// 2. Set up a simple streaming pipeline
let likes_stream = likes
    .filter(|event| {
        matches!(event.event_type(), CRDT_COUNTER_INCREMENT)
    })
    .subscribe(|event| {
        println!("Someone liked the post!");
        event
    });

// 3. Publish events using ring buffer pools
let user_id = create_test_xaeroid("user123");
let like_event = XaeroPoolManager::create_xaero_event(
    &1i64.to_le_bytes(),
    CRDT_COUNTER_INCREMENT,
    Some(user_id),
    None,
    None,
    current_timestamp(),
).expect("Pool allocation failed");

likes.data.sink.tx.send(like_event).unwrap();
```

### Advanced Pipeline Processing

Create sophisticated pipelines that handle both real-time and batch processing:

```rust
use xaeroflux_crdt::{Sort, Fold, Reduce};
use std::time::Duration;

let collaborative_likes = likes
    .buffer(
        Duration::from_millis(100),  // Collect events for 100ms
        Some(25),                    // Or until 25 events
        vec![
            // Batch processing pipeline
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::GCounter.to_operator()),
            Operator::Reduce(Reduce::CounterValue.to_operator()),
            // Transition back to streaming for real-time updates
            Operator::TransitionTo(
                SubjectExecutionMode::Buffer, 
                SubjectExecutionMode::Streaming
            ),
        ],
        Arc::new(|event| {
            // Route CRDT operations to batch processing
            matches!(event.event_type(),
                CRDT_COUNTER_INCREMENT | CRDT_COUNTER_DECREMENT
            )
        }),
    )
    .map(|resolved_event| {
        // This runs in streaming mode after batch processing
        add_ui_metadata(resolved_event)
    })
    .subscribe(|final_event| {
        // Handle both batch results and streaming events
        match final_event.event_type() {
            CRDT_COUNTER_STATE => {
                // Batch-processed counter state
                let data = final_event.data();
                if data.len() >= 8 {
                    let bytes: [u8; 8] = data[0..8].try_into().unwrap();
                    let total_likes = i64::from_le_bytes(bytes);
                    update_ui_likes(total_likes);
                }
            },
            _ => {
                // Regular streaming events
                handle_streaming_event(final_event);
            }
        }
        final_event
    });
```

## Subject API

### XaeroEvent Structure

Every event in Xaeroflux uses the new ring buffer architecture:

```rust
pub struct XaeroEvent {
    pub evt: PooledEventPtr,                     // Ring buffer pointer (zero-copy)
    pub author_id: Option<RingPtr<XaeroID>>,     // Stack-allocated
    pub merkle_proof: Option<RingPtr<FixedMerkleProof>>, // Stack-allocated
    pub vector_clock: Option<RingPtr<FixedVectorClock>>, // Stack-allocated
    pub latest_ts: u64,                          // Timestamp
}

// Zero-copy access methods
impl XaeroEvent {
    pub fn data(&self) -> &[u8];                // Access event data
    pub fn event_type(&self) -> u8;             // Access event type
    pub fn author_id(&self) -> Option<&XaeroID>; // Access author
    pub fn merkle_proof(&self) -> Option<&[u8]>; // Access proof
}
```

### Streaming Operators

Transform events in real-time with zero-copy access:

```rust
let processed = subject
    .map(|xe| {
        // Transform each event (zero-copy)
        add_metadata(xe)
    })
    .filter(|xe| {
        // Keep only events matching criteria (zero-copy)
        !xe.data().is_empty()
    })
    .filter_merkle_proofs()  // Keep only cryptographically verified events
    .scan(scan_window);      // Replay historical events
```

### Buffer Operators

Handle concurrent operations with sophisticated pipeline processing:

```rust
let pipeline = subject
    .buffer(
        duration,           // Time window for collecting events
        event_count,        // Optional count threshold  
        vec![
            Operator::Sort(sort_function),    // Order by causality
            Operator::Fold(merge_function),   // Resolve conflicts
            Operator::Reduce(extract_state),  // Get final state
            Operator::TransitionTo(from, to), // Switch processing modes
        ],
        route_predicate,    // Which events go to batch processing
    );
```

## Pipeline Processing

Xaeroflux features a sophisticated dual-loop architecture that processes events in parallel with zero-copy semantics:

### Dual-Loop Architecture

1. **Streaming Loop**: Handles real-time events with low latency
2. **Batch Loop**: Collects and processes events for CRDT conflict resolution
3. **Event Router**: Intelligently routes events between the two loops
4. **Ring Buffer Integration**: All data flows through zero-copy ring buffers

### Pipeline Example: Document Collaboration

```rust
use xaeroflux_crdt::{Sort, Fold, Reduce};

let doc_subject = subject!("workspace/docs/object/shared-document")
    .buffer(
        Duration::from_millis(200),  // Batch text operations
        Some(10),                    // Process immediately with 10 ops
        vec![
            // Sort operations by timestamp for proper ordering
            Operator::Sort(Sort::VectorClock.to_operator()),
            
            // Merge concurrent edits using CRDT rules
            Operator::Fold(Fold::LWWRegister.to_operator()),
            
            // Extract final document state
            Operator::Reduce(Reduce::RegisterValue.to_operator()),
            
            // Transition back to streaming for real-time cursor updates
            Operator::TransitionTo(
                SubjectExecutionMode::Buffer,
                SubjectExecutionMode::Streaming
            ),
        ],
        Arc::new(|event| {
            // Route text operations to batch, cursor moves to streaming
            matches!(event.event_type(),
                DOC_TEXT_INSERT | DOC_TEXT_DELETE | DOC_FORMAT_CHANGE
            )
        }),
    )
    .filter(|event| {
        // Filter out system events in streaming mode
        !matches!(event.event_type(), 200..=255) // System event range
    })
    .subscribe(|event| {
        match event.event_type() {
            DOC_COMMIT_STATE => {
                // Handle batch-processed document commits
                apply_document_changes(event);
            },
            DOC_CURSOR_MOVE => {
                // Handle real-time cursor updates
                update_cursor_position(event);
            },
            _ => {}
        }
        event
    });

// The system automatically handles:
// - Text edits → batch processing → conflict resolution → commit
// - Cursor moves → streaming → immediate UI updates
// - Parallel processing without blocking
// - Zero-copy data access throughout
```

### Signal Control

Advanced flow control with signals:

```rust
// Emergency stop - drops all future events
subject.data_signal_pipe.sink.tx.send(Signal::Blackhole).unwrap();

// Graceful shutdown
subject.data_signal_pipe.sink.tx.send(Signal::Kill).unwrap();

// Control-specific signals
subject.control_signal_pipe.sink.tx.send(Signal::ControlBlackhole).unwrap();
```

### Performance Features

- **Ring Buffer Pools**: Stack-based allocation with predictable memory usage
- **Zero-Copy Operations**: Events never copied between processing stages
- **Bounded Channels**: Built-in backpressure management
- **Lock-free Routing**: Efficient event distribution
- **Parallel Processing**: Batch and streaming loops run concurrently
- **Cache Efficiency**: Sequential memory layout optimizes CPU cache usage

## CRDT Operations

CRDTs (Conflict-free Replicated Data Types) automatically resolve conflicts when multiple users edit the same data simultaneously. All CRDT operations use the ring buffer architecture for optimal performance.

### Available CRDT Types

| CRDT Type | Use Case | Operations | Example |
|-----------|----------|------------|---------|
| **OR-Set** | Collections that can grow/shrink | `ADD`, `REMOVE` | User reactions, tags, participants |
| **G-Counter** | Counters that only increase | `INCREMENT` | View counts, downloads |
| **PN-Counter** | Counters that can increase/decrease | `INCREMENT`, `DECREMENT` | Likes/dislikes, voting |
| **LWW-Register** | Single values with last-writer-wins | `WRITE` | User status, settings |

### Event Type Constants

```rust
// OR-Set operations
pub const CRDT_SET_ADD: u8 = 30;
pub const CRDT_SET_REMOVE: u8 = 31;
pub const CRDT_SET_STATE: u8 = 32;

// Counter operations  
pub const CRDT_COUNTER_INCREMENT: u8 = 33;
pub const CRDT_COUNTER_DECREMENT: u8 = 34;
pub const CRDT_COUNTER_STATE: u8 = 35;

// Register operations
pub const CRDT_REGISTER_WRITE: u8 = 43;
pub const CRDT_REGISTER_STATE: u8 = 44;
```

### How CRDTs Work in Xaeroflux

1. **Users create operations** (add, remove, increment, etc.) using ring buffer pools
2. **Event router** directs CRDT operations to batch processing
3. **Operations are collected** in time windows via `.buffer()`
4. **Sort by causality** to determine proper order
5. **Fold operations** using CRDT merge rules
6. **Reduce to final state** for your application
7. **Transition to streaming** for real-time updates
8. **Zero-copy access** throughout the entire pipeline

## Collaborative Examples

### Example 1: Gaming Leaderboard with Ring Buffers

```rust
use xaeroflux_macros::subject;
use xaeroflux_crdt::{Sort, Fold, Reduce};
use xaeroflux_core::XaeroPoolManager;

// Initialize ring buffer pools
XaeroPoolManager::init();

// Create leaderboard with mixed processing
let leaderboard = subject!("workspace/game/object/arena-leaderboard")
    .buffer(
        Duration::from_millis(500),  // Batch score updates every 500ms
        Some(100),                   // Or when 100 score changes accumulate
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::PNCounter.to_operator()),
            Operator::Reduce(Reduce::CounterValue.to_operator()),
            // Transition back to streaming for real-time player actions
            Operator::TransitionTo(
                SubjectExecutionMode::Buffer,
                SubjectExecutionMode::Streaming
            ),
        ],
        Arc::new(|event| {
            // Route score changes to batch, player actions to streaming
            matches!(event.event_type(),
                SCORE_CHANGE | CRDT_COUNTER_INCREMENT | CRDT_COUNTER_DECREMENT
            )
        }),
    )
    .subscribe(|event| {
        match event.event_type() {
            CRDT_COUNTER_STATE => {
                // Batch-processed leaderboard update
                let data = event.data();
                if data.len() >= 8 {
                    let bytes: [u8; 8] = data[0..8].try_into().unwrap();
                    let final_score = i64::from_le_bytes(bytes);
                    update_leaderboard_display(final_score);
                    broadcast_leaderboard_update(final_score);
                }
            },
            PLAYER_MOVE => {
                // Real-time player movement (streaming)
                update_player_position(event);
            },
            CHAT_MESSAGE => {
                // Real-time chat (streaming)
                display_chat_message(event);
            },
            _ => {}
        }
        event
    });

// Usage: Multiple players affect scores simultaneously
fn player_scores(player_id: XaeroID, points: i64) -> Result<(), PoolError> {
    let event = XaeroPoolManager::create_xaero_event(
        &points.to_le_bytes(),
        CRDT_COUNTER_INCREMENT,
        Some(player_id),
        None,
        None,
        current_timestamp(),
    )?;
    leaderboard.data.sink.tx.send(event).unwrap();
    Ok(())
}

fn player_moves(player_id: XaeroID, position: (f32, f32)) -> Result<(), PoolError> {
    let mut data = Vec::new();
    data.extend_from_slice(&position.0.to_le_bytes());
    data.extend_from_slice(&position.1.to_le_bytes());
    
    let event = XaeroPoolManager::create_xaero_event(
        &data,
        PLAYER_MOVE,
        Some(player_id),
        None,
        None,
        current_timestamp(),
    )?;
    leaderboard.data.sink.tx.send(event).unwrap();
    Ok(())
}

// Concurrent operations work efficiently:
// - Score changes are batched and resolved via CRDT
// - Player movements are streamed in real-time
// - Chat messages flow through streaming pipeline
// - All using zero-copy ring buffer architecture
```

## System Integration

### Storage Architecture

Every Subject automatically connects to a sophisticated storage and processing system with ring buffer integration:

1. **Event Router**: Distributes events between streaming and batch processing
2. **Dual Processing Loops**: Parallel streaming and batch event processing with zero-copy
3. **AOF Actor**: Appends all events to LMDB using new archive format
4. **MMR Actor**: Builds Merkle Mountain Range for cryptographic proofs
5. **Segment Writer**: Pages events to memory-mapped files with zero-copy serialization
6. **P2P Sync**: Exchanges events with peers via Iroh networking

### Archive Format

**NEW**: Optimized binary format for ring buffer events:

- **24-byte header**: Magic marker, event type, length, timestamp
- **Zero-copy serialization**: Direct memory mapping from ring buffers
- **~50% size reduction**: Compared to previous rkyv format
- **Alignment-safe**: Handles unaligned memory access correctly

### Actor Responsibilities

- **Event Router**: Intelligent event distribution based on predicates
- **Batch Processor**: Collects concurrent events for CRDT resolution using ring buffers
- **Streaming Processor**: Handles real-time events with zero-copy access
- **AOF Actor**: Durable event persistence using new archive format
- **MMR Actor**: Cryptographic proof generation with ring buffer hashing
- **Segment Writer**: Efficient file-based storage with zero-copy serialization
- **P2P Sync**: Peer-to-peer event synchronization

### Starting the System

```rust
use xaeroflux_macros::subject;
use xaeroflux_core::XaeroPoolManager;

// Initialize ring buffer pools
XaeroPoolManager::init();

// Create subject with automatic system integration
let my_subject = subject!("workspace/myapp/object/data");

// All storage actors and processing loops start automatically
// Ring buffer pools are shared across all system components
```

### Historical Replay

Access historical events using the scan operator with zero-copy reconstruction:

```rust
use xaeroflux_core::event::ScanWindow;

let historical = subject
    .scan(ScanWindow {
        start: yesterday_timestamp,
        end: now_timestamp,
    })
    .subscribe(|historical_event| {
        // Events are reconstructed into ring buffers from storage
        println!("Replaying: type={}, data_len={}", 
                 historical_event.event_type(), 
                 historical_event.data().len());
        historical_event
    });
```

### Signal Control and Shutdown

```rust
// Graceful shutdown of all processing
subject.data_signal_pipe.sink.tx.send(Signal::Kill).unwrap();
subject.control_signal_pipe.sink.tx.send(Signal::ControlKill).unwrap();

// Emergency stop (blackhole)
subject.data_signal_pipe.sink.tx.send(Signal::Blackhole).unwrap();
```

## Testing

### Running Tests

```bash
# All tests with ring buffer stack size
RUST_MIN_STACK=32000000 cargo test

# CRDT-specific tests  
cargo test -p xaeroflux-crdt

# Ring buffer pool tests
cargo test -p xaeroflux-core pool::

# Pipeline processing tests
cargo test test_pipeline_

# Concurrent operation tests
cargo test test_concurrent_
```

### Test Categories

1. **Unit Tests**: Individual CRDT operations and conflict resolution
2. **Pipeline Tests**: Buffer → Sort → Fold → Reduce → Transition workflows
3. **Integration Tests**: Subject pipelines with storage actors
4. **Concurrency Tests**: Multiple users editing simultaneously
5. **Signal Tests**: Kill and blackhole signal handling
6. **Performance Tests**: Large batches and high throughput
7. **Ring Buffer Tests**: Pool allocation, zero-copy access, memory safety

### Example Test - Ring Buffer Integration

```rust
#[test]
fn test_ring_buffer_pipeline() {
    XaeroPoolManager::init();
    
    let subject = subject!("test/ring-buffer-processing");
    let results = Arc::new(Mutex::new(Vec::new()));
    
    let pipeline = subject
        .buffer(
            Duration::from_millis(50), 
            Some(5), 
            vec![
                Operator::Sort(Sort::VectorClock.to_operator()),
                Operator::Fold(Fold::ORSet.to_operator()),
                Operator::Reduce(Reduce::SetContents.to_operator()),
                Operator::TransitionTo(
                    SubjectExecutionMode::Buffer,
                    SubjectExecutionMode::Streaming
                ),
            ],
            Arc::new(|event| {
                matches!(event.event_type(), CRDT_SET_ADD)
            }),
        )
        .subscribe({
            let results = results.clone();
            move |event| {
                // Verify zero-copy access
                assert!(!event.data().is_empty());
                assert!(event.event_type() != 0);
                
                results.lock().unwrap().push(event.clone());
                event
            }
        });
    
    // Create events using ring buffer pools
    for i in 0..3 {
        let event = XaeroPoolManager::create_xaero_event(
            &format!("item{}", i).as_bytes(),
            CRDT_SET_ADD,
            None,
            None,
            None,
            current_timestamp(),
        ).expect("Pool allocation failed");
        
        subject.data.sink.tx.send(event).unwrap();
    }
    
    std::thread::sleep(Duration::from_millis(100));
    
    // Verify pipeline processed events correctly
    let final_results = results.lock().unwrap();
    assert!(!final_results.is_empty());
    
    // Verify all events used ring buffer allocation
    for event in final_results.iter() {
        assert!(event.is_pure_zero_copy());
    }
}
```

## Contributing

We welcome contributions! Here's how to get involved:

### Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork**: `git clone https://github.com/yourusername/xaeroflux.git`
3. **Create a branch**: `git checkout -b feature/amazing-feature`
4. **Install dependencies**: `cargo build`
5. **Run tests**: `RUST_MIN_STACK=32000000 cargo test`

### Development Guidelines

- **Write tests** for new functionality, especially pipeline and CRDT scenarios
- **Test ring buffer integration** for any memory-related changes
- **Update documentation** including examples in the README
- **Follow Rust conventions** and run `cargo fmt` and `cargo clippy`
- **Test concurrent scenarios** when working on CRDT features
- **Test pipeline processing** when working on buffer/sort/fold/reduce features
- **Maintain backward compatibility** with existing Subject API
- **Consider memory safety** when working with ring buffer pools

### Areas We Need Help

- **More CRDT types**: Text editing (RGA), Trees, Maps
- **Pipeline optimization**: Buffer processing efficiency with ring buffers
- **Advanced operators**: Custom sort/fold/reduce implementations
- **Network protocols**: Better P2P discovery and sync
- **Mobile support**: iOS/Android ring buffer optimizations
- **Documentation**: More examples and tutorials
- **Benchmarking**: Performance testing of ring buffer architecture

### Ring Buffer Development

When working with the ring buffer architecture:

- **Initialize pools**: Always call `XaeroPoolManager::init()` in tests
- **Use create_xaero_event()**: Don't manually construct XaeroEvent
- **Handle pool exhaustion**: Use proper error handling for allocation failures
- **Test zero-copy**: Verify data access uses `.data()` and `.event_type()` methods
- **Memory safety**: Ensure Arc<XaeroEvent> sharing is correct

### Submitting Changes

1. **Push your changes**: `git push origin feature/amazing-feature`
2. **Open a Pull Request** with clear description
3. **Respond to feedback** from maintainers
4. **Celebrate** when your PR is merged! 🎉

## License

This project is licensed under the **Mozilla Public License 2.0**.

**What this means:**
- ✅ **Commercial use** - Build commercial products with Xaeroflux
- ✅ **Modification** - Change the code to fit your needs
- ✅ **Distribution** - Share your applications
- ✅ **Patent use** - Protection from patent claims
- ⚠️ **Copyleft** - Changes to Xaeroflux itself must be shared back

See [LICENSE](LICENSE) for the complete license text.

---

**Ready to build the future of collaborative applications?** Start with our [Getting Started](#getting-started) guide and join the decentralized revolution! 🚀