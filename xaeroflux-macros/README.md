# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

Xaeroflux is an Rx-like distributed, decentralized, peer-to-peer event streaming engine with built-in CRDT support for conflict-free collaborative applications. It combines reactive programming patterns with automatic conflict resolution to enable truly decentralized, offline-first applications.

## Table of Contents

1. [Introduction](#introduction)
2. [Core Architecture](#core-architecture)
3. [Getting Started](#getting-started)
4. [Subject API](#subject-api)
5. [CRDT Operations](#crdt-operations)
6. [Collaborative Examples](#collaborative-examples)
7. [System Integration](#system-integration)
8. [Testing](#testing)
9. [Contributing](#contributing)
10. [License](#license)

## Introduction

### What is Xaeroflux?

Xaeroflux enables you to build **collaborative, decentralized applications** where multiple users can edit shared data simultaneously without conflicts. Think Google Docs, but without Google's servers - everything runs peer-to-peer.

### Key Features

- **🔄 Reactive Streams**: Rx-style event processing with operators like `map`, `filter`, `scan`
- **🤝 Conflict-Free**: Built-in CRDT support automatically resolves concurrent edits
- **📱 Offline-First**: Works without internet, syncs when reconnected
- **🔐 Cryptographically Secure**: Events signed with Falcon-512 quantum-resistant signatures
- **⚡ High Performance**: Zero-copy serialization and mmap-based storage
- **🌐 P2P Native**: Direct peer-to-peer sync via Iroh networking

## Core Architecture

Xaeroflux is built around three main concepts that work together:

### 1. Subjects - Event Streams
A `Subject` is a named event stream that multiple participants can write to and read from:

```rust
let chat = Subject::new("workspace/team-alpha/object/general-chat".into());
```

### 2. Operators - Stream Processing
Transform and filter events as they flow through the system:

```rust
let filtered_chat = chat
    .filter(|msg| !msg.evt.data.is_empty())
    .map(|msg| add_timestamp(msg));
```

### 3. CRDT Batch Processing - Conflict Resolution
Automatically resolve conflicts when multiple users edit simultaneously:

```rust
let collaborative_doc = subject
    .batch(Duration::from_millis(50), Some(20), vec![
        Operator::Sort(Sort::VectorClock.to_operator()),
        Operator::Fold(Fold::ORSet.to_operator()),
        Operator::Reduce(Reduce::SetContents.to_operator()),
    ]);
```

This creates a pipeline where concurrent operations are collected, sorted by causality, merged using CRDT rules, and then emitted as resolved state.

## Getting Started

### Basic Subject Usage

```rust
use xaeroflux::{Subject, XaeroEvent};
use xaeroflux_core::event::*;
use std::sync::Arc;

// 1. Create a subject for your data
let likes = Subject::new("workspace/blog/object/post-123-likes".into());

// 2. Set up a simple pipeline
let likes_stream = likes
    .filter(|event| {
        matches!(event.evt.event_type, 
            EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT))
    })
    .subscribe(|event| {
        println!("Someone liked the post!");
    });

// 3. Connect to storage and networking
let _handle = likes.unsafe_run(); // Starts AOF, MMR, P2P sync

// 4. Publish events
let like_event = XaeroEvent {
    evt: Event::new(1i64.to_le_bytes().to_vec(), CRDT_COUNTER_INCREMENT),
    merkle_proof: None,
    author_id: Some(user_id),
    latest_ts: Some(timestamp),
};
likes.data.sink.tx.send(like_event).unwrap();
```

### Adding Conflict Resolution

When multiple users interact simultaneously, add CRDT batch processing:

```rust
use xaeroflux_crdt::{Sort, Fold, Reduce};
use std::time::Duration;

let collaborative_likes = likes
    .batch(
        Duration::from_millis(100),  // Collect events for 100ms
        Some(25),                    // Or until 25 events
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::GCounter.to_operator()),    // Sum all likes
            Operator::Reduce(Reduce::CounterValue.to_operator()),
        ]
    )
    .subscribe(|resolved_state| {
        // Get final like count after resolving conflicts
        let bytes: [u8; 8] = resolved_state.evt.data[0..8].try_into().unwrap();
        let total_likes = i64::from_le_bytes(bytes);
        update_ui_likes(total_likes);
    });
```

## Subject API

### XaeroEvent Structure

Every event in Xaeroflux is wrapped in a `XaeroEvent`:

```rust
pub struct XaeroEvent {
    pub evt: Event<Vec<u8>>,           // Your data + metadata
    pub merkle_proof: Option<Vec<u8>>, // Cryptographic proof
    pub author_id: Option<XaeroID>,    // Who created this event
    pub latest_ts: Option<u64>,        // Author's logical timestamp
}
```

### Streaming Operators

Transform events in real-time:

```rust
let processed = subject
    .map(|xe| {
        // Transform each event
        add_metadata(xe)
    })
    .filter(|xe| {
        // Keep only events matching criteria
        !xe.evt.data.is_empty()
    })
    .filter_merkle_proofs()  // Keep only cryptographically verified events
    .scan(scan_window);      // Replay historical events
```

### Batch Operators

Handle concurrent operations with conflict resolution:

```rust
let crdt_pipeline = subject
    .batch(duration, event_count, vec![
        Operator::Sort(sort_function),    // Order by causality
        Operator::Fold(merge_function),   // Resolve conflicts
        Operator::Reduce(extract_state),  // Get final state
    ]);
```

### Publishing Events

```rust
// Send to data bus (most events)
subject.data.sink.tx.send(event).unwrap();

// Send to control bus (system events)
subject.control.sink.tx.send(control_event).unwrap();
```

## CRDT Operations

CRDTs (Conflict-free Replicated Data Types) automatically resolve conflicts when multiple users edit the same data simultaneously.

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

1. **Users create operations** (add, remove, increment, etc.)
2. **Operations are collected** in time windows via `.batch()`
3. **Sort by causality** to determine proper order
4. **Fold operations** using CRDT merge rules
5. **Reduce to final state** for your application

## Collaborative Examples

### Example 1: Real-time Reaction System

```rust
use xaeroflux::{Subject, XaeroEvent};
use xaeroflux_crdt::{Sort, Fold, Reduce};
use std::collections::HashMap;

// Create subject for post reactions
let reactions = Subject::new("workspace/social/object/post-456-reactions".into());

// Pipeline that handles concurrent reactions
let reaction_pipeline = reactions
    .batch(
        Duration::from_millis(200),
        Some(50),
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::ORSet.to_operator()),
            Operator::Reduce(Reduce::SetContents.to_operator()),
        ]
    )
    .subscribe(|final_reactions| {
        // Parse final reaction set
        if let Ok(reactions) = rkyv::from_bytes::<Vec<Vec<u8>>, _>(&final_reactions.evt.data) {
            let reaction_list: Vec<String> = reactions.into_iter()
                .filter_map(|r| String::from_utf8(r).ok())
                .collect();
            
            println!("📱 Current reactions: {:?}", reaction_list);
            update_reaction_ui(reaction_list);
        }
    });

// Start the system
let _handle = reactions.unsafe_run();

// Multiple users react simultaneously
async fn add_reaction(user_id: XaeroID, reaction: &str) {
    let event = XaeroEvent {
        evt: Event::new(reaction.as_bytes().to_vec(), CRDT_SET_ADD),
        merkle_proof: None,
        author_id: Some(user_id),
        latest_ts: Some(current_timestamp()),
    };
    reactions.data.sink.tx.send(event).unwrap();
}

// Users react at the same time - no conflicts!
add_reaction(alice_id, "👍").await;
add_reaction(bob_id, "❤️").await;
add_reaction(charlie_id, "👍").await;  // Same reaction as Alice - handled correctly
```

### Example 2: Collaborative Document Tags

```rust
// Create a tag management system
let tags = Subject::new("workspace/docs/object/proposal-v2-tags".into());

let tag_system = tags
    .filter(|xe| {
        // Only process tag-related events
        matches!(xe.evt.event_type,
            EventType::ApplicationEvent(CRDT_SET_ADD) |
            EventType::ApplicationEvent(CRDT_SET_REMOVE)
        )
    })
    .batch(
        Duration::from_millis(300),  // Longer window for tags
        None,                        // No count limit
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::ORSet.to_operator()),
            Operator::Reduce(Reduce::SetContents.to_operator()),
        ]
    )
    .subscribe(|tag_state| {
        if let Ok(tag_data) = rkyv::from_bytes::<Vec<Vec<u8>>, _>(&tag_state.evt.data) {
            let current_tags: Vec<String> = tag_data.into_iter()
                .filter_map(|t| String::from_utf8(t).ok())
                .collect();
                
            println!("🏷️  Document tags: {}", current_tags.join(", "));
            refresh_tag_display(current_tags);
        }
    });

let _handle = tags.unsafe_run();

// Team members add/remove tags concurrently
fn add_tag(user: XaeroID, tag: &str) {
    let event = XaeroEvent {
        evt: Event::new(tag.as_bytes().to_vec(), CRDT_SET_ADD),
        merkle_proof: None,
        author_id: Some(user),
        latest_ts: Some(current_timestamp()),
    };
    tags.data.sink.tx.send(event).unwrap();
}

fn remove_tag(user: XaeroID, tag: &str) {
    let event = XaeroEvent {
        evt: Event::new(tag.as_bytes().to_vec(), CRDT_SET_REMOVE),
        merkle_proof: None,
        author_id: Some(user),
        latest_ts: Some(current_timestamp()),
    };
    tags.data.sink.tx.send(event).unwrap();
}

// Concurrent tag operations
add_tag(alice_id, "urgent");
add_tag(bob_id, "needs-review");
remove_tag(charlie_id, "draft");     // Someone removes old tag
add_tag(alice_id, "approved");       // Alice adds final status
```

### Example 3: Voting System with PN-Counter

```rust
// Create a voting system where users can upvote/downvote
let votes = Subject::new("workspace/feature-requests/object/dark-mode-votes".into());

let voting_system = votes
    .batch(
        Duration::from_millis(100),  // Quick resolution for votes
        Some(30),
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::PNCounter.to_operator()),
            Operator::Reduce(Reduce::CounterValue.to_operator()),
        ]
    )
    .subscribe(|vote_result| {
        if vote_result.evt.data.len() >= 8 {
            let bytes: [u8; 8] = vote_result.evt.data[0..8].try_into().unwrap();
            let total_score = i64::from_le_bytes(bytes);
            
            println!("🗳️  Vote score: {}", total_score);
            update_vote_display(total_score);
            
            // Trigger notifications for milestone scores
            if total_score % 10 == 0 {
                notify_milestone_reached(total_score);
            }
        }
    });

let _handle = votes.unsafe_run();

// Users vote simultaneously
fn upvote(user_id: XaeroID) {
    let event = XaeroEvent {
        evt: Event::new(1i64.to_le_bytes().to_vec(), CRDT_COUNTER_INCREMENT),
        merkle_proof: None,
        author_id: Some(user_id),
        latest_ts: Some(current_timestamp()),
    };
    votes.data.sink.tx.send(event).unwrap();
}

fn downvote(user_id: XaeroID) {
    let event = XaeroEvent {
        evt: Event::new(1i64.to_le_bytes().to_vec(), CRDT_COUNTER_DECREMENT),
        merkle_proof: None,
        author_id: Some(user_id),
        latest_ts: Some(current_timestamp()),
    };
    votes.data.sink.tx.send(event).unwrap();
}

// Multiple users vote at once - all votes counted correctly
upvote(alice_id);
upvote(bob_id);
downvote(charlie_id);
upvote(diana_id);
// Final score: +2 (3 upvotes, 1 downvote)
```

## System Integration

### Storage Architecture

Every Subject automatically connects to a suite of storage actors. The batch processor collects concurrent events for CRDT resolution, which then flows through Sort → Fold → Reduce pipeline before being stored by AOF Actor in LMDB, indexed by MMR Actor, and paged by Segment Writer to memory-mapped files.

### Actor Responsibilities

- **AOF Actor**: Appends all events to LMDB for durability
- **MMR Actor**: Builds Merkle Mountain Range for cryptographic proofs
- **Segment Writer**: Pages events to memory-mapped files for efficient access
- **Batch Processor**: Collects concurrent events for CRDT resolution
- **P2P Sync**: Exchanges events with peers via Iroh networking

### Starting the System

```rust
// Connect subject to all storage actors
let handle = subject.unsafe_run();

// Keep handle alive to maintain connections
// Dropping handle shuts down storage actors
```

### Historical Replay

Access historical events using the scan operator:

```rust
use xaeroflux_core::event::ScanWindow;

let historical = subject
    .scan(ScanWindow {
        start: yesterday_timestamp,
        end: now_timestamp,
    })
    .subscribe(|historical_event| {
        println!("Replaying: {:?}", historical_event);
    });
```

## Testing

### Running Tests

```bash
# All tests
cargo test

# CRDT-specific tests  
cargo test -p xaeroflux-crdt

# Integration tests with storage
cargo test test_crdt_with_storage

# Concurrent operation tests
cargo test test_concurrent_
```

### Test Categories

1. **Unit Tests**: Individual CRDT operations and conflict resolution
2. **Integration Tests**: Subject pipelines with storage actors
3. **Concurrency Tests**: Multiple users editing simultaneously
4. **Network Tests**: P2P sync scenarios
5. **Performance Tests**: Large batches and high throughput

### Example Test

```rust
#[test]
fn test_concurrent_set_operations() {
    let subject = Subject::new("test/concurrent-set".into());
    let results = Arc::new(Mutex::new(Vec::new()));
    
    let pipeline = subject
        .batch(Duration::from_millis(50), Some(10), vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            Operator::Fold(Fold::ORSet.to_operator()),
            Operator::Reduce(Reduce::SetContents.to_operator()),
        ])
        .subscribe({
            let results = results.clone();
            move |final_state| {
                results.lock().unwrap().push(final_state);
            }
        });
    
    // Simulate concurrent operations
    subject.data.sink.tx.send(add_event("alice")).unwrap();
    subject.data.sink.tx.send(add_event("bob")).unwrap();
    subject.data.sink.tx.send(remove_event("alice")).unwrap();
    
    // Verify conflict resolution
    std::thread::sleep(Duration::from_millis(100));
    let final_results = results.lock().unwrap();
    assert_eq!(final_results.len(), 1);
    // Should contain only "bob" after conflict resolution
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

- **Write tests** for new functionality, especially CRDT scenarios
- **Update documentation** including examples in the README
- **Follow Rust conventions** and run `cargo fmt` and `cargo clippy`
- **Test concurrent scenarios** when working on CRDT features
- **Maintain backward compatibility** with existing Subject API

### Areas We Need Help

- **More CRDT types**: Text editing (RGA), Trees, Maps
- **Performance optimization**: Batch processing efficiency
- **Network protocols**: Better P2P discovery and sync
- **Mobile support**: iOS/Android optimizations
- **Documentation**: More examples and tutorials

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