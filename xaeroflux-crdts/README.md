# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

Xaeroflux is an Rx-like distributed, decentralized, peer-to-peer event streaming engine with built-in CRDT support for conflict-free collaborative applications. It combines reactive programming patterns with automatic conflict resolution to enable truly decentralized, offline-first applications.

## Table of Contents

1. [Introduction](#introduction)
2. [Core Architecture](#core-architecture)
3. [Getting Started](#getting-started)
4. [Subject API](#subject-api)
5. [Pipeline Processing](#pipeline-processing)
6. [CRDT Operations](#crdt-operations)
7. [Collaborative Examples](#collaborative-examples)
8. [System Integration](#system-integration)
9. [Testing](#testing)
10. [Contributing](#contributing)
11. [License](#license)

## Introduction

### What is Xaeroflux?

Xaeroflux enables you to build **collaborative, decentralized applications** where multiple users can edit shared data simultaneously without conflicts. Think Google Docs, but without Google's servers - everything runs peer-to-peer.

### Key Features

- **🔄 Reactive Streams**: Rx-style event processing with operators like `map`, `filter`, `scan`
- **🤝 Conflict-Free**: Built-in CRDT support automatically resolves concurrent edits
- **📱 Offline-First**: Works without internet, syncs when reconnected
- **🔐 Cryptographically Secure**: Events signed with quantum-resistant signatures
- **⚡ High Performance**: Zero-copy serialization and mmap-based storage
- **🌐 P2P Native**: Direct peer-to-peer sync via Iroh networking
- **🔄 Dual Processing**: Parallel streaming and batch event processing pipelines
- **⚙️ Signal Control**: Advanced flow control with kill and blackhole signals

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
    .filter(|msg| !msg.evt.data.is_empty())
    .map(|msg| add_timestamp(msg));
```

### 3. Pipeline Processing - Batch Operations
**NEW in v0.7.0-m5**: Sophisticated event processing pipelines with parallel streaming and batch loops:

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

## Getting Started

### Basic Subject Usage

```rust
use xaeroflux_macros::subject;
use xaeroflux_core::event::*;
use xaeroid::XaeroID;
use std::sync::Arc;

// 1. Create a subject for your data
let likes = subject!("workspace/blog/object/post-123-likes");

// 2. Set up a simple streaming pipeline
let likes_stream = likes
    .filter(|event| {
        matches!(event.evt.event_type, 
            EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT))
    })
    .subscribe(|event| {
        println!("Someone liked the post!");
        event
    });

// 3. Publish events
let user_id = create_test_xaeroid("user123");
let like_event = XaeroEvent {
    evt: Event::new(1i64.to_le_bytes().to_vec(), CRDT_COUNTER_INCREMENT),
    merkle_proof: None,
    author_id: Some(user_id),
    latest_ts: Some(current_timestamp()),
};
likes.data.sink.tx.send(like_event).unwrap();
```

### Advanced Pipeline Processing

**NEW**: Create sophisticated pipelines that handle both real-time and batch processing:

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
            matches!(event.evt.event_type,
                EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT) |
                EventType::ApplicationEvent(CRDT_COUNTER_DECREMENT)
            )
        }),
    )
    .map(|resolved_event| {
        // This runs in streaming mode after batch processing
        add_ui_metadata(resolved_event)
    })
    .subscribe(|final_event| {
        // Handle both batch results and streaming events
        match final_event.evt.event_type {
            EventType::ApplicationEvent(CRDT_COUNTER_STATE) => {
                // Batch-processed counter state
                let bytes: [u8; 8] = final_event.evt.data[0..8].try_into().unwrap();
                let total_likes = i64::from_le_bytes(bytes);
                update_ui_likes(total_likes);
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

### Buffer Operators

**NEW**: Handle concurrent operations with sophisticated pipeline processing:

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

**NEW in v0.7.0-m5**: Xaeroflux now features a sophisticated dual-loop architecture that processes events in parallel:

### Dual-Loop Architecture

1. **Streaming Loop**: Handles real-time events with low latency
2. **Batch Loop**: Collects and processes events for CRDT conflict resolution
3. **Event Router**: Intelligently routes events between the two loops

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
            matches!(event.evt.event_type,
                EventType::ApplicationEvent(DOC_TEXT_INSERT) |
                EventType::ApplicationEvent(DOC_TEXT_DELETE) |
                EventType::ApplicationEvent(DOC_FORMAT_CHANGE)
            )
        }),
    )
    .filter(|event| {
        // Filter out system events in streaming mode
        !matches!(event.evt.event_type, EventType::SystemEvent(_))
    })
    .subscribe(|event| {
        match event.evt.event_type {
            EventType::ApplicationEvent(DOC_COMMIT_STATE) => {
                // Handle batch-processed document commits
                apply_document_changes(event);
            },
            EventType::ApplicationEvent(DOC_CURSOR_MOVE) => {
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
```

### Signal Control

**NEW**: Advanced flow control with signals:

```rust
// Emergency stop - drops all future events
subject.data_signal_pipe.sink.tx.send(Signal::Blackhole).unwrap();

// Graceful shutdown
subject.data_signal_pipe.sink.tx.send(Signal::Kill).unwrap();

// Control-specific signals
subject.control_signal_pipe.sink.tx.send(Signal::ControlBlackhole).unwrap();
```

### Performance Features

- **Bounded Channels**: Built-in backpressure management
- **Lock-free Routing**: Efficient event distribution
- **Parallel Processing**: Batch and streaming loops run concurrently
- **Memory Efficient**: Events are moved, not copied, between processing stages

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
2. **Event router** directs CRDT operations to batch processing
3. **Operations are collected** in time windows via `.buffer()`
4. **Sort by causality** to determine proper order
5. **Fold operations** using CRDT merge rules
6. **Reduce to final state** for your application
7. **Transition to streaming** for real-time updates

## Collaborative Examples

### Example 1: Mixed Processing - Gaming Leaderboard

```rust
use xaeroflux_macros::subject;
use xaeroflux_crdt::{Sort, Fold, Reduce};

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
            matches!(event.evt.event_type,
                EventType::ApplicationEvent(SCORE_CHANGE) |
                EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT) |
                EventType::ApplicationEvent(CRDT_COUNTER_DECREMENT)
            )
        }),
    )
    .filter(|event| {
        // Keep all events in streaming mode
        true
    })
    .subscribe(|event| {
        match event.evt.event_type {
            EventType::ApplicationEvent(CRDT_COUNTER_STATE) => {
                // Batch-processed leaderboard update
                let bytes: [u8; 8] = event.evt.data[0..8].try_into().unwrap();
                let final_score = i64::from_le_bytes(bytes);
                update_leaderboard_display(final_score);
                
                // Broadcast to all players
                broadcast_leaderboard_update(final_score);
            },
            EventType::ApplicationEvent(PLAYER_MOVE) => {
                // Real-time player movement (streaming)
                update_player_position(event);
            },
            EventType::ApplicationEvent(CHAT_MESSAGE) => {
                // Real-time chat (streaming)
                display_chat_message(event);
            },
            _ => {}
        }
        event
    });

// Usage: Multiple players affect scores simultaneously
fn player_scores(player_id: XaeroID, points: i64) {
    let event = XaeroEvent {
        evt: Event::new(points.to_le_bytes().to_vec(), CRDT_COUNTER_INCREMENT),
        merkle_proof: None,
        author_id: Some(player_id),
        latest_ts: Some(current_timestamp()),
    };
    leaderboard.data.sink.tx.send(event).unwrap();
}

fn player_moves(player_id: XaeroID, position: (f32, f32)) {
    let mut data = Vec::new();
    data.extend_from_slice(&position.0.to_le_bytes());
    data.extend_from_slice(&position.1.to_le_bytes());
    
    let event = XaeroEvent {
        evt: Event::new(data, EventType::ApplicationEvent(PLAYER_MOVE).to_u8()),
        merkle_proof: None,
        author_id: Some(player_id),
        latest_ts: Some(current_timestamp()),
    };
    leaderboard.data.sink.tx.send(event).unwrap();
}

// Concurrent operations work perfectly:
// - Score changes are batched and resolved via CRDT
// - Player movements are streamed in real-time
// - Chat messages flow through streaming pipeline
```

### Example 2: IoT Sensor Data with Batch Processing

```rust
// Smart building temperature control system
let building_sensors = subject!("workspace/iot/object/building-climate")
    .buffer(
        Duration::from_millis(1000),  // Aggregate sensor data every second
        Some(50),                     // Or when 50 readings accumulate
        vec![
            // Sort by sensor ID and timestamp
            Operator::Sort(Arc::new(|a, b| {
                let a_id = a.author_id.as_ref().map(|id| id.did_peer_len).unwrap_or(0);
                let b_id = b.author_id.as_ref().map(|id| id.did_peer_len).unwrap_or(0);
                a_id.cmp(&b_id).then_with(|| a.latest_ts.unwrap_or(0).cmp(&b.latest_ts.unwrap_or(0)))
            })),
            
            // Aggregate sensor readings
            Operator::Fold(Arc::new(|_acc, events| {
                let reading_count = events.len() as u32;
                let avg_temp: f32 = events.iter()
                    .filter_map(|e| {
                        if e.evt.data.len() >= 4 {
                            let bytes: [u8; 4] = e.evt.data[0..4].try_into().ok()?;
                            Some(f32::from_le_bytes(bytes))
                        } else {
                            None
                        }
                    })
                    .sum::<f32>() / events.len() as f32;

                let mut data = Vec::new();
                data.extend_from_slice(&reading_count.to_le_bytes());
                data.extend_from_slice(&avg_temp.to_le_bytes());

                XaeroEvent {
                    evt: Event::new(data, EventType::ApplicationEvent(SENSOR_BATCH_READING).to_u8()),
                    merkle_proof: None,
                    author_id: Some(create_system_xaeroid()),
                    latest_ts: Some(current_timestamp()),
                }
            })),
            
            // Transition to streaming for alerts
            Operator::TransitionTo(
                SubjectExecutionMode::Buffer,
                SubjectExecutionMode::Streaming
            ),
        ],
        Arc::new(|event| {
            // Route sensor readings to batch processing
            matches!(event.evt.event_type,
                EventType::ApplicationEvent(SENSOR_TEMPERATURE) |
                EventType::ApplicationEvent(SENSOR_HUMIDITY) |
                EventType::ApplicationEvent(SENSOR_PRESSURE)
            )
        }),
    )
    .filter(|event| {
        // In streaming mode, pass through alerts and system events
        true
    })
    .subscribe(|event| {
        match event.evt.event_type {
            EventType::ApplicationEvent(SENSOR_BATCH_READING) => {
                // Process aggregated sensor data
                if event.evt.data.len() >= 8 {
                    let count_bytes: [u8; 4] = event.evt.data[0..4].try_into().unwrap();
                    let temp_bytes: [u8; 4] = event.evt.data[4..8].try_into().unwrap();
                    
                    let reading_count = u32::from_le_bytes(count_bytes);
                    let avg_temperature = f32::from_le_bytes(temp_bytes);
                    
                    println!("📊 Processed {} readings, avg temp: {:.1}°C", 
                             reading_count, avg_temperature);
                    
                    // Update building systems
                    update_hvac_system(avg_temperature);
                    log_to_building_management(reading_count, avg_temperature);
                    
                    // Check for alerts
                    if avg_temperature > 26.0 || avg_temperature < 18.0 {
                        trigger_temperature_alert(avg_temperature);
                    }
                }
            },
            EventType::ApplicationEvent(EMERGENCY_ALERT) => {
                // Emergency alerts bypass batch processing (streaming)
                handle_emergency_immediately(event);
            },
            _ => {}
        }
        event
    });

// High-frequency sensor data gets batched efficiently
fn send_temperature_reading(sensor_id: &str, temperature: f32) {
    let sensor_xid = create_sensor_xaeroid(sensor_id);
    let event = XaeroEvent {
        evt: Event::new(
            temperature.to_le_bytes().to_vec(), 
            EventType::ApplicationEvent(SENSOR_TEMPERATURE).to_u8()
        ),
        merkle_proof: None,
        author_id: Some(sensor_xid),
        latest_ts: Some(current_timestamp()),
    };
    building_sensors.data.sink.tx.send(event).unwrap();
}

// Emergency alerts go straight through streaming
fn send_emergency_alert(alert_type: &str) {
    let event = XaeroEvent {
        evt: Event::new(
            alert_type.as_bytes().to_vec(), 
            EventType::ApplicationEvent(EMERGENCY_ALERT).to_u8()
        ),
        merkle_proof: None,
        author_id: Some(create_system_xaeroid()),
        latest_ts: Some(current_timestamp()),
    };
    building_sensors.data.sink.tx.send(event).unwrap();
}

// System intelligently handles:
// - 100s of sensor readings → batched every second → aggregated data
// - Emergency alerts → immediate streaming → instant response
// - Different event types routed to appropriate processing mode
```

### Example 3: Collaborative Task Management with Transitions

```rust
// Project management with sophisticated event routing
let project_tasks = subject!("workspace/projects/object/website-redesign")
    .buffer(
        Duration::from_millis(300),  // Batch task updates
        Some(20),
        vec![
            Operator::Sort(Sort::VectorClock.to_operator()),
            
            // Merge task state changes using OR-Set CRDT
            Operator::Fold(Fold::ORSet.to_operator()),
            
            // Extract final task list
            Operator::Reduce(Reduce::SetContents.to_operator()),
            
            // Switch to streaming for comments and real-time updates
            Operator::TransitionTo(
                SubjectExecutionMode::Buffer,
                SubjectExecutionMode::Streaming
            ),
        ],
        Arc::new(|event| {
            // Route task operations to batch processing
            matches!(event.evt.event_type,
                EventType::ApplicationEvent(TASK_CREATED) |
                EventType::ApplicationEvent(TASK_COMPLETED) |
                EventType::ApplicationEvent(TASK_ASSIGNED) |
                EventType::ApplicationEvent(CRDT_SET_ADD) |
                EventType::ApplicationEvent(CRDT_SET_REMOVE)
            )
        }),
    )
    .map(|event| {
        // Add processing metadata in streaming mode
        add_notification_metadata(event)
    })
    .filter(|event| {
        // Filter out debug events in streaming
        !matches!(event.evt.event_type, EventType::SystemEvent(_))
    })
    .subscribe(|event| {
        match event.evt.event_type {
            EventType::ApplicationEvent(CRDT_SET_STATE) => {
                // Batch-processed task list update
                if let Ok(task_data) = rkyv::from_bytes::<Vec<Vec<u8>>, _>(&event.evt.data) {
                    let current_tasks: Vec<String> = task_data.into_iter()
                        .filter_map(|t| String::from_utf8(t).ok())
                        .collect();
                        
                    println!("📋 Project tasks updated: {} active tasks", current_tasks.len());
                    refresh_task_board(current_tasks);
                    
                    // Notify team of major changes
                    if current_tasks.len() < 5 {
                        notify_sprint_completion();
                    }
                }
            },
            EventType::ApplicationEvent(COMMENT_ADDED) => {
                // Real-time comments (streaming)
                display_new_comment(event);
                send_notification_to_team(event);
            },
            EventType::ApplicationEvent(USER_TYPING) => {
                // Real-time typing indicators (streaming)
                show_typing_indicator(event);
            },
            _ => {}
        }
        event
    });

// Task operations (go to batch processing)
fn create_task(user_id: XaeroID, task_name: &str) {
    let event = XaeroEvent {
        evt: Event::new(task_name.as_bytes().to_vec(), CRDT_SET_ADD),
        merkle_proof: None,
        author_id: Some(user_id),
        latest_ts: Some(current_timestamp()),
    };
    project_tasks.data.sink.tx.send(event).unwrap();
}

fn complete_task(user_id: XaeroID, task_name: &str) {
    let event = XaeroEvent {
        evt: Event::new(task_name.as_bytes().to_vec(), CRDT_SET_REMOVE),
        merkle_proof: None,
        author_id: Some(user_id),
        latest_ts: Some(current_timestamp()),
    };
    project_tasks.data.sink.tx.send(event).unwrap();
}

// Real-time operations (go to streaming)
fn add_comment(user_id: XaeroID, comment: &str) {
    let event = XaeroEvent {
        evt: Event::new(comment.as_bytes().to_vec(), EventType::ApplicationEvent(COMMENT_ADDED).to_u8()),
        merkle_proof: None,
        author_id: Some(user_id),
        latest_ts: Some(current_timestamp()),
    };
    project_tasks.data.sink.tx.send(event).unwrap();
}

// Demonstrates the power of mixed processing:
// - Task changes are batched and conflict-resolved
// - Comments and typing appear in real-time
// - System scales efficiently with team size
```

## System Integration

### Storage Architecture

Every Subject automatically connects to a sophisticated storage and processing system:

1. **Event Router**: Distributes events between streaming and batch processing
2. **Dual Processing Loops**: Parallel streaming and batch event processing
3. **AOF Actor**: Appends all events to LMDB for durability
4. **MMR Actor**: Builds Merkle Mountain Range for cryptographic proofs
5. **Segment Writer**: Pages events to memory-mapped files for efficient access
6. **P2P Sync**: Exchanges events with peers via Iroh networking

### Actor Responsibilities

- **Event Router**: Intelligent event distribution based on predicates
- **Batch Processor**: Collects concurrent events for CRDT resolution
- **Streaming Processor**: Handles real-time events with low latency
- **AOF Actor**: Durable event persistence in LMDB
- **MMR Actor**: Cryptographic proof generation
- **Segment Writer**: Efficient file-based storage
- **P2P Sync**: Peer-to-peer event synchronization

### Starting the System

```rust
use xaeroflux_macros::subject;

// Create subject with automatic system integration
let my_subject = subject!("workspace/myapp/object/data");

// All storage actors and processing loops start automatically
// No need for explicit unsafe_run() with the macro
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
# All tests
cargo test

# CRDT-specific tests  
cargo test -p xaeroflux-crdt

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

### Example Test - Pipeline Processing

```rust
#[test]
fn test_mixed_processing_pipeline() {
    let subject = subject!("test/mixed-processing");
    let batch_results = Arc::new(Mutex::new(Vec::new()));
    let streaming_results = Arc::new(Mutex::new(Vec::new()));
    
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
                matches!(event.evt.event_type, EventType::ApplicationEvent(CRDT_SET_ADD))
            }),
        )
        .subscribe({
            let batch_results = batch_results.clone();
            let streaming_results = streaming_results.clone();
            move |event| {
                match event.evt.event_type {
                    EventType::ApplicationEvent(CRDT_SET_STATE) => {
                        batch_results.lock().unwrap().push(event.clone());
                    },
                    EventType::ApplicationEvent(STREAMING_EVENT) => {
                        streaming_results.lock().unwrap().push(event.clone());
                    },
                    _ => {}
                }
                event
            }
        });
    
    // Send mixed events
    subject.data.sink.tx.send(create_set_add_event("item1")).unwrap();
    subject.data.sink.tx.send(create_streaming_event("update1")).unwrap();
    subject.data.sink.tx.send(create_set_add_event("item2")).unwrap();
    
    std::thread::sleep(Duration::from_millis(100));
    
    // Verify both processing modes worked
    assert_eq!(batch_results.lock().unwrap().len(), 1);   // One batch result
    assert_eq!(streaming_results.lock().unwrap().len(), 1); // One streaming result
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

- **Write tests** for new functionality, especially pipeline and CRDT scenarios
- **Update documentation** including examples in the README
- **Follow Rust conventions** and run `cargo fmt` and `cargo clippy`
- **Test concurrent scenarios** when working on CRDT features
- **Test pipeline processing** when working on buffer/sort/fold/reduce features
- **Maintain backward compatibility** with existing Subject API

### Areas We Need Help

- **More CRDT types**: Text editing (RGA), Trees, Maps
- **Pipeline optimization**: Buffer processing efficiency
- **Advanced operators**: Custom sort/fold/reduce implementations
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