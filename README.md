# XaeroFlux

A decentralized peer-to-peer event log that syncs across devices using [Iroh](https://iroh.computer) and SQLite.

## What It Does

- **Send events** from your app
- **Receive events** synced from other peers
- **Store events** in SQLite
- **Sync events** over gossip (no central server)

## Why Use It

- Single discovery key controls who syncs with you
- Events are deduplicated automatically
- Works offline - syncs when connected
- No configuration - just a discovery key and database path
- 280 lines of Rust

## Installation

```toml
[dependencies]
xaeroflux = { git = "https://github.com/block-xaero/xaeroflux" }
tokio = { version = "1", features = ["full"] }
```

## Basic Usage

```rust
use xaeroflux::{XaeroFlux, Event, generate_event_id};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut xf = XaeroFlux::new(
        "my-app-key".to_string(),
        "events.db".to_string()
    ).await?;

    // Send an event
    let now = chrono::Utc::now().timestamp() as u64;
    xf.event_tx.send(Event {
        id: generate_event_id("hello world", &xf.node_id, now),
        payload: "hello world".to_string(),
        source: xf.node_id.clone(),
        ts: now,
    })?;

    // Receive synced events from other peers
    while let Some(event) = xf.event_rx.recv().await {
        println!("Received: {} from {}", event.payload, event.source);
    }

    Ok(())
}
```

## Event Structure

```rust
pub struct Event {
    pub id: String,      // Unique ID (blake3 hash)
    pub payload: String, // Your data (any string)
    pub source: String,  // Node ID that created event
    pub ts: u64,        // Unix timestamp
}
```

## How It Works

1. You send an event via `event_tx`
2. XaeroFlux stores it in SQLite
3. XaeroFlux broadcasts it on gossip topic `xsp-1.0/{discovery_key}/events`
4. Other peers with same discovery key receive and store it
5. You receive synced events via `event_rx`

Events from your own node are not sent back to `event_rx` (you already have them).

## Discovery Keys

The discovery key determines which peers sync with each other:

```rust
// These sync together (same discovery key)
let peer1 = XaeroFlux::new("project-alpha".to_string(), "db1.db".to_string()).await?;
let peer2 = XaeroFlux::new("project-alpha".to_string(), "db2.db".to_string()).await?;

// This syncs separately (different discovery key)
let peer3 = XaeroFlux::new("project-beta".to_string(), "db3.db".to_string()).await?;
```
## CLI Demo

### Build

```bash
cargo build --release --bin xaeroflux-cli
```

### Terminal 1 - Sender

```bash
./target/release/xaeroflux-cli \
  --discovery-key demo \
  --db peer1.db \
  send "Hello from peer 1"
```

### Terminal 2 - Receiver

```bash
./target/release/xaeroflux-cli \
  --discovery-key demo \
  --db peer2.db \
  recv
```

The receiver will print events sent by the first peer (and any other peers on the same discovery key).

### Send Multiple Events

```bash
# Terminal 1
./target/release/xaeroflux-cli -k demo -d peer1.db send "Event 1"
./target/release/xaeroflux-cli -k demo -d peer1.db send "Event 2"
./target/release/xaeroflux-cli -k demo -d peer1.db send "Event 3"
```

### Interactive Mode

```bash
./target/release/xaeroflux-cli -k demo -d peer1.db interactive
```

Type messages and press Enter. Type `quit` to exit.

## Example: Building a Chat App

```rust
use xaeroflux::{XaeroFlux, Event, generate_event_id};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    user: String,
    message: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut xf = XaeroFlux::new("my-chat".to_string(), "chat.db".to_string()).await?;
    let node_id = xf.node_id.clone();

    // Send chat messages
    let tx = xf.event_tx.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut lines = tokio::io::BufReader::new(stdin).lines();
        
        while let Ok(Some(line)) = lines.next_line().await {
            let msg = ChatMessage {
                user: node_id[..8].to_string(),
                message: line,
            };
            let payload = serde_json::to_string(&msg).unwrap();
            let now = chrono::Utc::now().timestamp() as u64;
            
            tx.send(Event {
                id: generate_event_id(&payload, &node_id, now),
                payload,
                source: node_id.clone(),
                ts: now,
            }).unwrap();
        }
    });

    // Receive and display messages
    while let Some(event) = xf.event_rx.recv().await {
        let msg: ChatMessage = serde_json::from_str(&event.payload)?;
        println!("[{}] {}", msg.user, msg.message);
    }

    Ok(())
}
```

## Limitations

- No snapshot sync - only events broadcast after joining
- No compaction - events stored forever
- No authentication - discovery key is only access control -- See XaeroID for this.
- No ordering guarantees beyond timestamps

## Use Cases

- Collaborative apps (docs, whiteboards, spreadsheets)
- Chat applications
- Activity feeds
- Multiplayer game state sync
- IoT sensor data collection
- Distributed logging

## Protocol

**Topic**: `xsp-1.0/{discovery_key}/events`  
**Transport**: Iroh gossip  
**Format**: JSON-serialized Event struct

## License
MPL v2.0
