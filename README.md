# Xaeroflux

⚠️ **Work in progress – NOT READY FOR PRODUCTION USE** ⚠️

## Introduction

Xaeroflux is a mobile-friendly, decentralized append-only storage and indexing engine powering truly cloud-less, peer-to-peer applications. It provides:

- **Reactive event streams** using a lightweight `Subject`/`Observable` abstraction  
- **Zero-copy payloads** via raw `Vec<u8>` and optional `rkyv` archiving  
- **Append-only log** (AOF) + paged Merkle trees + Merkle Mountain Range (MMR) proofs  
- **Plug-and-play networking** over Iroh gossip/blob (Libp2p, QUIC, WebRTC, Tor)  
- **Priority routing**: System, Identity, and per-Topic application streams  

---

## Reactive Core Types

```rust
use core::event::Event;
use crate::core::listeners::EventListener;

pub type Observer   = EventListener<Vec<u8>>;
pub struct Observable {
    pub tx: crossbeam::channel::Sender<Vec<u8>>,
}
impl Observable {
    /// Push one raw event (zero-copy).
    pub fn on_next(&self, evt: Vec<u8>) {
        let _ = self.tx.send(evt);
    }
}

pub struct Subject {
    pub observers: Vec<Observer>,
    pub rx:        crossbeam::channel::Receiver<Vec<u8>>,
}
impl Subject {
    /// Create a new Subject (hot multicast source) and its Observable handle.
    pub fn new() -> (Self, Observable) {
        let (tx, rx) = crossbeam::channel::unbounded();
        (Subject { observers: Vec::new(), rx }, Observable { tx })
    }

    /// Subscribe an observer to receive every event.
    pub fn subscribe(&mut self, obs: Observer) {
        self.observers.push(obs);
    }

    /// Run the dispatch loop, broadcasting each event to all observers.
    pub fn run(self) {
        while let Ok(evt) = self.rx.recv() {
            for obs in &self.observers {
                let _ = obs.inbox.send(Event::new(evt.clone(), 1));
            }
        }
    }
}
```

---

## Quickstart

### Hello, Xaeroflux

```rust
use std::{sync::Arc, thread, time::Duration};
use crossbeam::channel;
use rkyv::{to_bytes, from_bytes};
use xaeroflux::Subject;
use xaeroflux::Observable;
use crate::core::event::Event;
use crate::core::listeners::EventListener;
use xaeroflux::EventKind;

// 1) Build the bus
let (mut subject, bus) = Subject::new();

// 2) Spawn dispatcher
thread::spawn(move || subject.run());

// 3) Subscribe to all events
let (tx, rx) = channel::unbounded::<Event<Vec<u8>>>();
let listener = EventListener::new(
    "printer",
    Arc::new(move |e: Event<Vec<u8>>| { tx.send(e).unwrap(); }),
    None, None,
);
subject.subscribe(listener);

// 4) Publish events
let raw = to_bytes::<_,256>(&Event { data: b"Hello".to_vec(), timestamp: now_ms(), kind: EventKind::Application }).unwrap().into_inner();
bus.on_next(raw.clone());
bus.on_next(raw);

// 5) Drain
thread::sleep(Duration::from_millis(50));
while let Ok(evt) = rx.try_recv() {
    let e: Event<Vec<u8>> = from_bytes(&evt.data).unwrap();
    println!("Got {:?}", e.data);
}
```

### Topic-based streams

```rust
// Create “rust” topic
let (mut rust_subj, rust_bus) = Subject::new();
thread::spawn(move || rust_subj.run());
let rust_listener = EventListener::new(
    "rust_ui",
    Arc::new(|e: Event<Vec<u8>>| println!("Rust: {:?}", e.data)),
    None, None,
);
rust_subj.subscribe(rust_listener);

let mut buf = vec![0x10];
buf.extend(b"rust");
buf.extend(b"Hello r/rust".to_vec());
rust_bus.on_next(buf);
```

---

## Architecture Overview

### 1. Serialization

- **Zero-copy** with raw `Vec<u8>`; optional `rkyv` for rich types  
- Deserialize on-demand at the application edge

### 2. Storage Layer

- **AOF**: LMDB-backed append-only log  
- **Segment Writer**: crossbeam channel → fixed-size pages → `mmap` + flush + rollover  
- **Per-page Merkle**: O(log P) proofs within pages  
- **MMR**: instant per-event proofs + minimal diffing

### 3. Networking Layer

- **Iroh gossip/blob** over Libp2p (QUIC, TCP, WebRTC) + Tor fallback  
- **Merkle sync**: exchange MMR peaks → fetch missing pages → sync done  
- **Op-based CRDT** glue optional

### 4. Runtime

- **Crossbeam** channels + per-listener thread-pools  
- **`mmap`** for efficient IO  
- **No async/await** (native threads + pools)

---

## Testing & Quality

```bash
cargo test --all
cargo fmt --all
cargo clippy --all -- -D warnings
```

---

## Contributing

1. Fork & branch  
2. Add tests & docs  
3. Open a PR  
4. Iterate

See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

Mozilla Public License 2.0  
See [LICENSE](LICENSE).
