//! Append-only event log (AOF) actor using LMDB for persistent storage.
//!
//! This module provides:
//! - `AOFActor`: listens for events and writes them durably into LMDB.
//! - `LmdbEnv`: wrapper around the LMDB environment and databases.
//! - Functions to push events, scan event ranges, and manage metadata.

use std::sync::{Arc, Mutex};

use xaeroflux_core::listeners::EventListener;

use super::storage::lmdb::{LmdbEnv, push_event};
use crate::{
    core::{CONF, initialize},
    system::control_bus::ControlBus,
};

/// An append-only file (AOF) actor that persists events into LMDB.
///
/// The `AOFActor` encapsulates an LMDB environment and an event listener.
/// It writes incoming events to disk for durable storage and later retrieval.
pub struct AOFActor {
    pub cb: Arc<ControlBus>,
    pub env: Arc<Mutex<LmdbEnv>>,
    pub listener: EventListener<Vec<u8>>,
}

impl AOFActor {
    pub fn new(cb: Arc<ControlBus>) -> Self {
        initialize();
        let c = CONF.get().expect("failed to unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new("xaeroflux-actors-aof").expect("failed to unravel"),
        ));
        let env_c = Arc::clone(&env);
        let h = Arc::new(move |e| {
            tracing::info!("Pushing event to LMDB: {:?}", e);
            push_event(&env_c, &e).expect("failed to unravel");
        });
        Self {
            cb,
            env,
            listener: EventListener::new(
                "xaeroflux-actors-aof",
                h,
                Some(CONF.get().expect("failed to load config").aof.buffer_size),
                Some(c.aof.threads.num_worker_threads),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use xaeroflux_core::event::{Event, EventType};

    use super::*;

    #[test]
    fn test_aof_actor_initialization() {
        initialize();
        let cb = Arc::new(ControlBus::new());
        let cbc = Arc::clone(&cb);
        // Ensure we can create an AOFActor without panicking.
        let _actor = AOFActor::new(cb);
        let _actor_default: AOFActor = AOFActor::new(cbc);
    }

    #[test]
    fn test_aof_actor_push_event() {
        initialize();
        let cb = Arc::new(ControlBus::new());
        // Initialize the actor and push a sample event through its listener.
        let actor = AOFActor::new(cb);
        let sample_data = Event::new(
            b"hello world".to_vec(),
            EventType::ApplicationEvent(1).to_u8(),
        );
        // Send event to the listener's inbox; should not panic.
        actor
            .listener
            .inbox
            .send(sample_data.clone())
            .expect("Failed to send to AOFActor listener inbox");
        // Give the background thread a moment to process.
        thread::sleep(Duration::from_millis(100));
    }
}
