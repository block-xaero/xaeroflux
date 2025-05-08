//! A simple Reactive Subject/Observable implementation using crossbeam channels
//!
//! This module defines two primary types:
//!  - `Observable` (a sink for events)
//!  - `Subject` (a hot multicast source)
//!
//! # Example
//!
//! ```
use core::event::Event;

use crate::core::listeners::EventListener;

pub mod core;
pub mod indexing;
pub mod logs;
pub mod networking;
pub mod sys;

pub type Observer = EventListener<Vec<u8>>;
pub struct Observable {
    pub tx: crossbeam::channel::Sender<Vec<u8>>,
}

impl Observable {
    pub fn on_next(&self, evt: Vec<u8>) {
        let _ = self.tx.send(evt);
    }
}

pub struct Subject {
    pub observers: Vec<Observer>,
    pub rx: crossbeam::channel::Receiver<Vec<u8>>,
}

impl Subject {
    /// Create a new Subject (and its Observable handle).
    pub fn new() -> (Self, Observable) {
        let (tx, rx) = crossbeam::channel::unbounded();
        let observable = Observable { tx };
        let subject = Subject {
            observers: Vec::new(),
            rx,
        };
        (subject, observable)
    }

    /// Register a new observer (an EventListener) to fan events into.
    pub fn subscribe(&mut self, obs: Observer) {
        // we don't hold the obs forever here—just push it into our list
        self.observers.push(obs);
    }

    /// Start the dispatch loop.  
    /// This should be spawned onto a thread (or task) and run forever.
    pub fn run(self) {
        while let Ok(evt) = self.rx.recv() {
            // Fan-out: push the same Event into every observer's inbox
            for obs in &self.observers {
                let ec = evt.clone();
                let res = obs.inbox.send(Event::new(ec, 1));
                match res {
                    Ok(_) => {
                        tracing::info!("event dispatched to observer");
                    }
                    Err(e) => {
                        tracing::error!("failed to dispatch event: {:?}", e);
                    }
                }
            }
        }
    }
}
