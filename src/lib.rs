//! A simple Reactive Subject/Observable implementation using crossbeam channels
//!
//! This module defines two primary types:
//!  - `Observable` (a sink for events)
//!  - `Subject` (a hot multicast source)
//!
//! # Example
//!
//! ```
use core::event::{ArchivedEvent, ArchivedEventType, Event, EventType};

use rkyv::rancor::Failure;

use crate::core::listeners::EventListener;

pub mod core;
pub mod indexing;
pub mod logs;
pub mod networking;
pub mod sys;

pub type Observer = EventListener<Vec<u8>>;

/// An `Observable` is a stream of events that can be observed.
/// It is a sink for events, allowing you to push events into it.
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

impl Drop for Subject {
    fn drop(&mut self) {
        tracing::info!("dropping subject");
        // Move each observer out and shutdown
        for obs in self.observers.drain(..) {
            obs.shutdown();
            tracing::info!("shutting down observer");
        }
    }
}
// Define Operator as a trait object type alias
pub type Operator = Box<dyn FnOnce(Subject) -> Subject + Send + 'static>;


#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use crossbeam::channel;

    use super::*;
    use crate::core::{event::Event, initialize, listeners::EventListener};

    #[test]
    fn test_subject_new_and_observable() {
        let (subject, observable) = Subject::new();
        // Subject should have zero observers initially
        assert_eq!(subject.observers.len(), 0);
        // Observable::on_next should not panic
        observable.on_next(b"hello".to_vec());
    }

    #[test]
    fn test_subscribe_and_publish() {
        // Initialize core (in case it sets up logging or other state)
        initialize();

        let (mut subject, observable) = Subject::new();
        // Set up a channel to receive events from the EventListener
        let (listener_tx, listener_rx) = channel::unbounded();
        // Create a default EventListener that sends events to listener_tx
        let listener = EventListener::new(
            "test-listener",
            Arc::new(move |evt: Event<Vec<u8>>| {
                let _ = listener_tx.send(evt);
            }),
            None,
            Some(1),
        );
        subject.subscribe(listener);

        // Spawn the subject dispatch loop
        let _handle = thread::spawn(move || {
            subject.run();
        });

        // Publish an event
        let test_data = b"test-message".to_vec();
        observable.on_next(test_data.clone());

        // Wait for the event to arrive at the listener
        let received = listener_rx.recv_timeout(Duration::from_secs(1));
        assert!(received.is_ok(), "Did not receive event in time");
        let event = received.expect("Failed to receive event");
        assert_eq!(event.data, test_data);

        // Optionally, drop the observable to end the dispatch loop
        drop(observable);
        // Give the dispatch loop a moment to exit
        thread::sleep(Duration::from_millis(50));
        // No need to join the thread, as it should exit when rx is closed
    }
}
