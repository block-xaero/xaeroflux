//! Event listener actor for xaeroflux-actors.
//!
//! This module provides:
//! - `EventListener<T>`: an actor that receives events via a channel, dispatches them to worker
//!   threads, and tracks metrics.
//! - `EventListenerMeta`: statistics on processed and dropped events.
//! - `VersioningScheme` and `AddressingScheme` traits for generating unique listener versions and
//!   addresses.

use std::{
    fmt::Debug,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, atomic::AtomicUsize},
    thread::JoinHandle,
};

use crossbeam::channel::Sender;
use threadpool::ThreadPool;

use crate::{
    DISPATCHER_POOL, XaeroData,
    event::{Event, EventType::SystemEvent, SystemEventKind::Shutdown},
    init_global_dispatcher_pool,
};

/// An asynchronous event listener actor.
///
/// Listens for `Event<T>` on an unbounded crossbeam channel, spawns a
/// dedicated dispatcher thread to pull events, and uses a shared
/// thread pool (`ThreadPool`) to execute user-provided handlers.
///
/// Fields:
/// - `id`: unique listener ID (SHA-256 hash of the name).
/// - `address`: optional hierarchical address in the event graph.
/// - `inbox`: sender side of the event channel.
/// - `pool`: shared thread pool for handler execution.
/// - `dispatcher`: handle to the thread receiving from `inbox`.
/// - `meta`: shared metrics (processed / dropped counts).
pub struct EventListener<T>
where
    T: XaeroData + Send + Sync + 'static,
{
    pub inbox: Sender<Event<T>>,
    pub pool: ThreadPool,
    pub dispatcher: Option<JoinHandle<()>>,
    pub meta: Arc<EventListenerMeta>,
}

/// Metadata for an EventListener, tracking event processing metrics.
///
/// - `events_processed`: number of events successfully handled.
/// - `events_dropped`: number of events that panicked or failed.
#[derive(Debug, Clone)]
pub struct EventListenerMeta {
    pub events_processed: Arc<AtomicUsize>,
    pub events_dropped: Arc<AtomicUsize>,
}

impl<T> EventListener<T>
where
    T: XaeroData + Send + Sync + 'static,
{
    /// Constructs a new `EventListener`.
    ///
    /// # Arguments
    /// - `name`: human-readable name for the listener (used to generate `id`).
    /// - `handler`: `Arc<Fn(Event<T>)>` closure to process each event.
    /// - `_event_buffer_size`: optional capacity hint (currently unused).
    /// - `_pool_size_override`: optional override for thread pool size.
    ///
    /// Initializes the global dispatcher pool, spawns a dispatcher thread
    /// that receives events and schedules them on the pool, catching panics
    /// to increment `events_dropped`.
    pub fn new(
        name: &str,
        handler: Arc<dyn Fn(Event<T>) + Send + Sync + 'static>,
        _event_buffer_size: Option<usize>,
        _pool_size_override: Option<usize>,
    ) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<Event<T>>();
        let id: [u8; 32] = crate::hash::sha_256::<String>(&name.to_string());
        // Choose pool:
        let pool: ThreadPool = if let Some(size) = _pool_size_override {
            ThreadPool::new(size)
        } else {
            init_global_dispatcher_pool();
            DISPATCHER_POOL
                .get()
                .expect("Dispatcher pool not initialized")
                .clone()
        };
        let poolc = pool.clone();
        let events_processed = Arc::new(AtomicUsize::new(0));
        let events_dropped = Arc::new(AtomicUsize::new(0));
        let meta = Arc::new(EventListenerMeta {
            events_processed,
            events_dropped,
        });
        let f_meta_c = meta.clone();
        let dispatcher = std::thread::Builder::new()
            .name(format!(
                "xaeroflux-actors-event-listener-{}",
                hex::encode(id)
            ))
            .spawn(move || {
                while let Ok(event) = rx.recv() {
                    if event.event_type == SystemEvent(Shutdown) {
                        // if handler is in a loop - they must clean themselves.
                        break;
                    }
                    let meta_c = meta.clone();
                    let h = Arc::clone(&handler);
                    pool.execute(move || {
                        let res = panic::catch_unwind(AssertUnwindSafe(|| (h)(event)));
                        match res {
                            Ok(_) => {
                                meta_c
                                    .events_processed
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                tracing::info!("event processed");
                            }
                            Err(e) => {
                                tracing::error!("event processing failed: {:?}", e);
                                meta_c
                                    .events_dropped
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                    });
                }
            })
            .expect("failed to spawn dispatcher thread");
        EventListener {
            inbox: tx,
            pool: poolc,
            dispatcher: Some(dispatcher),
            meta: f_meta_c,
        }
    }

    /// Gracefully shuts down the listener.
    ///
    /// Drops the inbox to stop receiving new events, joins the dispatcher
    /// thread, and waits for all pending handler tasks to complete.
    pub fn shutdown(self) {
        tracing::info!("shutting down event listener");
        drop(self.inbox);
        // wait for the dispatcher thread to finish scheduling any remaining events
        if let Some(handle) = self.dispatcher {
            handle.join().expect("failed to join dispatcher thread");
        }
        // now wait for *all* the processing tasks to complete
        self.pool.join();
    }
}

/// Unit tests for the `EventListener` actor,
/// verifying event dispatch, metrics, and shutdown behavior.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::initialize;

    #[test]
    fn test_event_listener() {
        initialize();
        let listener = EventListener::<String>::new(
            "test",
            Arc::new(|event| {
                println!("Received event: {:?}", event.data);
            }),
            None,
            None,
        );
        let res = listener
            .inbox
            .send(Event::<String>::new("test".to_string(), 0));
        assert!(res.is_ok());
        listener.shutdown();
    }

    #[test]
    fn test_event_listener_multiple_events() {
        initialize();
        let listener = EventListener::<String>::new(
            "test",
            Arc::new(move |event| println!("Received event: {:#?}", event)),
            Some(5),
            Some(3),
        );
        let m_c = listener.meta.clone();
        for i in 0..9 {
            listener
                .inbox
                .send(Event::<String>::new(format!("test-{}", i), 0))
                .expect("failed to send event");
        }
        listener.shutdown();
        assert_eq!(
            m_c.events_processed
                .load(std::sync::atomic::Ordering::SeqCst),
            9
        );
    }

    #[test]
    fn test_event_pushes_with_buffer_limits() {
        initialize();
        let listener = EventListener::<String>::new(
            "test",
            Arc::new(|event| {
                println!("Received event: {:#?}", event);
            }),
            Some(2),
            None, // Added missing pool_size_override argument
        );
        for i in 0..10 {
            listener
                .inbox
                .send(Event::<String>::new(format!("test-{}", i), 0))
                .expect("failed to send event");
        }
        let mc = listener.meta.clone();
        listener.shutdown();
        assert_eq!(
            mc.events_processed
                .load(std::sync::atomic::Ordering::SeqCst),
            10
        );
    }
}
