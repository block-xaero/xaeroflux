use std::{
    fmt::Debug,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, atomic::AtomicUsize},
    thread::JoinHandle,
};

use crossbeam::channel::Sender;
use threadpool::ThreadPool;

use super::{DISPATCHER_POOL, XaeroData, event::Event, init_global_dispatcher_pool};

pub struct EventListener<T>
where
    T: XaeroData + Send + Sync + 'static,
{
    pub id: [u8; 32], // auto-generated
    pub address: Option<String>,
    pub(crate) inbox: Sender<Event<T>>,
    pub pool: ThreadPool,
    pub(crate) dispatcher: Option<JoinHandle<()>>,
    pub meta: Arc<EventListenerMeta>,
}

#[derive(Debug, Clone)]
pub struct EventListenerMeta {
    pub events_processed: Arc<AtomicUsize>,
    pub events_dropped: Arc<AtomicUsize>,
}
pub trait VersioningScheme {
    fn emit_version(&self, seed_name: &str, seed_group: &str) -> String;
}
impl<T> VersioningScheme for EventListener<T>
where
    T: XaeroData + 'static,
{
    fn emit_version(&self, seed_name: &str, seed_group: &str) -> String {
        format!(
            "{}_{}_{}",
            seed_group,
            seed_name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_micros() as u64
        )
    }
}
pub trait AddressingScheme {
    fn emit_address(&self, seed_name: &str, seed_group: &str) -> String;
}

impl<T> AddressingScheme for EventListener<T>
where
    T: XaeroData,
{
    fn emit_address(&self, seed_name: &str, seed_group: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            seed_group
                .to_lowercase()
                .replace(" ", "_")
                .replace("-", "_"),
            seed_name.to_lowercase().replace(" ", "_").replace("-", "_"),
            self.emit_version(seed_name, seed_group),
            crate::indexing::hash::sha_256::<String>(&seed_name.to_string())
                .to_vec()
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect::<String>()
                .to_lowercase()
                .replace("-", "_")
        )
    }
}

impl<T> EventListener<T>
where
    T: XaeroData + Send + Sync + 'static,
{
    pub fn new(
        name: &str,
        handler: Arc<dyn Fn(Event<T>) + Send + Sync + 'static>,
        event_buffer_size: Option<usize>,
        _pool_size_override: Option<usize>,
    ) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded();

        let id: [u8; 32] = crate::indexing::hash::sha_256::<String>(&name.to_string());
        // let pool_size = match pool_size_override {
        //     Some(size) => {
        //         tracing::info!("Thread pool size: {}", size);
        //         size
        //     }
        //     None => {
        //         tracing::info!("Thread pool size: default");
        //         let config = CONF.get_or_init(|| Config::default());
        //         config.threads.num_worker_threads.max(1)
        //     }
        // };
        // FIXME: This IGNORES the pool size override
        // INTENTIONAL - REFACTOR ON M3.
        init_global_dispatcher_pool();
        let events_processed = Arc::new(AtomicUsize::new(0));
        let events_dropped = Arc::new(AtomicUsize::new(0));
        let meta = Arc::new(EventListenerMeta {
            events_processed,
            events_dropped,
        });
        let f_meta_c = meta.clone();
        let tp = DISPATCHER_POOL
            .get()
            .expect("dispatcher pool not initialized");
        let moveable = tp.clone();
        let dispatcher = std::thread::Builder::new()
            .name(format!("xaeroflux-event-listener-{}", hex::encode(id)))
            .spawn(move || {
                while let Ok(event) = rx.recv() {
                    let meta_c = meta.clone();
                    let h = Arc::clone(&handler);
                    moveable.execute(move || {
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
            id,
            address: None,
            inbox: tx,
            pool: tp.clone(),
            dispatcher: Some(dispatcher),
            meta: f_meta_c,
        }
    }

    pub fn shutdown(self) {
        tracing::info!("shutting down event listener");
        self.pool.join();
        drop(self.inbox);
        match self.dispatcher {
            Some(handle) => {
                handle.join().expect("failed to join dispatcher thread");
            }
            None => {
                tracing::warn!("dispatcher thread already shut down");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::initialize;

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
        listener
            .inbox
            .send(Event::<String>::new("test".to_string(), 0))
            .expect("failed to send event");
        assert_eq!(listener.id.len(), 32);
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
