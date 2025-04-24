use std::sync::Arc;

use crossbeam::channel::Sender;
use rkyv::Archive;
use tracing::warn;

use crate::{
    core::{
        event::{Event, EventType, SystemEventKind},
        initialize,
        listeners::EventListener,
    },
    indexing::merkle_tree::{XaeroMerkleTree, XaeroMerkleTreeOps},
};

#[repr(C)]
#[derive(Debug, Clone, Copy, Archive)]
#[rkyv(derive(Debug))]
pub struct XaeroMerkleGlobalMessage {
    pub page_index: u64,
    pub page_root: [u8; 32],
}
/// Merkle indexing actor that listens to events from reader and writer threads.
pub struct MerkleIndexingActor {
    pub merkle_tree: Arc<std::sync::RwLock<XaeroMerkleTree>>,
    pub listener: EventListener<XaeroMerkleGlobalMessage>,
}

/// Default merkle indexing actor handler
impl MerkleIndexingActor {
    pub fn new(
        name: &str,
        event_buffer_size: usize,
        reader_inbox: Sender<Event<String>>,
        pool_size_override: Option<usize>,
        rebuild: bool,
    ) -> Self {
        initialize();

        if name.is_empty() {
            panic!("name cannot be empty");
        }
        if event_buffer_size == 0 {
            panic!("event_buffer_size must be greater than 0");
        }

        if rebuild {
            warn!(
                "Sending Reader to rebuild merkle index using pages again {}",
                name
            );
            let res = reader_inbox.send(Event::new(
                "restart".to_string(),
                EventType::SystemEvent(SystemEventKind::Restart).to_u8(),
            ));
            match res {
                Ok(_) => {
                    tracing::info!("Sent restart event to reader");
                }
                Err(e) => {
                    tracing::error!("Failed to send restart event to reader: {}", e);
                }
            }
        }
        tracing::info!(
            "Creating MerkleIndexingActor: {} for target_os: {} ",
            name,
            std::env::consts::OS
        );
        let num_threads = if cfg!(any(target_os = "ios", target_os = "android")) {
            // on iOS/Android, stay single-threaded
            1
        } else {
            // on desktop, use all available cores (or clamp to some max)
            pool_size_override.unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .unwrap_or(std::num::NonZeroUsize::new(1).expect("at least one thread"))
                    .get()
            })
        };
        tracing::info!("Thread pool size: {}", num_threads);
        let global_tree = Arc::new(std::sync::RwLock::new(XaeroMerkleTree::neo(vec![])));
        let global_tree = global_tree.clone();
        MerkleIndexingActor {
            merkle_tree: global_tree.clone(),
            listener: EventListener::<XaeroMerkleGlobalMessage>::new(
                name,
                {
                    Arc::new(move |e| {
                        tracing::info!("Event: {:?}", &e);
                        let merkle_tree = global_tree.clone();
                        if let Ok(mut tree) = merkle_tree.write() {
                            tracing::info!("Inserting page: {:?}", e.data.page_root);
                            let inserted = tree.insert_root(e.data.page_root);
                            if inserted {
                                tracing::info!("Inserted page: {:?}", e.data.page_root);
                            } else {
                                tracing::warn!("Failed to insert page: {:?}", e.data.page_root);
                            }
                        } else {
                            tracing::error!("Failed to acquire write lock on merkle_tree");
                        }
                    })
                },
                Some(event_buffer_size),
                Some(num_threads),
            ),
        }
    }
}
