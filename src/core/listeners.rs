use std::{collections::BTreeMap, sync::Arc, thread::JoinHandle};

use crossbeam::channel::{Sender, *};
use libp2p::futures::executor::ThreadPool;

use super::{CONF, XaeroData, event::Event};

/// Event listener forms the crux of our event plumbing.
/// Mostly event listener is bound to a thread-pool and is organized in a tree like
/// hierarchy, an event processed spawns new events which are sent to children.
/// hierarchy also helps in filtering events.
pub struct EventListener<T>
where
    T: XaeroData + 'static,
{
    id: Option<[u8; 32]>, // auto-generated
    pub address: Option<String>,
    pub inbox: Sender<Event<T>>,
    pub rx: Receiver<Event<T>>,
    pub pool: ThreadPool,
    pub handler: Arc<dyn Fn(Event<T>) + Send + Sync>,
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
                .map(|x| format!("{:02x}", x))
                .collect::<String>()
                .to_lowercase()
                .replace("-", "_")
        )
    }
}
impl<T> EventListener<T>
where
    T: XaeroData + 'static,
{
    pub fn new(name: &str, group: String, handler: Arc<dyn Fn(Event<T>) + Send + Sync>) -> Self {
        // TODO: versioning scheme duplication - REMOVE
        let v = format!(
            "{}_{}_{}",
            group,
            name,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_micros() as u64
        );
        let (tx, rx) = crossbeam::channel::unbounded();
        let id = crate::indexing::hash::sha_256::<String>(&String::from(name))
            .try_into()
            .expect("failed to convert hash");
        let tp = ThreadPool::builder()
            .pool_size(
                CONF.get()
                    .expect("failed to load config")
                    .threads
                    .num_worker_threads,
            )
            .name_prefix(format!("xaeroflux-event-listener-{}", hex::encode(id.unwrap_or_default())))
            .create()
            .expect("failed to create thread pool");
        EventListener {
            id,
            address: None,
            inbox: tx,
            rx,
            pool: tp,
            handler,
        }
    }
}
