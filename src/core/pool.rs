use std::{collections::HashMap, fmt::format, sync::Arc, thread};

use mio::event::Event;
use threadpool::ThreadPool;

use super::{CONF, XaeroData};

pub struct XaeroRouter<T>
where
    T: XaeroData,
{
    pub pool: Arc<ThreadPool>,
    pub system_events: (
        crossbeam::channel::Sender<()>,
        crossbeam::channel::Receiver<()>,
    ),
    pub events: (
        crossbeam::channel::Sender<T>,
        crossbeam::channel::Receiver<T>,
    ),
    pub listeners: HashMap<u64, crossbeam::channel::Sender<T>>,
    pub id: u64,
    pub name: String,
    pub version: u64,
}

pub trait XaeroRouting<T>
where
    T: XaeroData,
{
    fn route_event(&self, event: T);
}

impl<T> XaeroRouter<T>
where
    T: XaeroData,
{
    #[tracing::instrument]
    pub fn new(id: u64, name: String, version: u64) -> Self {
        let pool = Arc::new(ThreadPool::new(
            CONF.get()
                .expect("failed to load config")
                .threads
                .num_worker_threads,
        ));
        let system_events = crossbeam::channel::unbounded();
        let events = crossbeam::channel::unbounded();
        let listeners = HashMap::new();

        XaeroRouter {
            pool,
            system_events,
            events,
            listeners,
            id,
            name,
            version,
        }
    }
}
