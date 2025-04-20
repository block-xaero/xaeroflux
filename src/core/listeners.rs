use std::{collections::HashMap, sync::Arc, thread::JoinHandle};

use crossbeam::channel::{Sender, *};

use super::{XaeroData, event::Event};

/// Event listener forms the crux of our event plumbing.
/// Mostly event listener is bound to a thread-pool and is organized in a tree like
/// hierarchy, an event processed spawns new events which are sent to children.
/// hierarchy also helps in filtering events.
pub struct EventListener<T>
where
    T: XaeroData + 'static,
{
    pub id: u64,
    pub rx: Receiver<Event<T>>,
    children: HashMap<JoinHandle<()>, Sender<Event<T>>>,
    pub handler: Arc<dyn Fn(Event<T>) + Send + Sync>,
}

impl<T> EventListener<T>
where
    T: XaeroData + 'static,
{
    pub fn new(
        id: u64,
        rx: Receiver<Event<T>>,
        children: HashMap<JoinHandle<()>, Sender<Event<T>>>,
        handler: Arc<dyn Fn(Event<T>) + Send + Sync>,
    ) -> Self {
        EventListener {
            id,
            children,
            rx,
            handler,
        }
    }
}

pub fn bootstrap_listener<T>(
    id: u64,
    rx: Receiver<Event<T>>,
    children: Vec<Sender<Event<T>>>,
    handler: Arc<dyn Fn(Event<T>) + Send + Sync>,
) -> EventListener<T>
where
    T: XaeroData + 'static,
{
    let j = std::thread::spawn(move || {
        while let Ok(event) = rx.recv() {
            (handler)(event);
        }
    });
    EventListener::new(id, rx, children, handler)
}
