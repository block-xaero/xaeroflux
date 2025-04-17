use std::collections::HashMap;

use crossbeam::channel::*;

use super::event::EventType;
use crate::core::{XaeroData, event::Event};

pub struct EventListener<T>
where
    T: XaeroData,
{
    pub id: usize,
    pub filter: EventType,
    pub rx: Receiver<Event<T>>,
    pub handler: Box<dyn Fn(Event<T>) + Send + Sync>,
}

pub struct EventListenerRouter<T>
where
    T: XaeroData,
{
    pub listeners: HashMap<usize, EventListener<T>>,
    pub next_id: usize,
    pub inbox: Receiver<Event<T>>,
}

pub trait EventListenerRouterOps<T>
where
    T: XaeroData,
{
    fn add_listener<F>(&mut self, filter: EventType, handler: F) -> usize
    where
        F: Fn(Event<T>) + Send + Sync + 'static;
    fn start(&self) -> Result<(), String>;
    fn stop(&self) -> Result<(), String>;
}
