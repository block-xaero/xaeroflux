use std::sync::Arc;

use crossbeam::channel::{Receiver, Sender, bounded};

use crate::{XaeroEvent, next_id};

/// Helps `Subject` De-lineate control and data event flow to relevant actors.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusKind {
    Control,
    Data,
}

/// Receiver side of a `Subject` channel for `XaeroEvent`s.
pub struct Source {
    /// Unique identifier for this source.
    pub id: u64,
    /// Underlying Crossbeam receiver.
    pub rx: Receiver<XaeroEvent>,
    pub(crate) tx: Sender<XaeroEvent>,
    pub kind: BusKind,
}

impl Source {
    /// Constructs a new `Source` from the given receiver.
    pub fn new(bounds: Option<usize>, bus_kind: BusKind) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(bounds.unwrap_or(100));
        Self {
            id: next_id(),
            rx,
            tx,
            kind: bus_kind,
        }
    }
}

/// Sender side of a `Subject` channel for `XaeroEvent`s.
pub struct Sink {
    /// Unique identifier for this sink.
    pub id: u64,
    /// Underlying Crossbeam sender.
    pub tx: Sender<XaeroEvent>,
    pub(crate) rx: Receiver<XaeroEvent>,
    pub kind: BusKind,
}

impl Sink {
    /// Constructs a new `Sink` from the given sender.
    pub fn new(bounds: Option<usize>, bus_kind: BusKind) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(bounds.unwrap_or(100));
        Self {
            id: next_id(),
            tx,
            rx,
            kind: bus_kind,
        }
    }
}

#[repr(C)]
#[derive(Clone)]
pub struct Pipe {
    pub source: Arc<Source>,
    pub sink: Arc<Sink>,
}
impl Pipe {
    pub fn new(kind: BusKind, bounds: Option<usize>) -> Arc<Self> {
        // FIXME:
        // CONF.get()
        //                 .expect("configuration_not_initialized")
        //                 .event_buffers
        //                 .get("application_event")
        //                 .expect("value_not_set")
        //                 .capacity as usize
        Arc::new(Self {
            source: Arc::new(Source::new(bounds, kind)),
            sink: Arc::new(Sink::new(bounds, kind)),
        })
    }
}
