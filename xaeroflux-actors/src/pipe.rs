use std::sync::Arc;

use crossbeam::channel::{Receiver, Sender};

use crate::{XaeroEvent, next_id, subject::Signal};

/// Helps `Subject` De-lineate control and data event flow to relevant actors.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusKind {
    Control,
    Data,
}

#[derive(Debug)]
/// Receiver side of a `Subject` channel for `XaeroEvent`s.
pub struct Source<T> {
    /// Unique identifier for this source.
    pub id: u64,
    /// Underlying Crossbeam receiver.
    pub rx: Receiver<T>,
    pub(crate) tx: Sender<T>,
    pub kind: BusKind,
}

impl<T> Source<T> {
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

#[derive(Debug)]
/// Sender side of a `Subject` channel for `XaeroEvent`s.
pub struct Sink<T> {
    /// Unique identifier for this sink.
    pub id: u64,
    /// Underlying Crossbeam sender.
    pub tx: Sender<T>,
    pub(crate) rx: Receiver<T>,
    pub kind: BusKind,
}

impl<T> Sink<T> {
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
#[derive(Clone, Debug)]
pub struct Pipe {
    pub source: Arc<Source<XaeroEvent>>,
    pub sink: Arc<Sink<XaeroEvent>>,
}

impl Pipe {
    pub fn new(kind: BusKind, bounds: Option<usize>) -> Arc<Self> {
        // TODO: use config
        Arc::new(Self {
            source: Arc::new(Source::new(bounds, kind)),
            sink: Arc::new(Sink::new(bounds, kind)),
        })
    }
}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct SignalPipe {
    pub source: Arc<Source<Signal>>,
    pub sink: Arc<Sink<Signal>>,
}

impl SignalPipe {
    pub fn new(kind: BusKind, bounds: Option<usize>) -> Arc<Self> {
        Arc::new(SignalPipe {
            source: Arc::new(Source::new(bounds, kind)),
            sink: Arc::new(Sink::new(bounds, kind)),
        })
    }
}
