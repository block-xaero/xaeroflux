use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use crossbeam::channel::{Receiver, Sender};

use crate::{event::XaeroEvent, next_id};

/// Helps `Subject` De-lineate control and data event flow to relevant actors.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusKind {
    Control,
    Data,
}

/// Subject control signals
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Signal {
    Blackhole = 0,
    Kill = 1,
    ControlBlackhole = 2,
    ControlKill = 3,
}

impl Signal {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Signal::Blackhole),
            1 => Some(Signal::Kill),
            2 => Some(Signal::ControlBlackhole),
            3 => Some(Signal::ControlKill),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct AtomicSignal(AtomicU8);

impl AtomicSignal {
    pub fn new(signal: Signal) -> Self {
        Self(AtomicU8::new(signal as u8))
    }

    pub fn store(&self, signal: Signal, ordering: Ordering) {
        self.0.store(signal as u8, ordering);
    }

    pub fn load(&self, ordering: Ordering) -> Signal {
        Signal::from_u8(self.0.load(ordering)).expect("corrupted atomic signal")
    }

    pub fn compare_exchange(
        &self,
        current: Signal,
        new: Signal,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Signal, Signal> {
        match self
            .0
            .compare_exchange(current as u8, new as u8, success, failure)
        {
            Ok(prev) => Ok(Signal::from_u8(prev).expect("corrupted signal")),
            Err(prev) => Err(Signal::from_u8(prev).expect("corrupted signal")),
        }
    }
}

#[derive(Debug)]
/// Receiver side of a `Subject` channel for `XaeroEvent`s.
pub struct Source<T> {
    /// Unique identifier for this source.
    pub id: u64,
    /// Underlying Crossbeam receiver.
    pub rx: Receiver<T>,
    pub tx: Sender<T>,
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
    pub rx: Receiver<T>,
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
#[derive(Debug)]
pub struct GenPipe<T> {
    pub source: Arc<Source<T>>,
    pub sink: Arc<Sink<T>>,
}

#[repr(C)]
#[derive(Clone)]
pub struct Pipe {
    pub source: Arc<Source<Arc<XaeroEvent>>>,
    pub sink: Arc<Sink<Arc<XaeroEvent>>>,
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

    pub fn single() -> Arc<Self> {
        Arc::new(SignalPipe {
            source: Arc::new(Source::new(Some(1), BusKind::Control)),
            sink: Arc::new(Sink::new(Some(1), BusKind::Control)),
        })
    }
}
