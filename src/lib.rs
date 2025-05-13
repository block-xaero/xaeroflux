pub mod core;
pub mod indexing;
pub mod logs;
pub mod networking;
pub mod sys;

use core::event::Event;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
};

use crossbeam::channel::{Receiver, Select, Sender, unbounded};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

#[derive(Clone)]
/// Your envelope
pub struct XaeroEvent {
    pub evt: Event<Vec<u8>>,
    pub merkle_proof: Option<Vec<u8>>,
}

/// Just holds the Rx side
pub struct Source {
    pub id: u64,
    pub rx: Receiver<XaeroEvent>,
}
impl Source {
    pub fn new(rx: Receiver<XaeroEvent>) -> Self {
        Self { id: next_id(), rx }
    }
}

/// Just holds the Tx side
pub struct Sink {
    pub id: u64,
    pub tx: Sender<XaeroEvent>,
}
impl Sink {
    pub fn new(tx: Sender<XaeroEvent>) -> Self {
        Self { id: next_id(), tx }
    }
}

/// Each operator must be a reusable Fn (not FnOnce)
#[derive(Clone)]
pub enum Operator {
    Map(Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>),
    Filter(Arc<dyn Fn(&XaeroEvent) -> bool + Send + Sync>),
    FilterMerkleProofs,
    Blackhole,
}

#[derive(Clone)]
/// Your 'topic' + pipeline description
pub struct Subject {
    pub name: String,
    pub id: u64,
    pub source: Arc<Source>,
    pub sink: Arc<Sink>,
    ops: Vec<Operator>,
}

impl Subject {
    /// Create a brand‐new subject with its own channel
    pub fn new(name: String) -> Arc<Self> {
        let (tx, rx) = unbounded();
        Arc::new(Subject {
            name,
            id: next_id(),
            source: Arc::new(Source::new(rx)),
            sink: Arc::new(Sink::new(tx)),
            ops: Vec::new(),
        })
    }
}

/// Chainable operator methods—no threads spun yet!
pub trait SubjectOps {
    fn map<F>(self: &Arc<Self>, f: F) -> Arc<Self>
    where
        F: Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static;

    fn filter<P>(self: &Arc<Self>, p: P) -> Arc<Self>
    where
        P: Fn(&XaeroEvent) -> bool + Send + Sync + 'static;

    fn filter_merkle_proofs(self: &Arc<Self>) -> Arc<Self>;
    fn blackhole(self: &Arc<Self>) -> Arc<Self>;
}

impl SubjectOps for Subject {
    fn map<F>(self: &Arc<Self>, f: F) -> Arc<Self>
    where
        F: Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Map(Arc::new(f)));
        Arc::new(new)
    }

    fn filter<P>(self: &Arc<Self>, p: P) -> Arc<Self>
    where
        P: Fn(&XaeroEvent) -> bool + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Filter(Arc::new(p)));
        Arc::new(new)
    }

    fn filter_merkle_proofs(self: &Arc<Self>) -> Arc<Self> {
        let mut new = (**self).clone();
        new.ops.push(Operator::FilterMerkleProofs);
        Arc::new(new)
    }

    fn blackhole(self: &Arc<Self>) -> Arc<Self> {
        let mut new = (**self).clone();
        new.ops.push(Operator::Blackhole);
        Arc::new(new)
    }
}

/// A handle you can drop/join later if you like
pub struct Subscription(pub JoinHandle<()>);

/// Pluggable execution model
pub trait Materializer: Send + Sync {
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Box<dyn FnMut(XaeroEvent) + Send>,
    ) -> Subscription;
}

/// Materializer that runs *one* thread per Subject
pub struct ThreadPerSubjectMaterializer;

impl Default for ThreadPerSubjectMaterializer {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPerSubjectMaterializer {
    pub fn new() -> Self {
        ThreadPerSubjectMaterializer
    }
}

impl Materializer for ThreadPerSubjectMaterializer {
    fn materialize(
        &self,
        subject: Arc<Subject>,
        mut handler: Box<dyn FnMut(XaeroEvent) + Send>,
    ) -> Subscription {
        // only need rx and ops, not tx!
        let rx = subject.source.rx.clone();
        let ops = subject.ops.clone();

        let jh = std::thread::spawn(move || {
            let mut sel = Select::new();
            sel.recv(&rx);

            while let Ok(mut evt) = sel.select().recv(&rx) {
                let mut keep = true;
                for op in &ops {
                    match op {
                        Operator::Map(f) => {
                            evt = f(evt);
                        }
                        Operator::Filter(p) if !p(&evt) => {
                            keep = false;
                            break;
                        }
                        Operator::FilterMerkleProofs if evt.merkle_proof.is_none() => {
                            keep = false;
                            break;
                        }
                        Operator::Blackhole => {
                            keep = false;
                            break;
                        }
                        _ => {}
                    }
                }
                if keep {
                    handler(evt);
                }
            }
        });

        Subscription(jh)
    }
}

/// Convenience method on Subject to pick your materializer
pub trait SubscribeWith {
    fn subscribe_with<M, F>(self: &Arc<Self>, mat: M, handler: F) -> Subscription
    where
        M: Materializer,
        F: FnMut(XaeroEvent) + Send + 'static;
}

impl SubscribeWith for Subject {
    fn subscribe_with<M, F>(self: &Arc<Self>, mat: M, handler: F) -> Subscription
    where
        M: Materializer,
        F: FnMut(XaeroEvent) + Send + 'static,
    {
        mat.materialize(self.clone(), Box::new(handler))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossbeam::channel;

    use super::*;
    use crate::core::{event::Event, initialize};

    /// Helper to make a simple XaeroEvent with optional proof
    fn make_evt(data: &[u8], with_proof: bool) -> XaeroEvent {
        XaeroEvent {
            evt: Event::new(data.to_vec(), 0),
            merkle_proof: if with_proof {
                Some(b"proof".to_vec())
            } else {
                None
            },
        }
    }

    #[test]
    fn publish_and_subscribe_roundtrip() {
        let subj = Subject::new("topic".into());
        let (tx, rx) = channel::unbounded();

        // subscribe: every incoming event sends its payload to tx
        let _sub = subj.subscribe_with(
            ThreadPerSubjectMaterializer::new(),
            move |xe: XaeroEvent| {
                tx.send(xe.evt.data.clone()).expect("failed_to_unwrap");
            },
        );

        // publish one event
        let payload = b"hello".to_vec();
        let evt = make_evt(&payload, true);
        subj.sink.tx.send(evt).expect("failed_to_unwrap");

        // expect to receive the same payload
        let got = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("did not receive event");
        assert_eq!(got, payload);
    }

    #[test]
    fn map_operator_transforms() {
        let subj = Subject::new("map".into()).map(|mut xe| {
            xe.evt.data.push(b'!'); // append an exclamation
            xe
        });

        let (tx, rx) = channel::unbounded();
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer::new(), move |xe| {
            tx.send(xe.evt.data).expect("failed_to_unwrap");
        });

        let payload = b"hey".to_vec();
        subj.sink
            .tx
            .send(make_evt(&payload, true))
            .expect("failed_to_unwrap");

        let got = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("missing mapped event");
        assert_eq!(got, b"hey!".to_vec());
    }

    #[test]
    fn filter_operator_drops() {
        initialize();
        let subj = Subject::new("filter".into()).filter(|xe| xe.evt.data[0] % 2 == 0); // only even first-byte

        let (tx, rx) = channel::unbounded();
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer::new(), move |xe| {
            tx.send(xe.evt.data[0]).expect("failed_to_unwrap");
        });

        // send odd (should drop) then even (should pass)
        subj.sink
            .tx
            .send(make_evt(&[1], true))
            .expect("failed_to_unwrap");
        subj.sink
            .tx
            .send(make_evt(&[2], true))
            .expect("failed_to_unwrap");

        // only one should get through
        let got = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("expected even event");
        assert_eq!(got, 2);
        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "no more events expected"
        );
    }

    #[test]
    fn filter_merkle_proofs_only() {
        initialize();
        let subj = Subject::new("proof".into()).filter_merkle_proofs();

        let (tx, rx) = channel::unbounded();
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer::new(), move |xe| {
            tx.send(xe.merkle_proof.is_some())
                .expect("failed_to_unwrap");
        });

        // send one without proof (dropped) and one with proof (passed)
        subj.sink
            .tx
            .send(make_evt(&[0], false))
            .expect("failed_to_unwrap");
        subj.sink
            .tx
            .send(make_evt(&[0], true))
            .expect("failed_to_unwrap");

        let got = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("expected one proof‐event");
        assert!(got);
        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "only one proof event should pass"
        );
    }

    #[test]
    fn blackhole_drops_all() {
        let subj = Subject::new("nothing".into()).blackhole();

        let (tx, rx) = channel::unbounded();
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer::new(), move |_xe| {
            tx.send(()).expect("failed_to_unwrap");
        });

        // send several events
        for i in 0..3 {
            subj.sink
                .tx
                .send(make_evt(&[i], true))
                .expect("failed_to_unwrap");
        }

        // we expect *no* events through
        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "blackhole should prevent any delivery"
        );
    }
}
