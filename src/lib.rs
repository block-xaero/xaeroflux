pub mod core;
pub mod indexing;
pub mod logs;
pub mod networking;
pub mod sys;
pub mod system;
use core::{DISPATCHER_POOL, aof::AOFActor, event::Event};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
};

use bytemuck::{Pod, Zeroable};
use crossbeam::channel::{Receiver, Sender, unbounded};
use indexing::storage::{
    actors::{
        mmr_actor::MmrIndexingActor,
        segment_reader_actor::SegmentReaderActor,
        segment_writer_actor::{SegmentConfig, SegmentWriterActor},
    },
    format::archive,
};
use system::{CONTROL_BUS, init_control_bus};
use threadpool::ThreadPool;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a unique, thread-safe `u64` ID.
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ScanWindow {
    pub start: u64,
    pub end: u64,
}
unsafe impl Pod for ScanWindow {}
unsafe impl Zeroable for ScanWindow {}

/// Envelope wrapping an application or system `Event` payload
/// along with an optional Merkle inclusion proof.
#[derive(Clone)]
pub struct XaeroEvent {
    /// Core event data (e.g., domain event encoded as bytes).
    pub evt: Event<Vec<u8>>,
    /// Optional Merkle proof bytes (e.g., from MMR).
    pub merkle_proof: Option<Vec<u8>>,
}

/// Receiver side of a `Subject` channel for `XaeroEvent`s.
pub struct Source {
    /// Unique identifier for this source.
    pub id: u64,
    /// Underlying Crossbeam receiver.
    pub rx: Receiver<XaeroEvent>,
}

impl Source {
    /// Constructs a new `Source` from the given receiver.
    pub fn new(rx: Receiver<XaeroEvent>) -> Self {
        Self { id: next_id(), rx }
    }
}

/// Sender side of a `Subject` channel for `XaeroEvent`s.
pub struct Sink {
    /// Unique identifier for this sink.
    pub id: u64,
    /// Underlying Crossbeam sender.
    pub tx: Sender<XaeroEvent>,
}

impl Sink {
    /// Constructs a new `Sink` from the given sender.
    pub fn new(tx: Sender<XaeroEvent>) -> Self {
        Self { id: next_id(), tx }
    }
}

/// Defines per-event pipeline operations.
#[derive(Clone)]
pub enum Operator {
    Scan(Arc<ScanWindow>),
    /// Transform the event into another event.
    Map(Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>),
    /// Keep only events matching the predicate.
    Filter(Arc<dyn Fn(&XaeroEvent) -> bool + Send + Sync>),
    /// Drop events without a Merkle proof.
    FilterMerkleProofs,
    /// Terminal op: drop all events.
    Blackhole,
}

/// A hot multicast channel with a configurable operator pipeline.
#[derive(Clone)]
pub struct Subject {
    /// Logical topic name.
    pub name: String,
    /// Unique subject ID.
    pub id: u64,
    /// Receiver endpoint (shared).
    pub source: Arc<Source>,
    /// Sender endpoint (shared).
    pub sink: Arc<Sink>,
    /// Ordered list of operators to apply per event.
    ops: Vec<Operator>,
    // /// Flag to indicate if `unsafe_run` has been called.
    /// 2 fold: 1. ensure unsafe run is called atleast once 2. do not double call.
    pub unsafe_run_called: Arc<AtomicBool>,
}

impl Subject {
    /// Creates a new `Subject` with its own unbounded channel.
    ///
    /// # Arguments
    ///
    /// * `name` - human-readable identifier for the subject.
    ///
    /// # Returns
    ///
    /// An `Arc<Subject>` you can share and configure.
    pub fn new(name: String) -> Arc<Self> {
        let (tx, rx) = unbounded();
        Arc::new(Subject {
            name,
            id: next_id(),
            source: Arc::new(Source::new(rx)),
            sink: Arc::new(Sink::new(tx)),
            ops: Vec::new(),
            unsafe_run_called: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Subscribe with a callback, using the default thread-pool materializer.
    ///
    /// Spawns a listener thread and processes incoming events through `ops`.
    pub fn subscribe<F>(self: &Arc<Self>, handler: F) -> Subscription
    where
        F: Fn(XaeroEvent) + Send + Sync + 'static,
    {
        self.subscribe_with(ThreadPoolForSubjectMaterializer::new(), handler)
    }

    /// Connects this `Subject` to system actors: AOF, segment writer, and MMR.
    ///
    /// Returns an `XFluxHandle` that keeps everything alive until dropped.
    pub fn unsafe_run(self: Arc<Self>) -> XFluxHandle {
        tracing::debug!("unsafe_run called for Subject: {}", self.name);
        tracing::debug!("initializing control bus");
        init_control_bus();
        tracing::debug!("control bus initialized");
        self.unsafe_run_called.store(true, Ordering::SeqCst);
        // Instantiate system actors
        let aof = Arc::new(AOFActor::new());
        let seg = Arc::new(SegmentWriterActor::new());
        let mmr = Arc::new(MmrIndexingActor::new(None, None));
        let seg_reader_actor = Arc::new(SegmentReaderActor::new(
            SegmentConfig::default(),
            self.sink.clone(),
        ));
        let aof_c = aof.clone();
        let seg_c = seg.clone();
        let mmr_c = mmr.clone();
        // Subscribe system pipeline
        let _sys_sub = Arc::new(self.clone().subscribe_with(
            ThreadPerSubjectMaterializer,
            move |xe: XaeroEvent| {
                seg_reader_actor
                    .inbox
                    .send(xe.evt.clone())
                    .expect("failed_to_unwrap");
                // Append-only log
                let evt = xe.evt.clone();
                let blob = archive(&xe.evt);
                aof_c
                    .listener
                    .inbox
                    .send(evt.clone())
                    .expect("failed_to_unwrap");

                // Segment archiving
                seg_c.inbox.send(blob).expect("failed_to_unwrap");

                // MMR indexing
                mmr_c.listener.inbox.send(evt).expect("failed_to_unwrap");

                // P2P sync would go here
            },
        ));

        XFluxHandle {
            _sys_sub,
            aof,
            seg,
            mmr,
        }
    }
}

/// Chainable subject operators without executing them until subscription.
pub trait SubjectOps {
    fn scan<F>(self: &Arc<Self>, scan_window: ScanWindow) -> Arc<Self>
    where
        F: Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static;

    /// Transform each event.
    fn map<F>(self: &Arc<Self>, f: F) -> Arc<Self>
    where
        F: Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static;

    /// Keep only events matching the predicate.
    fn filter<P>(self: &Arc<Self>, p: P) -> Arc<Self>
    where
        P: Fn(&XaeroEvent) -> bool + Send + Sync + 'static;

    /// Drop events lacking a Merkle proof.
    fn filter_merkle_proofs(self: &Arc<Self>) -> Arc<Self>;

    /// Terminal: drop all events.
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

    fn scan<F>(self: &Arc<Self>, scan_window: ScanWindow) -> Arc<Self>
    where
        F: Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Scan(Arc::new(scan_window)));
        Arc::new(new)
    }
}

/// Keeps a spawned thread alive for a subscription.
pub struct Subscription(pub JoinHandle<()>);

/// Strategy for wiring `Subject` + pipeline into threads and invoking handlers.
pub trait Materializer: Send + Sync {
    /// Materialize the pipeline, passing each processed event into `handler`.
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) + Send + Sync + 'static>,
    ) -> Subscription;
}

/// Materializer using a shared thread pool per subject.
pub struct ThreadPoolForSubjectMaterializer {
    pool: Arc<ThreadPool>,
}

impl Default for ThreadPoolForSubjectMaterializer {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPoolForSubjectMaterializer {
    /// Builds from global configuration.
    pub fn new() -> Self {
        Self {
            pool: Arc::new(DISPATCHER_POOL.get().expect("failed to load pool").clone()),
        }
    }
}

impl Materializer for ThreadPoolForSubjectMaterializer {
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) + Send + Sync + 'static>,
    ) -> Subscription {
        let rx = subject.source.rx.clone();
        let ops = subject.ops.clone();
        let pool = self.pool.clone();

        let jh = std::thread::spawn(move || {
            // before incoming events are starting to come in
            // we need to funnel in Subject sink with Scan
            if ops.iter().any(|op| matches!(&op, Operator::Scan(_))) {
                let scan_window = ops
                    .iter()
                    .find_map(|op| {
                        if let Operator::Scan(w) = op {
                            Some(w.clone())
                        } else {
                            None
                        }
                    })
                    .expect("scan_window_not_found");
                // fail because something is wrong!

                let sw = &scan_window.start.to_be_bytes();
                let data = bytemuck::bytes_of(sw);
                use core::event::EventType;
                // Ensure Replay is imported or defined
                use core::event::SystemEventKind;
                subject
                    .sink
                    .tx
                    .send(XaeroEvent {
                        evt: Event::new(
                            data.to_vec(),
                            EventType::SystemEvent(SystemEventKind::Replay).to_u8(),
                        ),
                        merkle_proof: None,
                    })
                    .expect("failed_to_unwrap");
            }
            for evt in rx.iter() {
                let h = handler.clone();
                let pipeline = ops.clone();
                let mut local_evt = evt.clone();
                pool.execute(move || {
                    let mut keep = true;
                    for op in &pipeline {
                        match op {
                            Operator::Map(f) => local_evt = f(local_evt),
                            Operator::Filter(p) if !p(&local_evt) => {
                                keep = false;
                                break;
                            }
                            Operator::FilterMerkleProofs if local_evt.merkle_proof.is_none() => {
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
                        h(local_evt)
                    }
                });
            }
        });
        Subscription(jh)
    }
}

/// Default thread-per-subject materializer (no pooling).
pub struct ThreadPerSubjectMaterializer;

impl Materializer for ThreadPerSubjectMaterializer {
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) + Send + Sync + 'static>,
    ) -> Subscription {
        let rx = subject.source.rx.clone();
        let ops = subject.ops.clone();

        let jh = std::thread::spawn(move || {
            for mut evt in rx.iter() {
                let mut keep = true;
                for op in &ops {
                    match op {
                        Operator::Map(f) => evt = f(evt),
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
                    handler(evt)
                }
            }
        });
        Subscription(jh)
    }
}

/// Convenience API for choosing a materializer during subscription.
pub trait SubscribeWith {
    fn subscribe_with<M, F>(self: &Arc<Self>, mat: M, handler: F) -> Subscription
    where
        M: Materializer,
        F: Fn(XaeroEvent) + Send + Sync + 'static;
}

impl SubscribeWith for Subject {
    fn subscribe_with<M, F>(self: &Arc<Self>, mat: M, handler: F) -> Subscription
    where
        M: Materializer,
        F: Fn(XaeroEvent) + Send + Sync + 'static,
    {
        mat.materialize(self.clone(), Arc::new(handler))
    }
}

/// Holds your system actors and subscription to keep them alive.
pub struct XFluxHandle {
    /// Composite subscription for core events.
    pub _sys_sub: Arc<Subscription>,
    /// Append-only log actor.
    pub aof: Arc<AOFActor>,
    /// Segment writer actor.
    pub seg: Arc<SegmentWriterActor>,
    /// MMR indexing actor.
    pub mmr: Arc<MmrIndexingActor>,
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytemuck::bytes_of;
    use crossbeam::channel;
    use memmap2::MmapMut;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        core::{
            aof::{LmdbEnv, push_event},
            date_time::emit_secs,
            event::{Event, EventType, SystemEventKind},
            initialize,
            meta::SegmentMeta,
        },
        indexing::storage::{
            actors::{
                segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentConfig,
            },
            format::PAGE_SIZE,
        },
    };

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
    fn test_source_sink() {
        initialize();
        let sub = Subject::new("posts".into());
        let payload = b"hello".to_vec();
        let evt = make_evt(&payload, true);
        tracing::debug!("sending event: {:?}", hex::encode(&evt.evt.data));
        sub.sink.tx.send(evt).expect("failed_to_unwrap");
        let got = sub
            .source
            .rx
            .recv_timeout(Duration::from_millis(100))
            .expect("did not receive event");
        tracing::debug!("got event: {:?}", hex::encode(&got.evt.data));
        assert_eq!(got.evt.data, payload);
    }

    #[test]
    fn publish_and_subscribe_roundtrip() {
        let subj = Subject::new("topic".into());
        let (tx, rx) = channel::unbounded();

        // subscribe: every incoming event sends its payload to tx
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer, move |xe: XaeroEvent| {
            tx.send(xe.evt.data.clone()).expect("failed_to_unwrap");
        });

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
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer, move |xe| {
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
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer, move |xe| {
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
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer, move |xe| {
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
        let _sub = subj.subscribe_with(ThreadPerSubjectMaterializer, move |_xe| {
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

    #[test]
    fn test_segment_reader_replay_then_live() {
        initialize();
        let tmp = tempdir().expect("failed to create tempdir");
        // write a segment file
        let ts = emit_secs();
        let idx = 0;
        let seg_path = tmp.path().join(format!("xaeroflux-{}-{}.seg", ts, idx));
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .read(true)
                .open(&seg_path)
                .expect("open seg file");
            file.set_len(PAGE_SIZE as u64).expect("set_len");
            let mut mmap = unsafe { MmapMut::map_mut(&file).expect("mmap") };
            let e = Event::new(b"hello".to_vec(), EventType::ApplicationEvent(1).to_u8());
            let frame = archive(&e);
            mmap[..frame.len()].copy_from_slice(&frame);
            mmap.flush().expect("flush");
        }
        // push meta into LMDB
        let meta_env = Arc::new(Mutex::new(
            LmdbEnv::new(tmp.path().to_str().expect("failed_to_unwrap")).expect("create lmdb"),
        ));
        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: idx,
            ts_start: ts,
            ts_end: ts,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 0,
        };
        let ev = Event::new(
            bytes_of(&seg_meta).to_vec(),
            EventType::MetaEvent(1).to_u8(),
        );
        push_event(&meta_env, &ev).expect("push_event");
        // instantiate actor
        let (tx, rx) = crossbeam::channel::unbounded::<XaeroEvent>();
        let sink = Arc::new(Sink::new(tx));
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };
        let actor = SegmentReaderActor::new(config, sink.clone());
        // send replay event
        let replay_evt = Event::new(
            Vec::new(),
            EventType::SystemEvent(SystemEventKind::Replay).to_u8(),
        );
        actor._listener.inbox.send(replay_evt).expect("send replay");
        // receive and assert first (should be "hello")
        let first = rx.recv_timeout(Duration::from_secs(1)).expect("recv first");
        assert_eq!(first.evt.data, b"hello".to_vec());
    }
}
