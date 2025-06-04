pub mod aof;
pub mod indexing;
pub mod networking;
pub mod system;
use core::DISPATCHER_POOL;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use aof::actor::AOFActor;
use bytemuck::{Pod, Zeroable};
use crossbeam::channel::{Receiver, Sender, unbounded};
use indexing::storage::{
    actors::{
        mmr_actor::MmrIndexingActor,
        secondary_index_actor::SecondaryIndexActor,
        segment_reader_actor::SegmentReaderActor,
        segment_writer_actor::{SegmentConfig, SegmentWriterActor},
    },
    format::archive,
};
use system::control_bus::ControlBus;
use threadpool::ThreadPool;
use xaeroflux_core as core;
use xaeroflux_core::{event::Event, hash::sha_256_hash};

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

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubjectHash(pub [u8; 32]);

unsafe impl Pod for SubjectHash {}
unsafe impl Zeroable for SubjectHash {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct TopicKey {
    /// bit 1: is_crdt
    /// bit 2: is_private
    /// bit 3-8: reserved  -- we might need more flags in the future
    pub flags: u8,
}
unsafe impl Pod for TopicKey {}
unsafe impl Zeroable for TopicKey {}
/// A hot multicast channel with a configurable operator pipeline.
#[derive(Clone)]
pub struct Subject {
    /// Logical topic name - subject name is blake3(workspace/w_id/object/object_id)
    pub name: String,
    /// Unique hash of the subject, derived from its name.
    /// This is a 32-byte Blake3 hash.
    pub hash: SubjectHash,

    /// A topic key
    pub topic_key: TopicKey,

    /// A workspace id
    pub workspace_id: [u8; 32],

    /// An object id
    pub object_id: [u8; 32],

    /// Unique subject ID.
    pub id: u64,
    /// Receiver endpoint (shared).
    pub source: Arc<Source>,
    /// Sender endpoint (shared).
    pub sink: Arc<Sink>,
    /// Ordered list of operators to apply per event.
    ops: Vec<Operator>,
    // /// Flag to indicate if `unsafe_run` has been called.
    /// 2-fold: 1. ensure unsafe run is called atleast once 2. do not double call.
    pub unsafe_run_called: Arc<AtomicBool>,
}

impl Subject {
    pub fn new_with_workspace(
        name: String,
        h: [u8; 32],
        workspace_id: String,
        object_id: String,
    ) -> Arc<Self> {
        let (tx, rx) = unbounded();
        Arc::new(Subject {
            name,
            hash: SubjectHash(h),
            topic_key: TopicKey { flags: 1 },
            id: next_id(),
            source: Arc::new(Source::new(rx)),
            sink: Arc::new(Sink::new(tx)),
            ops: Vec::new(),
            unsafe_run_called: Arc::new(AtomicBool::new(false)),
            workspace_id: sha_256_hash(workspace_id.as_bytes().to_vec()),
            object_id: sha_256_hash(object_id.as_bytes().to_vec()),
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
        let cb = Arc::new(ControlBus::new());
        tracing::debug!("control bus initialized");
        self.unsafe_run_called.store(true, Ordering::SeqCst);
        // Instantiate system actors
        let aof = Arc::new(AOFActor::new(cb.clone()));
        let seg = Arc::new(SegmentWriterActor::new(cb.clone()));
        let mmr = Arc::new(MmrIndexingActor::new(cb.clone(), None, None));
        // Create the secondary-index actor
        // Reuse the same LMDB environment used by AOFActor:
        let secondary_lmdb_env = aof.env.clone();
        let secondary_indexer =
            SecondaryIndexActor::new(cb.clone(), secondary_lmdb_env, Duration::from_secs(60));
        let seg_reader_actor = Arc::new(SegmentReaderActor::new(
            cb.clone(),
            SegmentConfig::default(),
            self.sink.clone(),
        ));
        let aof_c = aof.clone();
        let seg_c = seg.clone();
        let mmr_c = mmr.clone();
        // Subscribe system pipeline
        let _sys_sub = Arc::new(self.clone().subscribe_with(
            ThreadPoolForSubjectMaterializer::new(),
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
            secondary_indexer,
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
    fn scan<F>(self: &Arc<Self>, scan_window: ScanWindow) -> Arc<Self>
    where
        F: Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Scan(Arc::new(scan_window)));
        Arc::new(new)
    }

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
                use xaeroflux_core::event::EventType;
                // Ensure Replay is imported or defined
                use xaeroflux_core::event::SystemEventKind;
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
    /// Secondary indexer actor.
    pub secondary_indexer: Arc<SecondaryIndexActor>,
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytemuck::bytes_of;
    use iroh_blobs::store::bao_tree::blake3;
    use memmap2::MmapMut;
    use tempfile::tempdir;
    use xaeroflux_core::{
        date_time::emit_secs,
        event::{Event, EventType, SystemEventKind},
    };
    use xaeroflux_macros::subject;

    use super::*;
    use crate::{
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, push_event},
        },
        core::initialize,
        indexing::storage::{
            actors::{
                segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentConfig,
            },
            format::PAGE_SIZE,
        },
    };

    #[test]
    fn test_subject_macro() {
        initialize();
        let expected_sha256: [u8; 32] =
            xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let subject =
            subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");
        let workspace_created_event = subject
            .source
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed")
            .evt;
        assert_eq!(
            workspace_created_event.event_type,
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated)
        );
        assert_eq!(expected_sha256.as_ref(), subject.workspace_id.as_slice());
    }
    #[test]
    fn test_segment_reader_replay_then_live() {
        initialize();
        let cb = Arc::new(ControlBus::new());
        let tmp = tempdir().expect("failed to create tempdir");
        // write a segment file
        let ts = emit_secs();
        let idx = 0;
        let seg_path = tmp
            .path()
            .join(format!("xaeroflux-actors-{}-{}.seg", ts, idx));
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
        let (tx, rx) = unbounded::<XaeroEvent>();
        let sink = Arc::new(Sink::new(tx));
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };
        let actor = SegmentReaderActor::new(cb, config, sink.clone());
        // send replay event
        let replay_evt = Event::new(
            Vec::new(),
            EventType::SystemEvent(xaeroflux_core::event::SystemEventKind::Replay).to_u8(),
        );
        actor._listener.inbox.send(replay_evt).expect("send replay");
        // receive and assert first (should be "hello")
        let first = rx.recv_timeout(Duration::from_secs(1)).expect("recv first");
        assert_eq!(first.evt.data, b"hello".to_vec());
    }

    #[test]
    fn test_subject_macro_object_event_and_hash() {
        initialize();
        // Compute expected SHA-256 for workspace and object
        let expected_ws_sha: [u8; 32] =
            xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let expected_obj_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash(
            "cyan_object_white_board_id_134".as_bytes().to_vec(),
        );

        // Now compute the combined Blake3 hash exactly as the macro does:
        let mut hasher_ws = blake3::Hasher::new();
        hasher_ws.update("cyan_workspace_123".as_bytes());
        let ws_blake = hasher_ws.finalize();
        let mut hasher_obj = blake3::Hasher::new();
        hasher_obj.update("cyan_object_white_board_id_134".as_bytes());
        let obj_blake = hasher_obj.finalize();
        let mut combined_hasher = blake3::Hasher::new();
        combined_hasher.update(ws_blake.as_bytes());
        combined_hasher.update(obj_blake.as_bytes());
        let expected_combined: [u8; 32] = *combined_hasher.finalize().as_bytes();

        // Invoke the macro
        let subject =
            subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");

        // 1) First event was WorkspaceCreated; drain it
        let ws_evt = subject
            .source
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed")
            .evt;
        assert_eq!(
            ws_evt.event_type,
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated)
        );
        // Its payload should be the Blake3 of workspace (not SHA-256)
        let mut hasher_ws_payload = blake3::Hasher::new();
        hasher_ws_payload.update("cyan_workspace_123".as_bytes());
        let ws_blake_payload = hasher_ws_payload.finalize();
        assert_eq!(ws_evt.data, ws_blake_payload.as_bytes().to_vec());

        // 2) Second event should be ObjectCreated
        let obj_evt = subject
            .source
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed")
            .evt;
        assert_eq!(
            obj_evt.event_type,
            EventType::SystemEvent(SystemEventKind::ObjectCreated)
        );
        // Its payload should be the Blake3 of object (not SHA-256)
        let mut hasher_obj_payload = blake3::Hasher::new();
        hasher_obj_payload.update("cyan_object_white_board_id_134".as_bytes());
        let obj_blake_payload = hasher_obj_payload.finalize();
        assert_eq!(obj_evt.data, obj_blake_payload.as_bytes().to_vec());

        // 3) Check that the Subject struct fields match expectations:
        //    - workspace_id field holds the SHA-256
        assert_eq!(subject.workspace_id.as_slice(), expected_ws_sha.as_ref());
        //    - object_id field holds the SHA-256
        assert_eq!(subject.object_id.as_slice(), expected_obj_sha.as_ref());
        //    - hash field (SubjectHash) is the combined Blake3
        assert_eq!(subject.hash.0, expected_combined);
    }

    #[test]
    fn test_subject_macro_name_and_topic_key_flags() {
        initialize();
        // Use a different workspace/object pair
        let workspace = "MyWS";
        let object = "MyObj";

        // Compute SHA-256 for both
        let ws_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash(workspace.as_bytes().to_vec());
        let obj_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash(object.as_bytes().to_vec());
        // Combine via Blake3 as macro does
        let ws_blake = {
            let mut h = blake3::Hasher::new();
            h.update(workspace.as_bytes());
            h.finalize()
        };
        let obj_blake = {
            let mut h = blake3::Hasher::new();
            h.update(object.as_bytes());
            h.finalize()
        };
        let combined_blake = {
            let mut h = blake3::Hasher::new();
            h.update(ws_blake.as_bytes());
            h.update(obj_blake.as_bytes());
            *h.finalize().as_bytes()
        };

        let subject = subject!("workspace/MyWS/object/MyObj");
        // Name field should equal the literal used
        assert_eq!(subject.name, "workspace/MyWS/object/MyObj");
        // topic_key.flags was set to 1 in new_with_workspace
        assert_eq!(subject.topic_key.flags, 1);
        // workspace_id and object_id bytes checked above in previous test
        assert_eq!(subject.workspace_id.as_slice(), ws_sha.as_ref());
        assert_eq!(subject.object_id.as_slice(), obj_sha.as_ref());
        // hash field must match combined Blake3
        assert_eq!(subject.hash.0, combined_blake);
    }
}
