use std::{
    fmt::{Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    thread::JoinHandle,
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use crossbeam::channel::{SendError, unbounded};
use futures::task::SpawnExt;
use xaeroflux_core::{XAERO_DISPATCHER_POOL, hash::sha_256_hash};

use crate::{
    Operator, ScanWindow, XFluxHandle, XaeroEvent,
    aof::actor::AOFActor,
    indexing::storage::{
        actors::{
            mmr_actor::MmrIndexingActor,
            secondary_index_actor::SecondaryIndexActor,
            segment_reader_actor::SegmentReaderActor,
            segment_writer_actor::{SegmentConfig, SegmentWriterActor},
        },
        format::archive,
    },
    materializer::{Materializer, ThreadPoolForSubjectMaterializer},
    next_id,
    pipe::{BusKind, Pipe},
};

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubjectHash(pub [u8; 32]);
impl Display for SubjectHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(&self.0).fmt(f)
    }
}
impl From<[u8; 32]> for SubjectHash {
    fn from(value: [u8; 32]) -> Self {
        SubjectHash(value)
    }
}
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

/// A multicast namespaced Event channel that houses:
/// - A `Pipe` (tuple of `Source` and `Sink`).
/// - A network `TopicKey` that is essentially a duplicate of `SubjectHash` but kept for evolution.
/// - Allows you to listen in and sync events transparently with others when online!
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
    /// Control events flow from here.
    pub control: Arc<Pipe>,
    /// data events flow from here.
    pub data: Arc<Pipe>,
    /// Unique subject ID.
    pub id: u64,
    /// Receiver endpoint (shared).
    /// Ordered list of operators to apply per event.
    pub(crate) ops: Vec<Operator>,
    // /// Flag to indicate if `unsafe_run` has been called.
    /// 2-fold: 1. ensure unsafe run is called atleast once 2. do not double call.
    pub unsafe_run_called: Arc<AtomicBool>,
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

impl Subject {
    pub fn new_with_workspace(
        name: String,
        h: [u8; 32],
        workspace_id: String,
        object_id: String,
    ) -> Arc<Self> {
        Arc::new(Subject {
            name,
            hash: SubjectHash(h),
            topic_key: TopicKey { flags: 1 },
            id: next_id(),
            control: Pipe::new(BusKind::Control, None),
            data: Pipe::new(BusKind::Data, None),
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
        // 4) hook up the actor against the same project_root
        let (tx, rx) = unbounded::<XaeroEvent>();
        let control_pipe = self.control.clone();
        tracing::debug!("control bus initialized");
        self.unsafe_run_called.store(true, Ordering::SeqCst);
        // Instantiate system actors
        let subject_hash = self.hash;
        let control_aof = Arc::new(AOFActor::new(self.hash, self.control.clone()));
        let data_aof = Arc::new(AOFActor::new(self.hash, self.data.clone()));
        let control_seg_writer = Arc::new(SegmentWriterActor::new(self.hash, self.control.clone()));
        let data_seg_writer = Arc::new(SegmentWriterActor::new(self.hash, self.data.clone()));
        let data_mmr = Arc::new(MmrIndexingActor::new(self.hash, self.data.clone(), None));
        // Create the secondary-index actor
        // Reuse the same LMDB environment used by AOFActor:
        let secondary_lmdb_env = data_aof.env.clone();
        let data_secondary_indexer = SecondaryIndexActor::new(
            self.data.clone(),
            secondary_lmdb_env,
            Duration::from_secs(60),
        );
        let control_seg_reader = Arc::new(SegmentReaderActor::new(
            subject_hash,
            self.control.clone(),
            SegmentConfig::default(),
        ));
        let data_seg_reader = Arc::new(SegmentReaderActor::new(
            subject_hash,
            self.data.clone(),
            SegmentConfig::default(),
        ));

        // Subscribe system pipeline
        let _sys_sub_control = Arc::new(self.clone().subscribe_with(
            ThreadPoolForSubjectMaterializer::new(),
            move |control_xaero_event: XaeroEvent| {
                let res = control_aof.pipe.sink.tx.send(control_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }
                let res = control_seg_writer
                    .pipe
                    .sink
                    .tx
                    .send(control_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }

                let res = control_seg_reader
                    .pipe
                    .sink
                    .tx
                    .send(control_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }
            },
        ));

        let _sys_sub_data = Arc::new(self.clone().subscribe_with(
            ThreadPoolForSubjectMaterializer::new(),
            move |data_xaero_event: XaeroEvent| {
                let res = data_aof.pipe.sink.tx.send(data_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }
                let res = data_seg_writer.pipe.sink.tx.send(data_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }

                let res = data_seg_reader.pipe.sink.tx.send(data_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }

                let res = data_secondary_indexer
                    .pipe
                    .sink
                    .tx
                    .send(data_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }

                let res = data_mmr.pipe.sink.tx.send(data_xaero_event.clone());
                match res {
                    Ok(_) => {}
                    Err(_) => {
                        tracing::error!("failed to send xaero event to control segment reader");
                    }
                }
            },
        ));
        XFluxHandle {
            _sys_sub_data,
            _sys_sub_control,
        }
    }
}
