#[allow(deprecated)]
use std::{
    fmt::{Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use rusted_ring::AllocationError;
use sha2::digest::consts::U64;
use xaeroflux_core::{
    date_time::emit_secs,
    event::{Operator, ScanWindow, XaeroEvent},
    hash::sha_256_hash,
    next_id,
    pipe::{AtomicSignal, BusKind, Pipe, Signal, SignalPipe},
};

use crate::{
    XFluxHandle,
    aof::actor::AOFActor,
    indexing::storage::actors::{
        mmr_actor::MmrActor,
        secondary_index_actor::SecondaryIndexActor,
        segment_reader_actor::SegmentReaderActor,
        segment_writer_actor::{SegmentConfig, SegmentWriterActor},
    },
    materializer::{Materializer, ThreadPoolForSubjectMaterializer},
    system_actors::SystemActors,
};

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SubjectHash(pub [u8; 32]);
impl Display for SubjectHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        hex::encode(self.0).fmt(f)
    }
}
impl From<[u8; 32]> for SubjectHash {
    fn from(value: [u8; 32]) -> Self {
        SubjectHash(value)
    }
}
unsafe impl Pod for SubjectHash {}
unsafe impl Zeroable for SubjectHash {}

/// Subject's batch mode context that helps buffer events
/// and execute operations appropriately by remaining true to laziness.
#[derive(Clone, Debug)]
pub struct SubjectBatchContext {
    pub init_time: u64,
    pub duration: u64,
    pub event_count_threshold: Option<usize>,
    // context inputs and outputs for Control and data events that are CRDT Ops.
    pub control_pipe: Arc<Pipe>,
    pub data_pipe: Arc<Pipe>,
    pub pipeline: Vec<Operator>,
}

/// A multicast namespaced Event channel that houses:
/// - A `Pipe` (tuple of `Source` and `Sink`).
/// - A network `TopicKey` that is essentially a duplicate of `SubjectHash` but kept for evolution.
/// - Allows you to listen in and sync events transparently with others when online!
#[derive(Clone, Debug)]
pub struct Subject {
    /// Logical topic name - subject name is blake3(workspace/w_id/object/object_id)
    pub name: String,
    /// Unique hash of the subject, derived from its name.
    /// This is a 32-byte Blake3 hash.
    pub hash: SubjectHash,
    /// A workspace id
    pub workspace_id: [u8; 32],
    /// Unique subject ID.
    pub id: u64,
    /// An object id
    pub object_id: [u8; 32],
    /// Control events flow from here.
    pub control: Arc<Pipe>,
    /// data events flow from here.
    pub data: Arc<Pipe>,

    // signal pipes
    pub control_signal_pipe: Arc<SignalPipe>,
    pub data_signal_pipe: Arc<SignalPipe>,

    /// Receiver endpoint (shared).
    /// Ordered list of operators to apply per event.
    pub(crate) ops: Vec<Operator>,
    /// batch context holds buffer (`Pipe`) for events and operators that are applicable to batch
    /// mode.
    pub batch_context: Option<SubjectBatchContext>,
    /// loads true if batch mode is set -- all events once this is set go to SubjectBatchContext
    pub batch_mode: Arc<AtomicBool>,

    /// READ ONLY are system actors materialized?
    pub(crate) system_actor_materialized: Arc<AtomicBool>,
    /// read only for mode set - can be `Signal` Buffer, Streaming, Blackhole or Kill.
    pub(crate) mode_set: Arc<AtomicSignal>,
}

pub trait SubjectBatchOps {
    /// Buffers events in a duration window or `event_count_threshold` provided.
    /// With a pipeline of operators passed in. These operators are usually:
    /// Sort -> Fold or Reduce
    /// This machinery helps with things like support of CRDT Ops naturally in `Subject`
    /// type.
    fn buffer<F>(
        self: &Arc<Self>,
        duration: Duration,
        event_count_threshold: Option<usize>,
        pipeline: Vec<Operator>,
        route_with: Arc<F>,
    ) -> Arc<Self>
    where
        F: Fn(&Arc<XaeroEvent>) -> bool + Send + Sync + 'static;

    /// Sorts buffered events
    fn sort<F>(self: &Arc<Self>, sorter: Arc<F>) -> Arc<Self>
    where
        F: Fn(&Arc<XaeroEvent>, &Arc<XaeroEvent>) -> std::cmp::Ordering + Send + Sync + 'static;

    /// folds a `Vector` buffer of `XaeroEvent` to a single `XaeroEvent` packed in as a buffer with
    /// event header in archived form.
    fn fold_left<F>(self: &Arc<Self>, fold_left: Arc<F>) -> Arc<Self>
    where
        F: Fn(Arc<Option<XaeroEvent>>, Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError>
            + Send
            + Sync
            + 'static;

    /// Reduces vector of xaero events to a single XaeroEvent (stays in event domain).
    fn reduce<F>(self: &Arc<Self>, reducer: Arc<F>) -> Arc<Self>
    where
        F: Fn(Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError> + Send + Sync + 'static;
}

/// Chainable subject operators without executing them until subscription.
pub trait SubjectStreamingOps {
    fn scan<F>(self: &Arc<Self>, scan_window: Arc<ScanWindow>) -> Arc<Self>
    where
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static;

    /// Transform each event.
    fn map<F>(self: &Arc<Self>, f: Arc<F>) -> Arc<Self>
    where
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static;

    /// Keep only events matching the predicate.
    fn filter<P>(self: &Arc<Self>, p: Arc<P>) -> Arc<Self>
    where
        P: Fn(&Arc<XaeroEvent>) -> bool + Send + Sync + 'static;

    /// Drop events lacking a Merkle proof.
    fn filter_merkle_proofs(self: &Arc<Self>) -> Arc<Self>;

    /// Terminal: drop all events.
    fn blackhole(self: &Arc<Self>) -> Arc<Self>;
}

impl SubjectStreamingOps for Subject {
    fn scan<F>(self: &Arc<Self>, scan_window: Arc<ScanWindow>) -> Arc<Self>
    where
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Scan(scan_window));
        Arc::new(new)
    }

    fn map<F>(self: &Arc<Self>, f: Arc<F>) -> Arc<Self>
    where
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Map(f));
        Arc::new(new)
    }

    fn filter<P>(self: &Arc<Self>, p: Arc<P>) -> Arc<Self>
    where
        P: Fn(&Arc<XaeroEvent>) -> bool + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.ops.push(Operator::Filter(p));
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

impl SubjectBatchOps for Subject {
    fn buffer<F>(
        self: &Arc<Self>,
        duration: Duration,
        event_count_threshold: Option<usize>,
        pipeline: Vec<Operator>,
        route_with: Arc<F>,
    ) -> Arc<Self>
    where
        F: Fn(&Arc<XaeroEvent>) -> bool + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        new.batch_context = Some(SubjectBatchContext {
            init_time: emit_secs(),
            duration: duration.as_secs(),
            event_count_threshold,
            data_pipe: Pipe::new(BusKind::Data, None),
            control_pipe: Pipe::new(BusKind::Control, None),
            pipeline: pipeline.clone(),
        });
        new.ops.push(Operator::BufferMode(
            duration,
            event_count_threshold,
            pipeline,
            route_with,
        ));
        Arc::new(new)
    }

    fn sort<F>(self: &Arc<Self>, sorter: Arc<F>) -> Arc<Self>
    where
        F: Fn(&Arc<XaeroEvent>, &Arc<XaeroEvent>) -> std::cmp::Ordering + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        /// still lazy ~~ woohoo! ~~~
        new.ops.push(Operator::Sort(sorter));
        Arc::new(new)
    }

    fn fold_left<F>(self: &Arc<Self>, fold_left: Arc<F>) -> Arc<Self>
    where
        F: Fn(Arc<Option<XaeroEvent>>, Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError>
            + Send
            + Sync
            + 'static,
    {
        let mut new = (**self).clone();
        /// still lazy ~~ woohoo! ~~~
        new.ops.push(Operator::Fold(fold_left));
        Arc::new(new)
    }

    fn reduce<F>(self: &Arc<Self>, reducer: Arc<F>) -> Arc<Self>
    where
        F: Fn(Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError> + Send + Sync + 'static,
    {
        let mut new = (**self).clone();
        /// still lazy ~~ woohoo! ~~~
        new.ops.push(Operator::Reduce(reducer));
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
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static;
}

impl SubscribeWith for Subject {
    fn subscribe_with<M, F>(self: &Arc<Self>, mat: M, handler: F) -> Subscription
    where
        M: Materializer,
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static,
    {
        mat.materialize(self.clone(), Arc::new(handler))
    }
}

impl Subject {
    pub fn new_with_workspace(name: String, h: [u8; 32], workspace_id: String, object_id: String) -> Arc<Self> {
        Arc::new(Subject {
            name,
            hash: SubjectHash(h),
            id: next_id(),
            control: Pipe::new(BusKind::Control, None),
            data: Pipe::new(BusKind::Data, None),
            control_signal_pipe: SignalPipe::new(BusKind::Control, Some(1)),
            data_signal_pipe: SignalPipe::new(BusKind::Data, Some(1)),
            ops: Vec::new(),
            system_actor_materialized: Arc::new(AtomicBool::new(false)),
            workspace_id: sha_256_hash(workspace_id.as_bytes().to_vec()),
            object_id: sha_256_hash(object_id.as_bytes().to_vec()),
            batch_mode: Arc::new(AtomicBool::new(false)), // by default its false.
            batch_context: None,
            mode_set: Arc::new(AtomicSignal::new(
                Signal::from_u8(0).expect("cannot set an uninit mode from atomic signal - must panic!"),
            )),
        })
    }

    pub fn is_batch_mode(&self) -> bool {
        self.batch_mode.load(Ordering::SeqCst)
    }

    pub fn signal_set(&self) -> Signal {
        self.mode_set.load(Ordering::SeqCst)
    }

    /// Subscribe with a callback, using the default thread-pool materializer.
    ///
    /// Spawns a listener thread and processes incoming events through `ops`.
    pub fn subscribe<F>(self: &Arc<Self>, handler: F) -> Subscription
    where
        F: Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static,
    {
        self.subscribe_with(ThreadPoolForSubjectMaterializer::new(), handler)
    }

    pub(crate) fn setup_system_actors(self: Arc<Self>) -> Result<SystemActors, Box<dyn std::error::Error>> {
        if self.system_actor_materialized.load(Ordering::SeqCst) {
            return Err("System actor materialized is already initialized".into());
        }

        tracing::debug!("Setting up system actors for Subject: {}", self.name);
        tracing::debug!("Initializing ring buffer actors");

        self.system_actor_materialized.store(true, Ordering::SeqCst);

        // Get subject hash for actor initialization
        let subject_hash = self.hash;

        // Create AOF actors (still using old Pipe system for now)
        let control_aof = Arc::new(AOFActor::new(self.hash, self.control.clone()));
        let data_aof = Arc::new(AOFActor::new(self.hash, self.data.clone()));

        // Create NEW ring buffer segment writer actors
        let control_seg_writer =
            SegmentWriterActor::spin(subject_hash, BusKind::Control, Some(SegmentConfig::default()))?;
        let data_seg_writer = SegmentWriterActor::spin(subject_hash, BusKind::Data, Some(SegmentConfig::default()))?;

        // Create NEW ring buffer MMR actor for data bus (with segment writer integration)
        let data_mmr = MmrActor::spin(
            subject_hash,
            BusKind::Data,
            Some(SegmentConfig::default()), // Include segment writer integration
        )?;

        // Create NEW ring buffer secondary index actor
        // Reuse the same LMDB environment used by AOFActor
        let secondary_lmdb_env = data_aof.env.clone();
        let data_secondary_indexer = SecondaryIndexActor::spin(
            subject_hash,
            BusKind::Data,
            secondary_lmdb_env,
            Some(Duration::from_secs(300)), // 5 minute TTL
        )?;

        // Create segment reader actors (still using old system for now)
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

        tracing::info!("System actors initialized successfully for Subject: {}", self.name);
        tracing::info!("Ring buffer actors: MMR, Segment Writers (Control/Data), Secondary Index - ✅");
        tracing::info!("Legacy Pipe actors: AOF (Control/Data), Segment Readers (Control/Data) - 🚧");

        Ok(SystemActors {
            control_aof,
            data_aof,
            control_seg_writer,
            data_seg_writer,
            data_mmr,
            data_secondary_indexer,
            control_seg_reader,
            data_seg_reader,
        })
    }
}
