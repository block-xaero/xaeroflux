use std::sync::Arc;

use bytemuck::{Pod, Zeroable};
use crossbeam::channel::{Receiver, Sender};
use xaeroflux_macros::PipeKind;

use crate::{
    aof::storage::format::SegmentMeta, indexing::storage::mmr::Peak, pipe::BusKind, system_payload::SystemPayload,
};

// TODO: Maybe in future make `Source` and `Sink` and `Pipe` generic.
struct NetworkSource {
    pub(crate) rx: Receiver<NetworkPayload>,
    pub(crate) tx: Sender<NetworkPayload>,
}

struct NetworkSink {
    pub(crate) tx: Sender<NetworkPayload>,
    pub(crate) rx: Receiver<NetworkPayload>,
}

struct NetworkPipe {
    pub source: NetworkSource,
    pub sink: NetworkSink,
    pub bus_kind: BusKind,
    pub bounds: Option<usize>,
}

#[derive(PipeKind)]
#[pipe_kind(Control)]
pub struct ControlNetworkPipe(Arc<NetworkPipe>);

#[derive(PipeKind)]
#[pipe_kind(Data)]
pub struct DataNetworkPipe(Arc<NetworkPipe>);

#[repr(C)]
#[derive(Clone, Copy)]
pub enum NetworkPayload {
    PeerDiscovered([u8; 32], [u8; ADDRESS_MAX]),
    PeerLost([u8; 32], [u8; ADDRESS_MAX]),
    Peaks(u64, [u8; 32]),
    SegmentRolledOver(SegmentMeta),
    SegmentRolledOverFailed(SegmentMeta, u16),
    JoinedTopic(u32),
    LeftTopic(u32),
    MMRLeafAppended([u8; 32]),
}
unsafe impl Pod for NetworkPayload {}
unsafe impl Zeroable for NetworkPayload {}

impl From<NetworkPayload> for SystemPayload {
    fn from(event: NetworkPayload) -> Self {
        match event {
            NetworkPayload::PeerDiscovered(peer_id, address) => SystemPayload::PeerDiscovered { peer_id, address },
            NetworkPayload::PeerLost(peer_id, address) => SystemPayload::PeerLost { peer_id, address },
            NetworkPayload::Peaks(peak_count, root_hash) => SystemPayload::PeaksWritten { root_hash, peak_count },
            NetworkPayload::SegmentRolledOver(seg_meta) => SystemPayload::SegmentMetaChanged { meta: seg_meta },
            NetworkPayload::JoinedTopic(topic_id) => SystemPayload::JoinedTopic { topic_id },
            NetworkPayload::LeftTopic(topic) => SystemPayload::LeftTopic { topic_id: topic },
            NetworkPayload::MMRLeafAppended(leaf_hash) => SystemPayload::MMRLeafAppended { leaf_hash },
            NetworkPayload::SegmentRolledOverFailed(segment_meta, error_code) => SystemPayload::SegmentRollOverFailed {
                meta: segment_meta,
                error_code,
            },
        }
    }
}

impl From<SystemPayload> for NetworkPayload {
    fn from(payload: SystemPayload) -> Self {
        match payload {
            SystemPayload::PeerDiscovered { peer_id, address } => NetworkPayload::PeerDiscovered(peer_id, address),
            SystemPayload::PeerLost { peer_id, address } => NetworkPayload::PeerLost(peer_id, address),
            SystemPayload::PeaksWritten { peak_count, root_hash } => NetworkPayload::Peaks(peak_count, root_hash),
            SystemPayload::SegmentRollOverFailed {
                meta: seg_meta,
                error_code,
            } => NetworkPayload::SegmentRolledOverFailed(seg_meta, error_code),
            SystemPayload::JoinedTopic { topic_id } => NetworkPayload::JoinedTopic(topic_id),
            SystemPayload::LeftTopic { topic_id } => NetworkPayload::LeftTopic(topic_id),
            SystemPayload::MMRLeafAppended { leaf_hash } => NetworkPayload::MMRLeafAppended(leaf_hash),
            SystemPayload::PayloadWritten { leaf_hash, .. } => NetworkPayload::MMRLeafAppended(leaf_hash),
            SystemPayload::SegmentRolledOver { meta } => NetworkPayload::SegmentRolledOver(meta),
            SystemPayload::PageFlushed { .. } => {
                // No direct NetworkPayload variant, so fallback to a default or skip
                // For now, just panic to indicate unhandled case
                panic!("No NetworkPayload variant for PageFlushed")
            }
            SystemPayload::PageFlushFailed { .. } => {
                // No direct NetworkPayload variant, so fallback to a default or skip
                panic!("No NetworkPayload variant for PageFlushFailed")
            }
            SystemPayload::MmrAppended { leaf_hash } => NetworkPayload::MMRLeafAppended(leaf_hash),
            SystemPayload::MmrAppendFailed { .. } => {
                // No direct NetworkPayload variant, so fallback to a default or skip
                panic!("No NetworkPayload variant for MmrAppendFailed")
            }
            SystemPayload::SecondaryIndexWritten { .. } => {
                // No direct NetworkPayload variant, so fallback to a default or skip
                panic!("No NetworkPayload variant for SecondaryIndexWritten")
            }
            SystemPayload::SecondaryIndexFailed { .. } => {
                // No direct NetworkPayload variant, so fallback to a default or skip
                panic!("No NetworkPayload variant for SecondaryIndexFailed")
            }
            SystemPayload::SegmentMetaChanged { meta } => NetworkPayload::SegmentRolledOver(meta),
            // FIXME: IMPLEMENT P2P SYNC FOR THIS.
            SystemPayload::SubjectCreated { .. }
            | SystemPayload::WorkspaceCreated { .. }
            | SystemPayload::ObjectCreated { .. } => todo!(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum DataEvent {
    Event { from: Peer, event: XaeroGossipEvent },
    PageRequest { from: Peer, request: PageRequest },
    PageResponse { from: Peer, response: PageResponse },
    ProofRequest { from: Peer, request: ProofRequest },
    ProofResponse { from: Peer, response: ProofResponse },
    SyncDone(Peer),
}

#[derive(Debug)]
pub enum XaeroP2PError {
    ControlPlaneError,
    DataPlaneError,
    DiscoveryError,
    SendError(String),
}

pub type NodeId = [u8; 32];
pub const ADDRESS_MAX: usize = 128;

#[derive(Debug, Copy, Clone)]
pub struct Peer {
    pub id: NodeId,
    pub address: [u8; ADDRESS_MAX],
}
unsafe impl Pod for Peer {}
unsafe impl Zeroable for Peer {}

#[derive(Clone)]
pub struct XaeroGossipEvent {
    pub ts_micros: u64,
    pub kind: u8,
    pub event: crate::XaeroEvent,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct PageRequest {
    pub segment_index: usize,
    pub page_index: usize,
}
unsafe impl Pod for PageRequest {}
unsafe impl Zeroable for PageRequest {}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct PageResponse {
    pub segment_index: usize,
    pub page_index: usize,
    pub page: Vec<u8>,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ProofRequest {
    pub leaf_index: u64,
}
unsafe impl Pod for ProofRequest {}
unsafe impl Zeroable for ProofRequest {}

#[repr(C)]
#[derive(Clone, Debug)]
pub struct ProofResponse {
    pub leaf_index: u64,
    pub proof: Vec<[u8; 32]>,
}
pub trait XaeroPlane {
    fn join_topic(&self, topic: &str) -> Result<(), XaeroP2PError>;
    fn leave_topic(&self, topic: &str) -> Result<(), XaeroP2PError>;
}

pub trait XaeroControlPlane {
    fn announce_peaks(&self, peaks: Vec<Peak>, root_hash: [u8; 32]) -> Result<(), XaeroP2PError>;
    fn publish_segment_rolled_over(&self, seg_meta: SegmentMeta) -> Result<(), XaeroP2PError>;
    fn publish_segment_rolled_over_failed(&self, seg_meta: SegmentMeta, error_code: u16) -> Result<(), XaeroP2PError>;
    fn subscribe_control(&self) -> Receiver<NetworkPayload>;
}

pub const MAX_TOPIC_NAME_LEN: usize = 32;
pub const MAX_TOPICS_PER_GROUP: usize = 8;
pub const MAX_GROUP_NAME_LEN: usize = 32;

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum XaeroPlaneKind {
    ControlPlane,
    DataPlane,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct XaeroTopic {
    pub name_len: u8,
    pub name: [u8; MAX_TOPIC_NAME_LEN],
    pub plane: XaeroPlaneKind,
}
unsafe impl Pod for XaeroTopic {}
unsafe impl Zeroable for XaeroTopic {}

impl XaeroTopic {
    pub fn new(name_str: &str, plane: XaeroPlaneKind) -> Self {
        let bytes = name_str.as_bytes();
        let mut buf = [0u8; MAX_TOPIC_NAME_LEN];
        let name_len = if bytes.len() > MAX_TOPIC_NAME_LEN {
            &bytes[..MAX_TOPIC_NAME_LEN]
        } else {
            bytes
        };
        buf[..name_len.len()].copy_from_slice(name_len);

        XaeroTopic {
            name_len: name_len.len() as u8,
            name: buf,
            plane,
        }
    }

    pub fn as_str(&self) -> &str {
        let valid = &self.name[..self.name_len as usize];
        std::str::from_utf8(valid).unwrap_or("<invalid-utf8>")
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct XaeroTopicGroup {
    pub group_name_len: u8,
    pub group_name: [u8; MAX_GROUP_NAME_LEN],
    pub plane: XaeroPlaneKind,
    pub topic_count: u8,
    pub topics: [XaeroTopic; MAX_TOPICS_PER_GROUP],
}
unsafe impl Pod for XaeroTopicGroup {}
unsafe impl Zeroable for XaeroTopicGroup {}

impl XaeroTopicGroup {
    pub fn new(group_name_str: &str, plane: XaeroPlaneKind) -> Self {
        let bytes = group_name_str.as_bytes();
        let mut name_buf = [0u8; MAX_GROUP_NAME_LEN];
        let name_len = if bytes.len() > MAX_GROUP_NAME_LEN {
            &bytes[..MAX_GROUP_NAME_LEN]
        } else {
            bytes
        };
        name_buf[..name_len.len()].copy_from_slice(name_len);

        XaeroTopicGroup {
            group_name_len: name_len.len() as u8,
            group_name: name_buf,
            plane,
            topic_count: 0,
            topics: [XaeroTopic::new("", plane); MAX_TOPICS_PER_GROUP],
        }
    }

    pub fn add_topic(&mut self, topic: XaeroTopic) -> Result<(), XaeroP2PError> {
        if (self.topic_count as usize) < MAX_TOPICS_PER_GROUP {
            let idx = self.topic_count as usize;
            self.topics[idx] = topic;
            self.topic_count += 1;
            Ok(())
        } else {
            Err(XaeroP2PError::ControlPlaneError)
        }
    }

    pub fn name(&self) -> &str {
        let valid = &self.group_name[..self.group_name_len as usize];
        std::str::from_utf8(valid).unwrap_or("<invalid-utf8>")
    }

    pub fn iter_topics(&self) -> impl Iterator<Item = &XaeroTopic> {
        self.topics[..(self.topic_count as usize)].iter()
    }
}

pub trait XaeroDataPlane {
    fn send_data(&self, peer: &Peer, event: DataEvent) -> Result<(), XaeroP2PError>;
    fn subscribe_data(&self) -> Receiver<DataEvent>;
}

pub trait XaeroDiscoveryBehavior<T>
where
    T: XaeroPlane + Send + Sync + 'static,
{
    fn discover_peers(&self, topic: crate::networking::p2p::XaeroTopic) -> Vec<Peer>;
    fn on_peer_discovered<F>(&self, handler: Arc<dyn Fn(&Peer) + Send + Sync + 'static>);
    fn on_peer_lost<F>(&self, handler: Arc<dyn Fn(&Peer) + Send + Sync + 'static>);
}
