use rusted_ring::{Reader, Writer};
use xaeroflux_core::workspace::Conduit;
use xaeroid::XaeroID;

use crate::networking::network_conduit::NetworkConduit;

pub enum SizedReader {
    // Control plane - frequent small messages
    // Peer announcements, heartbeats, status updates
    Xs(Reader<64, 2048>),

    // Data plane - typical CRDT operations
    // Most CRDT ops, small text edits, cursor moves
    S(Reader<256, 1024>),

    // Bulk operations - document sync, large updates
    // Document chunks, batch CRDT resolution
    M(Reader<1024, 512>),

    // Large transfers - file sync, media
    // Large document sync, media metadata
    L(Reader<4096, 256>),

    // Rare huge transfers - initial sync, snapshots
    // Full document snapshots, complete state sync
    Xl(Reader<16384, 128>),
}

impl std::fmt::Debug for SizedReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SizedReader::Xs(_) => write!(f, "SizedReader::Xs"),
            SizedReader::S(_) => write!(f, "SizedReader::S"),
            SizedReader::M(_) => write!(f, "SizedReader::M"),
            SizedReader::L(_) => write!(f, "SizedReader::L"),
            SizedReader::Xl(_) => write!(f, "SizedReader::Xl"),
        }
    }
}

pub enum SizedWriter {
    // half the size of reader
    Xs(Writer<64, 2048>),
    S(Writer<256, 1024>),
    M(Writer<1024, 512>),
    L(Writer<4096, 256>),
    Xl(Writer<16384, 128>),
}

impl std::fmt::Debug for SizedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SizedWriter::Xs(_) => write!(f, "SizedWriter::Xs"),
            SizedWriter::S(_) => write!(f, "SizedWriter::S"),
            SizedWriter::M(_) => write!(f, "SizedWriter::M"),
            SizedWriter::L(_) => write!(f, "SizedWriter::L"),
            SizedWriter::Xl(_) => write!(f, "SizedWriter::Xl"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OnlinePeerInfo {
    pub xaero_id: XaeroID,
    pub last_seen: u64,
}

#[derive(Debug)]
pub struct FixedGroup<
    const MAX_WORKSPACES: usize,
    const MAX_PEERS: usize,
    const OBJECT_SIZE: usize,
    const MAX_WORKSPACE_PEERS: usize,
> {
    pub name: [u8; 32],
    pub online_peers: [OnlinePeerInfo; MAX_PEERS],
    pub workspaces: [FixedWorkspace<OBJECT_SIZE, MAX_WORKSPACE_PEERS>; MAX_WORKSPACES],
    pub conduit: Conduit,
}

#[derive(Debug)]
pub struct FixedWorkspace<const OBJECT_SIZE: usize, const MAX_PEERS: usize> {
    pub id: [u8; 32],
    pub online_peers: [OnlinePeerInfo; MAX_PEERS],
    pub objects: [FixedObject; OBJECT_SIZE],
    pub conduit: Conduit,
}

#[derive(Debug)]
pub struct FixedObject {
    pub id: [u8; 32],
    pub conduit: NetworkConduit,
}
