use xaeroflux_core::workspace::Conduit;
use xaeroid::XaeroID;

#[derive(Debug, Clone)]
pub struct OnlinePeerInfo {
    pub xaero_id: XaeroID,
    pub last_seen: u64,
}

#[derive(Debug)]
pub struct FixedGroup<const MAX_WORKSPACES: usize, const MAX_PEERS: usize, const OBJECT_SIZE: usize, const MAX_WORKSPACE_PEERS: usize> {
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
    pub conduit: Conduit,
}
