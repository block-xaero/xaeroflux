use xaeroid::XaeroID;

use crate::{
    aof::storage::format::SegmentMeta,
    networking::pool::{FixedGroup, FixedWorkspace},
};

pub struct GroupLimits<
    const MAX_WORKSPACES: usize,
    const MAX_TOTAL_PEERS: usize,
    const MAX_OBJECTS_PER_WORKSPACE: usize,
    const MAX_PEERS_PER_WORKSPACE: usize,
> {
    // Just a marker type for the const generics
}

pub type PersonalLimits = GroupLimits<3, 10, 5, 5>;
pub type SmallTeamLimits = GroupLimits<10, 50, 20, 25>;
pub type DepartmentLimits = GroupLimits<25, 200, 50, 50>;
pub type EnterpriseLimits = GroupLimits<50, 1000, 100, 100>;
pub type MassiveLimits = GroupLimits<100, 5000, 200, 500>;

pub struct GroupStateMachine<
    const MAX_WORKSPACES: usize,
    const MAX_PEERS: usize,
    const OBJECT_SIZE: usize,
    const MAX_WORKSPACE_PEERS: usize,
> {
    pub data: FixedGroup<MAX_WORKSPACES, MAX_PEERS, OBJECT_SIZE, MAX_WORKSPACE_PEERS>,
    pub current_state: GroupState,
    pub pending_handshakes: [XaeroID; 10],
    pub last_announcement: u64,
}

pub enum GroupState {
    Disconnected,
    Discovering,  // Broadcasting announcements, listening for peers
    Handshaking,  // Exchanging capabilities with discovered peers
    Connected,    // Actively participating in group
    Reconnecting, // Recovering from network partition
}

pub struct WorkspaceStateMachine<const OBJECT_SIZE: usize, const MAX_PEERS: usize> {
    pub data: FixedWorkspace<OBJECT_SIZE, MAX_PEERS>,
    pub current_state: WorkspaceState,
    pub sync_status: SyncStatus,
    pub peer_sync_states: [WorkspaceSyncState; MAX_PEERS],
}

pub enum WorkspaceState {
    Inactive,      // No activity
    Synchronizing, // Initial sync with peers
    Active,        // Normal operation
}

pub struct SyncStatus {
    pub last_sync_time: u64,
    pub missing_segments: [SegmentMeta; 32],
    pub sync_progress: f32,
}

pub struct WorkspaceSyncState {
    pub peer_id: XaeroID,
    pub last_known_peaks: [[u8; 32]; 16],
    pub sync_complete: bool,
}
