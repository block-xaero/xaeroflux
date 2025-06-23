mod p2p;
mod pool;
mod control_plane;
mod data_plane;
mod network_conduit;
mod state;

pub struct BufferStatus{
    pub capacity: usize,
    pub len: usize,
}


pub enum SyncContext{
    NEW_EVENT,
    REBOOT_SYNC,
    SYNC
}