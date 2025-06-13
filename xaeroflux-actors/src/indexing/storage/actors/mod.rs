pub mod mmr_actor;
pub mod secondary_index_actor;
pub mod segment_reader_actor;
pub mod segment_writer_actor;

pub enum ExecutionState {
    Waiting,
    Initialized,
    Running,
    Terminated,
}
