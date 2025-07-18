pub mod mmr_actor;
pub mod secondary_index_actor;
pub mod segment_writer_actor;
pub(crate) mod vector_search_actor;

pub enum ExecutionState {
    Waiting,
    Initialized,
    Running,
    Terminated,
}
