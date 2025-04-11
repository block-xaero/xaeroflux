use std::any::Any;

pub mod consumer;
pub mod event;
pub mod event_buffer;
pub mod producer;
pub mod storage;
pub mod storage_meta;
pub mod threads;
pub trait XaeroData: Any + Send + Sync + bincode::Decode<()> + bincode::Encode {}
