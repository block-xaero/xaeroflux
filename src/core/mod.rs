use std::any::Any;

pub mod consumer;
pub mod event;
pub mod event_buffer;
pub mod producer;
pub mod storage;
pub mod storage_meta;
pub mod threads;
pub trait XaeroData: Any + Send + Sync + bincode::Decode<()> + bincode::Encode {}

impl XaeroData for Vec<u8> {}
impl XaeroData for String {}
impl XaeroData for i32 {}
impl XaeroData for i64 {}
impl XaeroData for u32 {}
impl XaeroData for u64 {}
impl XaeroData for f32 {}
impl XaeroData for f64 {}
impl XaeroData for bool {}
impl XaeroData for () {}
impl XaeroData for [u8; 32] {}
impl XaeroData for [u8; 64] {}
impl XaeroData for [u8; 128] {}
impl XaeroData for [u8; 256] {}
impl XaeroData for [u8; 512] {}
