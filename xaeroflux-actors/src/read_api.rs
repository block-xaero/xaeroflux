use std::{error::Error, io::ErrorKind, sync::OnceLock};

use bytemuck::{Pod, Zeroable};
use rusted_ring::{RingBuffer, Writer};

use crate::{XaeroFlux, indexing::vec_search_actor::VectorQueryRequest};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PointQuery<const SIZE: usize> {
    pub blake_hash: [u8; 32],
    pub xaero_id: [u8; 32],
}
unsafe impl<const SIZE: usize> Pod for PointQuery<SIZE> {}
unsafe impl<const SIZE: usize> Zeroable for PointQuery<SIZE> {}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ReplayQuery<const SIZE: usize> {
    pub xaero_id: [u8; 32],
    pub start: u64,
    pub end: u64,
}

unsafe impl<const SIZE: usize> Pod for ReplayQuery<SIZE> {}
unsafe impl<const SIZE: usize> Zeroable for ReplayQuery<SIZE> {}

/// # Read API Infrastructure
pub trait ReadApi<const SIZE: usize> {
    fn init_buffers() -> Result<(), Box<dyn std::error::Error>>;
    fn point(&self, query: PointQuery<SIZE>) -> Result<XaeroInternalEvent<SIZE>, Box<dyn std::error::Error>>;
    fn replay(&self, query: ReplayQuery<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>>;
    fn vector_search(&self, query: VectorQueryRequest<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>>;
}
use rusted_ring::{S_CAPACITY, S_TSHIRT_SIZE};
use xaeroflux_core::pool::XaeroInternalEvent;

use crate::aof::storage::lmdb::get_event_by_hash;

pub static CONTINUOUS_QUERY_S: OnceLock<RingBuffer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();

pub static CONTINUOUS_QUERY_S_WRITER: OnceLock<Writer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();

impl<const SIZE: usize> ReadApi<SIZE> for XaeroFlux {
    fn init_buffers() -> Result<(), Box<dyn Error>> {
        // TODO: This is for querying p2p stuff that comes in
        // TODO: this is also for stuff that clients may react to.
        CONTINUOUS_QUERY_S_WRITER.get_or_init(|| Writer::new(CONTINUOUS_QUERY_S.get_or_init(RingBuffer::new)));
        Ok(())
    }

    fn point(&self, query: PointQuery<SIZE>) -> Result<XaeroInternalEvent<SIZE>, Box<dyn std::error::Error>> {
        let read_handle = self.read_handle.clone();
        let res = get_event_by_hash::<SIZE>(&read_handle.expect("failed to read"), query.blake_hash)?;
        match res {
            None => Err(Box::new(std::io::Error::new::<&str>(ErrorKind::NotFound, "cannot find the point asked for"))),
            Some(event) => Ok(event),
        }
    }

    fn replay(&self, query: ReplayQuery<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>> {
        todo!()
    }

    fn vector_search(&self, query: VectorQueryRequest<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>> {
        todo!()
    }
}
