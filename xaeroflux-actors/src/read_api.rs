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

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RangeQuery<const SIZE: usize> {
    pub xaero_id: [u8; 32],
    pub event_type: u32,
}

unsafe impl<const SIZE: usize> Pod for RangeQuery<SIZE> {}
unsafe impl<const SIZE: usize> Zeroable for RangeQuery<SIZE> {}

/// # Read API Infrastructure
pub trait ReadApi {
    fn init_buffers() -> Result<(), Box<dyn std::error::Error>>;

    fn point<const SIZE: usize>(&self, query: PointQuery<SIZE>) -> Result<XaeroInternalEvent<SIZE>, Box<dyn std::error::Error>>;

    fn range_query<const SIZE: usize>(&self, query: RangeQuery<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>>;

    fn range_query_with_filter<const SIZE: usize, P>(&self, query: RangeQuery<SIZE>, filter: Box<P>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>>
    where
        P: Fn(&XaeroInternalEvent<SIZE>) -> bool;

    fn replay<const SIZE: usize>(&self, query: ReplayQuery<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>>;

    fn vector_search<const SIZE: usize>(&self, query: VectorQueryRequest<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>>;

    fn find_current_state_by_eid<const SIZE: usize>(&self, eid: [u8; 32]) -> Result<Option<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>>;

    fn get_current_vc_hash(&self) -> Result<[u8; 32], Box<dyn std::error::Error>>;
}
use rusted_ring::{S_CAPACITY, S_TSHIRT_SIZE};
use xaeroflux_core::pool::XaeroInternalEvent;
use xaeroid::XaeroID;

use crate::aof::storage::lmdb::{get_current_state_by_entity_id, get_current_vc_hash, get_event_by_hash, get_events_by_event_type, get_vector_clock_meta};

pub static CONTINUOUS_QUERY_S: OnceLock<RingBuffer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();

pub static CONTINUOUS_QUERY_S_WRITER: OnceLock<Writer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();

impl ReadApi for XaeroFlux {
    fn init_buffers() -> Result<(), Box<dyn Error>> {
        CONTINUOUS_QUERY_S_WRITER.get_or_init(|| Writer::new(CONTINUOUS_QUERY_S.get_or_init(RingBuffer::new)));
        Ok(())
    }

    fn point<const SIZE: usize>(&self, query: PointQuery<SIZE>) -> Result<XaeroInternalEvent<SIZE>, Box<dyn std::error::Error>> {
        let read_handle = self.read_handle.clone();
        let res = get_event_by_hash::<SIZE>(&read_handle.expect("failed to read"), query.blake_hash)?;
        match res {
            None => Err(Box::new(std::io::Error::new::<&str>(ErrorKind::NotFound, "cannot find the point asked for"))),
            Some(event) => Ok(event),
        }
    }

    fn range_query<const SIZE: usize>(&self, query: RangeQuery<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>> {
        let read_handle = self.read_handle.clone();
        let res = unsafe { get_events_by_event_type::<SIZE>(&read_handle.expect("read_api not ready!"), query.event_type) }?;
        Ok(res)
    }

    fn range_query_with_filter<const SIZE: usize, P>(&self, query: RangeQuery<SIZE>, filter: Box<P>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>>
    where
        P: Fn(&XaeroInternalEvent<SIZE>) -> bool,
    {
        let read_handle = self.read_handle.clone();
        let res = unsafe { get_events_by_event_type::<SIZE>(&read_handle.expect("read_api not ready!"), query.event_type) }?;
        tracing::info!("found events raw: {res:#?}");
        let filtered_result = res.into_iter().filter(filter).collect();
        Ok(filtered_result)
    }

    fn replay<const SIZE: usize>(&self, query: ReplayQuery<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>> {
        todo!()
    }

    fn vector_search<const SIZE: usize>(&self, query: VectorQueryRequest<SIZE>) -> Result<Vec<XaeroInternalEvent<SIZE>>, Box<dyn Error>> {
        todo!()
    }

    fn find_current_state_by_eid<const SIZE: usize>(&self, eid: [u8; 32]) -> Result<Option<XaeroInternalEvent<SIZE>>, Box<dyn Error>> {
        let mut env = self.read_handle.clone().expect("read_api not ready!");
        get_current_state_by_entity_id(&mut env, eid)
    }

    fn get_current_vc_hash(&self) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        let env = self.read_handle.clone().expect("read_handle_not_ready");
        get_current_vc_hash(&env)
    }
}
