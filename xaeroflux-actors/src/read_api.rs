use std::{error::Error, io::ErrorKind, sync::OnceLock};

use bytemuck::{Pod, Zeroable};
use rusted_ring::{EventPoolFactory, EventSize, EventUtils, RingBuffer, Writer};

use crate::{XaeroFlux, indexing::vec_search_actor::VectorQueryRequest};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PointQuery<const SIZE: usize> {
    pub event_type: Option<u32>,
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
/// Read Api follows Ringbuffer architecture request and response architecture.
///
/// My reasoning for this is simple:
///
/// [*] Pre-allocated ring buffers allows no jumps in local memory
/// [*] Buffers can be safely read via FFI using
/// ```rust
/// use std::sync::atomic::{Ordering, fence};
/// fence(Ordering::Acquire)
/// ```
/// [*] Assumes that buffers are pre-allocated for all kinds of responses makes point queries
/// more precise and thoughtful. If however apps are unsure of what size their data is,
/// it will be spread to different buffers whatever results are found. Event types are usually
/// bound to pre-allocated buffers or event sizes. e.g. if you app declares
/// ```rust
/// use rusted_ring::{AutoSizedEvent, EventCreationError, EventUtils};
/// let payload = b"text_message: hello_world";
/// let event_rs = EventUtils::create_auto_sized_event(payload.as_slice(), 23);
/// match event_rs {
///     Ok(auto_sized_event) => {
///         // you know what size you are creating
///     }
///     Err(e) => {
///         panic!("failed due to : {e:?}")
///     }
/// }
/// ```
/// This pre-determined nature ensures that you know what you are doing while reading data,
/// it is *VERY* very vital especially in p2p, decentralized, offline-first application
/// that we control memory footprint as much as we can given the cloudless nature of our apps.
/// Single Writer writes results to read api ring-buffers;
/// following static buffers are available for sharing:
///
/// [*] PointQueryBuffer
/// [*] ReplayQueryBuffer
/// [*] VectorSearchResultBuffer
///
/// ## Disclaimer
/// NOTE: it is expected that these are not written to by *ANYTHING* else!
/// I do not provide guarantees that I can prevent source of writes other than ReadApi!
/// It is also importan
/// The readers of buffer should
/// use std::sync::atomic::{fence, Ordering};
/// use rusted ring's readers
/// Example use of readers:
/// ```rust
/// use rusted_ring::{PooledEvent, RingBuffer};
/// use rusted_ring::{S_CAPACITY,S_TSHIRT_SIZE};
/// use tracing::debug;
/// // NOTE: YOU DO NOT NEED TO
/// static POINT_QUERY_BUFFER : RingBuffer<S_TSHIRT_SIZE,S_CAPACITY> = RingBuffer::new();
///  let mut reader = rusted_ring::Reader::new(&POINT_QUERY_BUFFER);
///  while let Ok(event) = reader.by_ref(){
///     tracing::debug!("processing the event here: {event:?}")
/// }
trait ReadApi<const SIZE: usize> {
    fn init_buffers() -> Result<(), Box<dyn std::error::Error>>;
    fn point(&self, query: PointQuery<SIZE>) -> Result<(), Box<dyn std::error::Error>>;
    fn replay(&self, query: ReplayQuery<SIZE>) -> Result<(), Box<dyn std::error::Error>>;
    fn vector_search(&self, query: VectorQueryRequest<SIZE>) -> Result<(), Box<dyn std::error::Error>>;
}
use rusted_ring::{L_CAPACITY, L_TSHIRT_SIZE, M_CAPACITY, M_TSHIRT_SIZE, S_CAPACITY, S_TSHIRT_SIZE, XL_CAPACITY, XL_TSHIRT_SIZE, XS_CAPACITY, XS_TSHIRT_SIZE};
use xaeroflux_core::event::EventType;

use crate::aof::storage::lmdb::get_event_by_event_type;

pub static POINT_QUERY_XS: OnceLock<RingBuffer<XS_TSHIRT_SIZE, XS_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_S: OnceLock<RingBuffer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_M: OnceLock<RingBuffer<M_TSHIRT_SIZE, M_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_L: OnceLock<RingBuffer<L_TSHIRT_SIZE, L_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_XL: OnceLock<RingBuffer<XL_TSHIRT_SIZE, XL_CAPACITY>> = OnceLock::new();

pub static RANGE_QUERY_XL: OnceLock<RingBuffer<XL_TSHIRT_SIZE, XL_CAPACITY>> = OnceLock::new();

pub static CONTINUOUS_QUERY_S: OnceLock<RingBuffer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();

pub static POINT_QUERY_XS_WRITER: OnceLock<Writer<XS_TSHIRT_SIZE, XS_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_S_WRITER: OnceLock<Writer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_M_WRITER: OnceLock<Writer<M_TSHIRT_SIZE, M_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_L_WRITER: OnceLock<Writer<L_TSHIRT_SIZE, L_CAPACITY>> = OnceLock::new();
pub static POINT_QUERY_XL_WRITER: OnceLock<Writer<XL_TSHIRT_SIZE, XL_CAPACITY>> = OnceLock::new();

pub static RANGE_QUERY_XL_WRITER: OnceLock<Writer<XL_TSHIRT_SIZE, XL_CAPACITY>> = OnceLock::new();

pub static CONTINUOUS_QUERY_S_WRITER: OnceLock<Writer<S_TSHIRT_SIZE, S_CAPACITY>> = OnceLock::new();

impl<const SIZE: usize> ReadApi<SIZE> for XaeroFlux {
    fn init_buffers() -> Result<(), Box<dyn Error>> {
        let pt_query_xs = POINT_QUERY_XS.get_or_init(RingBuffer::new);
        let pt_query_s = POINT_QUERY_S.get_or_init(RingBuffer::new);
        let pt_query_m = POINT_QUERY_M.get_or_init(RingBuffer::new);
        let pt_query_l = POINT_QUERY_L.get_or_init(RingBuffer::new);
        let pt_query_xl = POINT_QUERY_XL.get_or_init(RingBuffer::new);
        let range_query_xl = RANGE_QUERY_XL.get_or_init(RingBuffer::new);
        let continuous_query_s = CONTINUOUS_QUERY_S.get_or_init(RingBuffer::new);
        // writers
        POINT_QUERY_XS_WRITER.get_or_init(|| Writer::new(pt_query_xs));
        POINT_QUERY_S_WRITER.get_or_init(|| Writer::new(pt_query_s));
        POINT_QUERY_M_WRITER.get_or_init(|| Writer::new(pt_query_m));
        POINT_QUERY_L_WRITER.get_or_init(|| Writer::new(pt_query_l));
        POINT_QUERY_XL_WRITER.get_or_init(|| Writer::new(pt_query_xl));
        RANGE_QUERY_XL_WRITER.get_or_init(|| Writer::new(range_query_xl));
        CONTINUOUS_QUERY_S_WRITER.get_or_init(|| Writer::new(continuous_query_s));
        Ok(())
    }

    fn point(&self, query: PointQuery<SIZE>) -> Result<(), Box<dyn std::error::Error>> {
        match query.event_type {
            None => Err(Box::new(std::io::Error::new::<&str>(ErrorKind::InvalidInput, "event_type not set".into()))),
            Some(event_type) => {
                let read_handle = self.read_handle.clone();
                let res = get_event_by_event_type::<SIZE>(&read_handle.expect("read_handle not set"), EventType::ApplicationEvent(event_type as u8), query.xaero_id)?;
                match res {
                    None => {
                        return Err(Box::new(std::io::Error::new::<&str>(ErrorKind::InvalidInput, "event_not set".into())));
                    }
                    Some(xaero_internal_event) => {
                        let size = EventPoolFactory::estimate_size(xaero_internal_event.evt.data.len());
                        match size {
                            EventSize::XS => {
                                let pooled_event = EventUtils::create_pooled_event(&xaero_internal_event.evt.data, xaero_internal_event.evt.event_type)?;
                                POINT_QUERY_XS_WRITER.get().expect("POINT_QUERY_XS_WRITER not set").add(pooled_event);
                            }
                            EventSize::S => {
                                let pooled_event = EventUtils::create_pooled_event(&xaero_internal_event.evt.data, xaero_internal_event.evt.event_type)?;
                                POINT_QUERY_S_WRITER.get().expect("POINT_QUERY_XS_WRITER not set").add(pooled_event);
                            }
                            EventSize::M => {
                                let pooled_event = EventUtils::create_pooled_event(&xaero_internal_event.evt.data, xaero_internal_event.evt.event_type)?;
                                POINT_QUERY_M_WRITER.get().expect("POINT_QUERY_XS_WRITER not set").add(pooled_event);
                            }
                            EventSize::L => {
                                let pooled_event = EventUtils::create_pooled_event(&xaero_internal_event.evt.data, xaero_internal_event.evt.event_type)?;
                                POINT_QUERY_L_WRITER.get().expect("POINT_QUERY_XS_WRITER not set").add(pooled_event);
                            }
                            EventSize::XL => {
                                let pooled_event = EventUtils::create_pooled_event(&xaero_internal_event.evt.data, xaero_internal_event.evt.event_type)?;
                                POINT_QUERY_XL_WRITER.get().expect("POINT_QUERY_XS_WRITER not set").add(pooled_event);
                            }
                            EventSize::XXL => {
                                panic!("reads unsupported for this event size for now!")
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn replay(&self, query: ReplayQuery<SIZE>) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn vector_search(&self, query: VectorQueryRequest<SIZE>) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
