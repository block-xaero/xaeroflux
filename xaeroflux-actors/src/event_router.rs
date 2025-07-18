use std::sync::OnceLock;

use rusted_ring_new::{PooledEvent, RingBuffer, Writer};
use xaeroflux_core::pipe::BusKind;

// Import the static ring buffers from your actors
use crate::{
    aof::ring_buffer_actor::{AOF_CONTROL_INPUT_RING, AOF_DATA_INPUT_RING},
    indexing::storage::actors::{
        mmr_actor::{MMR_CONTROL_INPUT_RING, MMR_DATA_INPUT_RING, MMR_LARGE_INPUT_RING, MMR_MEDIUM_INPUT_RING, MMR_XL_INPUT_RING},
        segment_writer_actor::{SEGMENT_CONTROL_INPUT_RING, SEGMENT_DATA_INPUT_RING, SEGMENT_LARGE_INPUT_RING, SEGMENT_MEDIUM_INPUT_RING, SEGMENT_XL_INPUT_RING},
    },
};

#[derive(Debug, Clone)]
pub enum ActorTarget {
    Aof(BusKind),
    SegmentWriter,
    Mmr,
    VectorSearch,
    P2P,
}

/// Type-safe event router that sends events to system actors via their dedicated ring buffers
pub struct EventRouter {
    // AOF Actor writers
    aof_control_writer: Writer<64, 2000>,
    aof_data_writer: Writer<256, 1000>,

    // Segment Writer writers
    segment_xs_writer: Writer<64, 2000>,
    segment_s_writer: Writer<256, 1000>,
    segment_m_writer: Writer<1024, 500>,
    segment_l_writer: Writer<4096, 100>,
    segment_xl_writer: Writer<16384, 50>,

    // MMR Actor writers
    mmr_xs_writer: Writer<64, 2000>,
    mmr_s_writer: Writer<256, 1000>,
    mmr_m_writer: Writer<1024, 500>,
    mmr_l_writer: Writer<4096, 100>,
    mmr_xl_writer: Writer<16384, 50>,
}

impl EventRouter {
    pub fn new() -> Self {
        Self {
            aof_control_writer: Writer::new(AOF_CONTROL_INPUT_RING.get_or_init(RingBuffer::new)),
            aof_data_writer: Writer::new(AOF_DATA_INPUT_RING.get_or_init(RingBuffer::new)),
            segment_xs_writer: Writer::new(SEGMENT_CONTROL_INPUT_RING.get_or_init(RingBuffer::new)),
            segment_s_writer: Writer::new(SEGMENT_DATA_INPUT_RING.get_or_init(RingBuffer::new)),
            segment_m_writer: Writer::new(SEGMENT_MEDIUM_INPUT_RING.get_or_init(RingBuffer::new)),
            segment_l_writer: Writer::new(SEGMENT_LARGE_INPUT_RING.get_or_init(RingBuffer::new)),
            segment_xl_writer: Writer::new(SEGMENT_XL_INPUT_RING.get_or_init(RingBuffer::new)),
            mmr_xs_writer: Writer::new(MMR_CONTROL_INPUT_RING.get_or_init(RingBuffer::new)),
            mmr_s_writer: Writer::new(MMR_DATA_INPUT_RING.get_or_init(RingBuffer::new)),
            mmr_m_writer: Writer::new(MMR_MEDIUM_INPUT_RING.get_or_init(RingBuffer::new)),
            mmr_l_writer: Writer::new(MMR_LARGE_INPUT_RING.get_or_init(RingBuffer::new)),
            mmr_xl_writer: Writer::new(MMR_XL_INPUT_RING.get_or_init(RingBuffer::new)),
        }
    }

    pub fn route_xs_event(&mut self, event: PooledEvent<64>, targets: &[ActorTarget]) {
        for target in targets {
            match target {
                ActorTarget::Aof(BusKind::Control) => {
                    self.aof_control_writer.add(event);
                }
                ActorTarget::SegmentWriter => {
                    self.segment_xs_writer.add(event);
                }
                ActorTarget::Mmr => {
                    self.mmr_xs_writer.add(event);
                }
                _ => {
                    tracing::warn!("Target {:?} doesn't support XS events", target);
                }
            }
        }
    }

    pub fn route_s_event(&mut self, event: PooledEvent<256>, targets: &[ActorTarget]) {
        for target in targets {
            match target {
                ActorTarget::Aof(BusKind::Data) => {
                    self.aof_data_writer.add(event);
                }
                ActorTarget::SegmentWriter => {
                    self.segment_s_writer.add(event);
                }
                ActorTarget::Mmr => {
                    self.mmr_s_writer.add(event);
                }
                _ => {
                    tracing::warn!("Target {:?} doesn't support S events", target);
                }
            }
        }
    }

    pub fn route_m_event(&mut self, event: PooledEvent<1024>, targets: &[ActorTarget]) {
        for target in targets {
            match target {
                ActorTarget::SegmentWriter => {
                    self.segment_m_writer.add(event);
                }
                ActorTarget::Mmr => {
                    self.mmr_m_writer.add(event);
                }
                _ => {
                    tracing::warn!("Target {:?} doesn't support M events", target);
                }
            }
        }
    }

    pub fn route_l_event(&mut self, event: PooledEvent<4096>, targets: &[ActorTarget]) {
        for target in targets {
            match target {
                ActorTarget::SegmentWriter => {
                    self.segment_l_writer.add(event);
                }
                ActorTarget::Mmr => {
                    self.mmr_l_writer.add(event);
                }
                _ => {
                    tracing::warn!("Target {:?} doesn't support L events", target);
                }
            }
        }
    }

    pub fn route_xl_event(&mut self, event: PooledEvent<16384>, targets: &[ActorTarget]) {
        for target in targets {
            match target {
                ActorTarget::SegmentWriter => {
                    self.segment_xl_writer.add(event);
                }
                ActorTarget::Mmr => {
                    self.mmr_xl_writer.add(event);
                }
                _ => {
                    tracing::warn!("Target {:?} doesn't support XL events", target);
                }
            }
        }
    }
}
