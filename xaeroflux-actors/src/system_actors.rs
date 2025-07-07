use std::sync::Arc;

use rusted_ring_new::EventUtils;
use xaeroflux_core::{event::XaeroEvent, pool::XaeroInternalEvent};

use crate::{
    aof::actor::AOFActor,
    indexing::storage::actors::{
        mmr_actor::MmrActor, secondary_index_actor::SecondaryIndexActor, segment_reader_actor::SegmentReaderActor,
        segment_writer_actor::SegmentWriterActor,
    },
    system_payload::SystemPayload,
};

pub struct SystemActors {
    // Control bus actors (mixed old/new patterns)
    pub control_aof: Arc<AOFActor>,                  // OLD: Pipe-based
    pub control_seg_writer: SegmentWriterActor,      // NEW: Ring buffer
    pub control_seg_reader: Arc<SegmentReaderActor>, // OLD: Pipe-based

    // Data bus actors (mixed old/new patterns)
    pub data_aof: Arc<AOFActor>,                     // OLD: Pipe-based
    pub data_seg_writer: SegmentWriterActor,         // NEW: Ring buffer
    pub data_seg_reader: Arc<SegmentReaderActor>,    // OLD: Pipe-based
    pub data_mmr: MmrActor,                          // NEW: Ring buffer
    pub data_secondary_indexer: SecondaryIndexActor, // NEW: Ring buffer
}

impl SystemActors {
    /// Send event to all control bus actors
    pub fn send_to_control(&mut self, event: &Arc<XaeroEvent>) {
        // Send to OLD pipe-based actors
        self.control_aof
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("control AOF actor must not fail");

        self.control_seg_reader
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("control segment reader actor must not fail");

        // Send to NEW ring buffer segment writer (requires conversion to XaeroInternalEvent)
        if let Some(internal_event) = self.convert_to_internal_event(event) {
            self.send_to_control_segment_writer(internal_event);
        }
    }

    /// Send event to all data bus actors
    pub fn send_to_data(&mut self, event: &Arc<XaeroEvent>) {
        // Send to OLD pipe-based actors
        self.data_aof
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data AOF actor must not fail");

        self.data_seg_reader
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data segment reader actor must not fail");

        // Send to NEW ring buffer actors (requires conversion)
        if let Some(internal_event) = self.convert_to_internal_event(event) {
            self.send_to_data_ring_buffer_actors(internal_event);
        }
    }

    /// Send SystemPayload to data secondary indexer
    pub fn send_system_payload_to_secondary_indexer(&mut self, payload: &SystemPayload) {
        let payload_bytes = bytemuck::bytes_of(payload);
        match EventUtils::create_pooled_event::<128>(payload_bytes, 301) {
            Ok(pooled_event) =>
                if !self.data_secondary_indexer.in_writer.add(pooled_event) {
                    tracing::warn!("Secondary indexer input buffer full");
                },
            Err(e) => tracing::error!("Failed to create SystemPayload event: {:?}", e),
        }
    }

    /// Convert XaeroEvent to XaeroInternalEvent for ring buffer actors
    fn convert_to_internal_event(&self, event: &Arc<XaeroEvent>) -> Option<Arc<XaeroInternalEvent<256>>> {
        // For now, we'll use S size (256 bytes) as a default
        // In production, you'd want logic to determine the appropriate size based on event data
        let event_data = event.data();

        if event_data.len() <= 256 {
            let mut internal_data = [0u8; 256];
            let copy_len = std::cmp::min(event_data.len(), 256);
            internal_data[..copy_len].copy_from_slice(&event_data[..copy_len]);

            let internal_event = XaeroInternalEvent::<256> {
                xaero_id_hash: [0; 32],     // Would derive from event in production
                vector_clock_hash: [0; 32], // Would derive from event in production
                evt: rusted_ring_new::PooledEvent {
                    data: internal_data,
                    len: copy_len as u32,
                    event_type: event.event_type() as u32,
                },
                latest_ts: event.latest_ts,
            };

            Some(Arc::new(internal_event))
        } else {
            tracing::warn!(
                "Event too large for current conversion logic: {} bytes",
                event_data.len()
            );
            None
        }
    }

    /// Send XaeroInternalEvent to control segment writer
    fn send_to_control_segment_writer(&mut self, internal_event: Arc<XaeroInternalEvent<256>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);
        match EventUtils::create_pooled_event::<256>(event_bytes, 302) {
            Ok(pooled_event) =>
                if !self.control_seg_writer.in_writer_s.add(pooled_event) {
                    tracing::warn!("Control segment writer S buffer full");
                },
            Err(e) => tracing::error!("Failed to create control segment writer event: {:?}", e),
        }
    }

    /// Send XaeroInternalEvent to data ring buffer actors
    fn send_to_data_ring_buffer_actors(&mut self, internal_event: Arc<XaeroInternalEvent<256>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);

        // Send to MMR actor
        match EventUtils::create_pooled_event::<256>(event_bytes, 303) {
            Ok(pooled_event) =>
                if !self.data_mmr.in_writer_s.add(pooled_event) {
                    tracing::warn!("Data MMR S buffer full");
                },
            Err(e) => tracing::error!("Failed to create MMR event: {:?}", e),
        }

        // Send to segment writer
        match EventUtils::create_pooled_event::<256>(event_bytes, 304) {
            Ok(pooled_event) =>
                if !self.data_seg_writer.in_writer_s.add(pooled_event) {
                    tracing::warn!("Data segment writer S buffer full");
                },
            Err(e) => tracing::error!("Failed to create segment writer event: {:?}", e),
        }
    }

    /// Send specific size events to ring buffer actors
    pub fn send_xs_to_data_mmr(&mut self, internal_event: Arc<XaeroInternalEvent<64>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);
        match EventUtils::create_pooled_event::<64>(event_bytes, 305) {
            Ok(pooled_event) =>
                if !self.data_mmr.in_writer_xs.add(pooled_event) {
                    tracing::warn!("Data MMR XS buffer full");
                },
            Err(e) => tracing::error!("Failed to create XS MMR event: {:?}", e),
        }
    }

    pub fn send_s_to_data_mmr(&mut self, internal_event: Arc<XaeroInternalEvent<256>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);
        match EventUtils::create_pooled_event::<256>(event_bytes, 306) {
            Ok(pooled_event) =>
                if !self.data_mmr.in_writer_s.add(pooled_event) {
                    tracing::warn!("Data MMR S buffer full");
                },
            Err(e) => tracing::error!("Failed to create S MMR event: {:?}", e),
        }
    }

    pub fn send_m_to_data_mmr(&mut self, internal_event: Arc<XaeroInternalEvent<1024>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);
        match EventUtils::create_pooled_event::<1024>(event_bytes, 307) {
            Ok(pooled_event) =>
                if !self.data_mmr.in_writer_m.add(pooled_event) {
                    tracing::warn!("Data MMR M buffer full");
                },
            Err(e) => tracing::error!("Failed to create M MMR event: {:?}", e),
        }
    }

    pub fn send_l_to_data_mmr(&mut self, internal_event: Arc<XaeroInternalEvent<4096>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);
        match EventUtils::create_pooled_event::<4096>(event_bytes, 308) {
            Ok(pooled_event) =>
                if !self.data_mmr.in_writer_l.add(pooled_event) {
                    tracing::warn!("Data MMR L buffer full");
                },
            Err(e) => tracing::error!("Failed to create L MMR event: {:?}", e),
        }
    }

    pub fn send_xl_to_data_mmr(&mut self, internal_event: Arc<XaeroInternalEvent<16384>>) {
        let event_bytes = bytemuck::bytes_of(&*internal_event);
        match EventUtils::create_pooled_event::<16384>(event_bytes, 309) {
            Ok(pooled_event) =>
                if !self.data_mmr.in_writer_xl.add(pooled_event) {
                    tracing::warn!("Data MMR XL buffer full");
                },
            Err(e) => tracing::error!("Failed to create XL MMR event: {:?}", e),
        }
    }

    /// Read confirmations from ring buffer actors
    pub fn read_mmr_confirmations(
        &mut self,
    ) -> Vec<crate::indexing::storage::actors::mmr_actor::MmrProcessConfirmation> {
        let mut confirmations = Vec::new();

        // Read from all MMR output readers
        while let Some(event) = self.data_mmr.out_reader_xs.next() {
            if let Ok(conf) = bytemuck::try_from_bytes(&event.data[..event.len as usize]) {
                confirmations.push(*conf);
            }
        }
        while let Some(event) = self.data_mmr.out_reader_s.next() {
            if let Ok(conf) = bytemuck::try_from_bytes(&event.data[..event.len as usize]) {
                confirmations.push(*conf);
            }
        }
        // ... similar for M, L, XL

        confirmations
    }

    pub fn read_segment_confirmations(
        &mut self,
    ) -> Vec<crate::indexing::storage::actors::segment_writer_actor::SegmentWriteConfirmation> {
        let mut confirmations = Vec::new();

        // Read from all segment writer output readers
        while let Some(event) = self.data_seg_writer.out_reader_xs.next() {
            if let Ok(conf) = bytemuck::try_from_bytes(&event.data[..event.len as usize]) {
                confirmations.push(*conf);
            }
        }
        while let Some(event) = self.data_seg_writer.out_reader_s.next() {
            if let Ok(conf) = bytemuck::try_from_bytes(&event.data[..event.len as usize]) {
                confirmations.push(*conf);
            }
        }
        // ... similar for M, L, XL

        confirmations
    }

    pub fn read_secondary_index_confirmations(
        &mut self,
    ) -> Vec<crate::indexing::storage::actors::secondary_index_actor::SecondaryIndexConfirmation> {
        let mut confirmations = Vec::new();

        while let Some(event) = self.data_secondary_indexer.out_reader.next() {
            if let Ok(conf) = bytemuck::try_from_bytes(&event.data[..event.len as usize]) {
                confirmations.push(*conf);
            }
        }

        confirmations
    }
}
