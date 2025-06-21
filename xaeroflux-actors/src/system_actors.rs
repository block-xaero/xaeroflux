use std::sync::Arc;

use xaeroflux_core::event::XaeroEvent;

use crate::{
    aof::actor::AOFActor,
    indexing::storage::actors::{
        mmr_actor::MmrIndexingActor, secondary_index_actor::SecondaryIndexActor,
        segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentWriterActor,
    },
};

pub struct SystemActors {
    // Control bus actors
    pub control_aof: Arc<AOFActor>,
    pub control_seg_writer: Arc<SegmentWriterActor>,
    pub control_seg_reader: Arc<SegmentReaderActor>,

    // Data bus actors
    pub data_aof: Arc<AOFActor>,
    pub data_seg_writer: Arc<SegmentWriterActor>,
    pub data_seg_reader: Arc<SegmentReaderActor>,
    pub data_mmr: Arc<MmrIndexingActor>,
    pub data_secondary_indexer: Arc<SecondaryIndexActor>,
}

impl SystemActors {
    /// Send event to all control bus actors
    pub fn send_to_control(&self, event: &Arc<XaeroEvent>) {
        self.control_aof
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("control AOF actor must not fail");
        self.control_seg_writer
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("control segment writer actor must not fail");
        self.control_seg_reader
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("control segment reader actor must not fail");
    }

    /// Send event to all data bus actors  
    pub fn send_to_data(&self, event: &Arc<XaeroEvent>) {
        self.data_aof
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data AOF actor must not fail");
        self.data_seg_writer
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data segment writer actor must not fail");
        self.data_seg_reader
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data segment reader actor must not fail");
        self.data_mmr
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data MMR actor must not fail");
        self.data_secondary_indexer
            .pipe
            .sink
            .tx
            .send(event.clone())
            .expect("data secondary indexer actor must not fail");
    }
}
