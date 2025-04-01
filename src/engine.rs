
use crate::{
    consumer::ConsumerOps,
    event_buffer::RawEvent,
    producer::ProducerOps,
    storage::Storage,
};

pub struct XaeroFluxEngine {
    pub storage: Box<dyn Storage>,
    pub raw_event_producer: Box<dyn ProducerOps<RawEvent>>,
    pub consumers: Box<dyn ConsumerOps>,
}

trait XaeroFluxEngineOps {
    fn init(&mut self, path: &str) -> anyhow::Result<()>;
    fn add_consumer(&mut self, consumer: Box<dyn ConsumerOps>);
    fn remove_consumer(&mut self, consumer: Box<dyn ConsumerOps>);
    fn consume(&mut self, event: &RawEvent);
    fn produce(&mut self, event: &RawEvent);
}

impl XaeroFluxEngineOps for XaeroFluxEngine {
    fn init(&mut self, path: &str) -> anyhow::Result<()> {
        todo!()
    }

    fn add_consumer(&mut self, consumer: Box<dyn ConsumerOps>) {
        todo!()
    }

    fn remove_consumer(&mut self, consumer: Box<dyn ConsumerOps>) {
        todo!()
    }

    fn consume(&mut self, event: &RawEvent) {
        todo!()
    }

    fn produce(&mut self, event: &RawEvent) {
        todo!()
    }
}
