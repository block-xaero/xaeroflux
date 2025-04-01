use crate::{
    consumer::ConsumerOps, event_buffer::RawEvent, producer::ProducerOps, storage::Storage,
};

pub struct XaeroFluxEngine {
    pub storage: Box<dyn Storage>,
    pub raw_event_producer: Box<dyn ProducerOps<RawEvent>>,
    pub consumers: Box<dyn ConsumerOps>,
}

pub trait XaeroFluxEngineOps {
    fn init(&mut self, path: &str) -> anyhow::Result<()>;
    fn start_producers(&mut self) -> anyhow::Result<()>;
    fn stop_producers(&mut self) -> anyhow::Result<()>;
    fn start_consumers(&mut self) -> anyhow::Result<()>;
    fn stop_consumers(&mut self) -> anyhow::Result<()>;
    fn start_storage(&mut self) -> anyhow::Result<()>;
    fn stop_storage(&mut self) -> anyhow::Result<()>;
}

impl XaeroFluxEngineOps for XaeroFluxEngine {
    fn init(&mut self, path: &str) -> anyhow::Result<()> {
        todo!()
    }

    fn start_producers(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn stop_producers(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn start_consumers(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn stop_consumers(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn start_storage(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn stop_storage(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
