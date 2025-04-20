use crate::core::event_buffer::RawEvent;

pub struct XaeroFluxEngine {
}

pub trait XaeroFluxEngineStorageOps {
    fn push_raw(&mut self, event: RawEvent) -> anyhow::Result<()>;
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
    fn init(&mut self, _path: &str) -> anyhow::Result<()> {
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
