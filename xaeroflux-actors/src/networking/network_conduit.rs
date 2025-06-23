use std::sync::Arc;
use xaeroflux_core::pipe::{Pipe, SignalPipe};
use crate::networking::pool::{SizedReader, SizedWriter};

#[derive(Debug)]
pub struct NetworkSource {
    pub reader: SizedReader,
    pub writer: SizedWriter,
}

#[derive(Debug)]
pub struct NetworkSink {
    pub reader: SizedReader,
    pub writer: SizedWriter,
}

#[derive(Debug)]
pub struct NetworkPipe {
    pub source: NetworkSource,
    pub sink: NetworkSink,
}
#[derive(Debug)]
pub struct NetworkConduit {
    pub control_pipe: Arc<Pipe>,
    pub signal_pipe: Arc<SignalPipe>,
    pub data_pipe: Arc<NetworkPipe>,
}
