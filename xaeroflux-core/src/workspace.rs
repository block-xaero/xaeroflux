use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use xaeroid::XaeroID;

use crate::pipe::{BusKind, Pipe, SignalPipe};

#[derive(Debug)]
pub struct Conduit {
    pub control_pipe: Arc<Pipe>,
    pub data_pipe: Arc<Pipe>,
    pub control_signal_pipe: Arc<SignalPipe>,
    pub data_signal_pipe: Arc<SignalPipe>,
}

impl Conduit {
    pub fn new(bounds: Option<usize>) -> Self {
        Conduit {
            control_pipe: Pipe::new(BusKind::Control, bounds),
            data_pipe: Pipe::new(BusKind::Data, bounds),
            control_signal_pipe: SignalPipe::new(BusKind::Control, bounds),
            data_signal_pipe: SignalPipe::new(BusKind::Data, bounds),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OnlinePeerInfo {
    pub xaero_id: XaeroID,
    pub last_seen: u64,
}

pub struct Group {
    pub name: String,
    pub online_peers: Arc<Mutex<Vec<OnlinePeerInfo>>>,
    pub workspaces: Vec<Workspace>,
    pub conduits: Conduit,
}

#[derive(Debug)]
pub struct Workspace {
    pub id: [u8; 32],
    pub peers: Arc<Mutex<Vec<XaeroID>>>,
    pub objects: Arc<Mutex<HashMap<[u8; 32], Object>>>,
    pub conduit: Conduit,
}

#[derive(Debug)]
pub struct Object {
    pub id: [u8; 32],
    pub conduit: Conduit,
}
