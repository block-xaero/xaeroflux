#![feature(associated_type_defaults)]
extern crate core;

pub mod aof;
pub mod event_router;
pub mod indexing;
pub mod networking;

pub mod subject;
pub mod system_payload;

use std::{collections::BTreeMap, sync::Arc};

use bytemuck::{Pod, Zeroable};
use xaeroflux_core::{
    event::ScanWindow, // Import from xaeroflux_core
    pipe::{BusKind, Pipe},
};

use crate::subject::SubjectHash;

#[derive(Debug, Clone, Copy)]
pub struct OnlinePeerInfo {
    pub xaero_id: [u8; 32],
    pub last_seen: u64,
}
unsafe impl Pod for OnlinePeerInfo {}
unsafe impl Zeroable for OnlinePeerInfo {}

pub struct Group {
    pub name: String,
    pub online_peers: Vec<OnlinePeerInfo>,
    pub workspaces: BTreeMap<[u8; 32], Workspace>,
}

#[derive(Debug)]
pub struct Workspace {
    pub id: [u8; 32],
    pub online_peers: Vec<OnlinePeerInfo>,
    pub objects: BTreeMap<[u8; 32], Object>,
}

#[derive(Debug)]
pub struct Object {
    pub subject_hash: SubjectHash,
}
