use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use bytemuck::{Pod, Zeroable};
use rusted_ring_new::RingBuffer;

use crate::{
    pipe::{BusKind, SignalPipe},
    ring_buffer_actor::RingPipe,
};
#[derive(Debug)]
pub struct Conduit<
    const C_TSHIRT_SIZE: usize,
    const C_RING_CAPACITY: usize,
    const D_TSHIRT_SIZE: usize,
    const D_RING_CAPACITY: usize,
> {
    pub control_pipe: RingPipe<C_TSHIRT_SIZE, C_RING_CAPACITY>,
    pub data_pipe: RingPipe<D_TSHIRT_SIZE, D_RING_CAPACITY>,
    pub control_signal_pipe: Arc<SignalPipe>,
    pub data_signal_pipe: Arc<SignalPipe>,
}

impl<
    const C_TSHIRT_SIZE: usize,
    const C_RING_CAPACITY: usize,
    const D_TSHIRT_SIZE: usize,
    const D_RING_CAPACITY: usize,
> Conduit<C_TSHIRT_SIZE, C_RING_CAPACITY, D_TSHIRT_SIZE, D_RING_CAPACITY>
{
    pub fn new(
        data_in_buffer: &'static RingBuffer<D_TSHIRT_SIZE, D_RING_CAPACITY>,
        data_out_buffer: &'static RingBuffer<D_TSHIRT_SIZE, D_RING_CAPACITY>,
        control_in_buffer: &'static RingBuffer<C_TSHIRT_SIZE, C_RING_CAPACITY>,
        control_out_buffer: &'static RingBuffer<C_TSHIRT_SIZE, C_RING_CAPACITY>,
        bounds: Option<usize>,
    ) -> Self {
        Conduit {
            control_pipe: RingPipe::new(control_in_buffer, control_out_buffer),
            data_pipe: RingPipe::new(data_in_buffer, data_out_buffer),
            control_signal_pipe: SignalPipe::new(BusKind::Control, bounds),
            data_signal_pipe: SignalPipe::new(BusKind::Data, bounds),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OnlinePeerInfo {
    pub xaero_id: [u8; 32],
    pub last_seen: u64,
}
unsafe impl Pod for OnlinePeerInfo {}
unsafe impl Zeroable for OnlinePeerInfo {}

pub struct Group<
    const C_TSHIRT_SIZE: usize,
    const C_RING_CAPACITY: usize,
    const D_TSHIRT_SIZE: usize,
    const D_RING_CAPACITY: usize,
> {
    pub name: String,
    pub online_peers: Vec<OnlinePeerInfo>,
    pub workspaces: BTreeMap<
        [u8; 32],
        Workspace<C_TSHIRT_SIZE, C_RING_CAPACITY, D_TSHIRT_SIZE, D_RING_CAPACITY>,
    >,
    pub conduit: Conduit<C_TSHIRT_SIZE, C_RING_CAPACITY, D_TSHIRT_SIZE, D_RING_CAPACITY>,
}

#[derive(Debug)]
pub struct Workspace<
    const C_TSHIRT_SIZE: usize,
    const C_RING_CAPACITY: usize,
    const D_TSHIRT_SIZE: usize,
    const D_RING_CAPACITY: usize,
> {
    pub id: [u8; 32],
    pub online_peers: Vec<OnlinePeerInfo>,
    pub objects:
        BTreeMap<[u8; 32], Object<C_TSHIRT_SIZE, C_RING_CAPACITY, D_TSHIRT_SIZE, D_RING_CAPACITY>>,
    pub conduit: Conduit<C_TSHIRT_SIZE, C_RING_CAPACITY, D_TSHIRT_SIZE, D_RING_CAPACITY>,
}

#[derive(Debug)]
pub struct Object<
    const C_TSHIRT_SIZE: usize,
    const C_RING_CAPACITY: usize,
    const D_TSHIRT_SIZE: usize,
    const D_RING_CAPACITY: usize,
> {
    /// This is `SubjectHash` The SubjectHash can be lookedup quickly in lmdb.
    pub id: [u8; 32],
}

