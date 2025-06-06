use std::sync::Arc;

use crossbeam::channel::Sender;
use xaeroflux_core::{event::Event, listeners::EventListener};

use crate::{
    networking::{control_plane::ControlPlane, iroh::IrohControlPlane},
    pipe::Pipe,
};

/// Receives messages from ControlBus and Relays it to ControlPlane - and out to peers.
/// Delegates a lot of work currently to `IrohControlPlane`
pub struct ControlPlaneActor {
    pub pipe: Arc<Pipe>,
    pub cp: Arc<ControlPlane>,
    pub listener: EventListener<Event<Vec<u8>>>,
    pub(crate) iroh_control_plane: IrohControlPlane,
}
