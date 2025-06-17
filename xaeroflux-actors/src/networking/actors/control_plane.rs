use std::sync::Arc;

use crate::{
    networking::{control_plane::ControlPlane, iroh::IrohControlPlane},
    pipe::Pipe,
};

/// Receives messages from ControlBus and Relays it to ControlPlane - and out to peers.
/// Delegates a lot of work currently to `IrohControlPlane`
pub struct ControlPlaneActor {
    pub cp: Arc<ControlPlane>,
    pub(crate) iroh_control_plane: IrohControlPlane,
}

impl ControlPlaneActor {
    pub fn new(system: Arc<Pipe>, cp: Arc<ControlPlane>) -> Arc<Self> {
        Arc::new(ControlPlaneActor {
            cp: cp.clone(),
            iroh_control_plane: IrohControlPlane::new(cp.clone(), None),
        })
    }
}

#[cfg(test)]
mod tests {
    use xaeroflux_core::{init_p2p_runtime, initialize};

    use crate::{
        networking::{actors::control_plane::ControlPlaneActor, control_plane::ControlPlane, p2p::ControlNetworkPipe},
        pipe::{BusKind, Pipe},
    };

    #[test]
    pub fn test_construction_control_plane() {
        // initialize();
        // init_p2p_runtime();
        // let pipe = Pipe::new(BusKind::Control, Some(100));
        // let control_pipe = ControlNetworkPipe::new(Some(100));
        // let cp = ControlPlane::init_using(pipe.clone(), control_pipe);
        // let cpa = ControlPlaneActor::new(pipe, cp);
    }
}
