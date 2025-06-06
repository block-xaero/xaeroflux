use std::sync::Arc;

use crossbeam::channel::{Receiver, Sender};
use xaeroflux_core::P2P_RUNTIME;

use super::p2p::{ControlNetworkPipe, NetworkPayload};
use crate::{pipe::Pipe, system_payload::SystemPayload};
/// ControlPlane manages relaying control events for the networking layer.
pub struct ControlPlane {
    /// system payloads flow in through this.
    pub pipe: Arc<Pipe>,
    pub network: ControlNetworkPipe,
}

impl ControlPlane {
    pub fn init_using(_system: Arc<Pipe>, _network: ControlNetworkPipe) -> Arc<Self> {
        Arc::new(ControlPlane {
            pipe: _system,
            network: _network,
        })
    }
}
fn _spin_incoming_loop(rxc: Receiver<NetworkPayload>, control_tx: Sender<SystemPayload>) {
    P2P_RUNTIME
        .get()
        .expect("p2p_runtime_not_initialized")
        .spawn(async move {
            while let Ok(e) = rxc.recv() {
                let payload: SystemPayload = e.into();
                if control_tx.send(payload).is_err() {
                    tracing::error!("Failed to send SystemPayload event");
                } else {
                    tracing::debug!("Sent SystemPayload event");
                }
            }
        });
}

fn _spin_outgoing_loop(tx: Sender<NetworkPayload>, control_rx: Receiver<SystemPayload>) {
    P2P_RUNTIME
        .get()
        .expect("p2p_runtime_not_initialized")
        .spawn(async move {
            while let Ok(e) = control_rx.recv() {
                let payload: NetworkPayload = e.into();
                if tx.send(payload).is_err() {
                    tracing::error!("Failed to send NetworkPayload event");
                } else {
                    tracing::debug!("Sent NetworkPayload event");
                }
            }
        });
}
