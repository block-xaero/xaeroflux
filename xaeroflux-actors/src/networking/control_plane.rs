use std::sync::Arc;

use crossbeam::channel::{self, Receiver, Sender};
use xaeroflux_core::P2P_RUNTIME;

use super::p2p::NetworkPayload;
use crate::system::control_bus::{ControlBus, SystemPayload};

/// ControlPlane manages relaying control events for the networking layer.
pub struct ControlPlane {
    pub control_tx: Sender<NetworkPayload>,
    pub control_rx: Receiver<NetworkPayload>,
}

impl ControlPlane {
    pub fn init_using(bus: Arc<ControlBus>) -> Arc<Self> {
        let control_rx = bus.subscribe();
        let control_tx = bus.tx.clone();
        let (tx, rx) = channel::unbounded::<NetworkPayload>();
        let txc = tx.clone();
        let rxc = rx.clone();
        let txcc = txc.clone();
        let rxcc = rxc.clone();
        // outgoing loop
        spin_outgoing_loop(tx, control_rx);
        spin_incoming_loop(rx, control_tx);
        Arc::new(ControlPlane {
            control_tx: txcc,
            control_rx: rxcc,
        })
    }
}
fn spin_incoming_loop(rxc: Receiver<NetworkPayload>, control_tx: Sender<SystemPayload>) {
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

fn spin_outgoing_loop(tx: Sender<NetworkPayload>, control_rx: Receiver<SystemPayload>) {
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
