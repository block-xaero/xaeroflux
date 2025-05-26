use std::sync::OnceLock;

use control_bus::ControlBus;

pub mod control_bus;

pub const CONTROL_BUS: OnceLock<ControlBus> = OnceLock::new();

pub fn init_control_bus() {
    CONTROL_BUS.get_or_init(ControlBus::new);
}
