// xaeroflux/xaeroflux/src/lib.rs

// Re-export everything from `xaeroflux-core` so users of `xaeroflux` get core‐types automatically:
pub use xaeroflux_core::*;

// Re-export your actors module under `.actors::…`:
pub mod actors {
    pub use xaeroflux_actors::*;
}
