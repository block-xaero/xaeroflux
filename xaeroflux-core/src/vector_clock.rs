use std::{cmp::Ordering, collections::HashMap};

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};
use xaeroid::XaeroID;

use crate::date_time::emit_secs;

#[repr(C)]
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct VectorClock {
    pub latest_timestamp: u64,
    pub neighbor_clocks: HashMap<XaeroID, u64>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VectorClockEntry {
    pub timestamp: u64,
    pub xaero_id: XaeroID,
}
unsafe impl Zeroable for VectorClockEntry {}
unsafe impl Pod for VectorClockEntry {}

impl VectorClock {
    pub fn new(latest_timestamp: u64, neighbor_clocks: HashMap<XaeroID, u64>) -> Self {
        VectorClock {
            latest_timestamp: latest_timestamp.max(
                neighbor_clocks
                    .values()
                    .max()
                    .unwrap_or(&latest_timestamp)
                    .wrapping_add(1),
            ),
            neighbor_clocks,
        }
    }

    pub fn tick(&mut self, xaero_id: XaeroID) {
        self.latest_timestamp = self.latest_timestamp.wrapping_add(1);
        self.neighbor_clocks.insert(xaero_id, self.latest_timestamp);
        self.neighbor_clocks.retain(|_, v| *v > 0);
    }

    pub fn resync(&mut self, neighbor_clocks: HashMap<XaeroID, u64>) -> Self {
        VectorClock {
            latest_timestamp: self.latest_timestamp.max(
                neighbor_clocks
                    .values()
                    .max()
                    .unwrap_or(&self.latest_timestamp)
                    .wrapping_add(1),
            ),
            neighbor_clocks,
        }
    }

    pub fn update(&mut self, neighbor_clock: VectorClockEntry) -> bool {
        self.neighbor_clocks
            .insert(neighbor_clock.xaero_id, neighbor_clock.timestamp);
        self.latest_timestamp = self
            .latest_timestamp
            .max(neighbor_clock.timestamp)
            .wrapping_add(1);
        true
    }
}

impl Eq for VectorClock {}

impl PartialEq<Self> for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        self.latest_timestamp == other.latest_timestamp
            && self.neighbor_clocks == other.neighbor_clocks
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd<Self> for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.latest_timestamp.partial_cmp(&other.latest_timestamp)
    }
}

impl Ord for VectorClock {
    fn cmp(&self, other: &Self) -> Ordering {
        self.latest_timestamp.cmp(&other.latest_timestamp)
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct XaeroClock {
    pub base_ts: u64,          // 8 bytes
    pub latest_timestamp: u16, // 2 bytes
    pub _pad: [u8; 6],         // 6 bytes padding = 16 bytes total
}

unsafe impl Pod for XaeroClock {}
unsafe impl Zeroable for XaeroClock {}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct XaeroVectorClockEntry(pub [u8; 32], pub XaeroClock); // 32 + 16 = 48 bytes
unsafe impl Pod for XaeroVectorClockEntry {}
unsafe impl Zeroable for XaeroVectorClockEntry {}

impl Default for XaeroClock {
    fn default() -> Self {
        Self::new()
    }
}

impl XaeroClock {
    pub fn with_base(base_ts: u64, latest_timestamp: u16) -> Self {
        Self {
            base_ts,
            latest_timestamp,
            _pad: [0; 6],
        }
    }

    pub fn new() -> Self {
        Self {
            base_ts: emit_secs(),
            latest_timestamp: 0, // Start at 0, tick() will increment
            _pad: [0; 6],
        }
    }

    pub fn logical_timestamp(&self) -> u64 {
        self.base_ts + self.latest_timestamp as u64
    }
}

// Support 10 peers - increase structure size
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct XaeroVectorClock {
    pub clock: XaeroClock,                  // 16 bytes
    pub peers: [XaeroVectorClockEntry; 10], // 10 * 48 = 480 bytes
    pub _pad: [u8; 8],                      /* 8 bytes padding
                                             * Total: 16 + 480 + 8 = 504 bytes */
}
unsafe impl Pod for XaeroVectorClock {}
unsafe impl Zeroable for XaeroVectorClock {}
