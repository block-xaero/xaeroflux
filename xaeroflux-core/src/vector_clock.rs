use std::{cmp::Ordering, collections::HashMap};

use bytemuck::{Pod, Zeroable};
use rkyv::{Archive, Deserialize, Serialize};
use xaeroid::XaeroID;

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
