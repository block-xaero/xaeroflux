// xaeroflux-crdt/src/lib.rs

#![feature(trivial_bounds)]
use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use bytemuck::{Pod, Zeroable};
use rkyv::rancor::Failure;
use rkyv::{Archive, Deserialize, Serialize};
use xaeroflux_core::event::{Event, EventType, XaeroEvent, CRDT_COUNTER_DECREMENT, CRDT_COUNTER_INCREMENT, CRDT_COUNTER_STATE, CRDT_REGISTER_STATE, CRDT_REGISTER_WRITE, CRDT_SET_ADD, CRDT_SET_REMOVE, CRDT_SET_STATE};
use xaeroid::XaeroID;

#[repr(C)]
#[derive(Clone, Archive, Serialize, Deserialize)]
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
                    .iter()
                    .map(|(_k, v)| v)
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
                    .iter()
                    .map(|(_k, v)| v)
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
        self.latest_timestamp == other.latest_timestamp &&
            self.neighbor_clocks == other.neighbor_clocks
    }
}

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

#[derive(Clone)]
pub enum Sort {
    /// Sort by vector clock (true causal ordering)
    VectorClock,
    /// Sort by operation ID (deterministic global order)
    OperationId,
    /// Sort by custom lamport timestamp
    LamportTimestamp,
}

// Separate storage for vector clocks
pub struct VectorClockStore {
    clocks: HashMap<XaeroID, VectorClock>,
}

impl VectorClockStore {
    pub fn new() -> Self {
        VectorClockStore {
            clocks: HashMap::new(),
        }
    }

    pub fn update_from_event(&mut self, e: &XaeroEvent) {
        // Extract author info from event (you'll need to add these fields to XaeroEvent)
        // For now, we'll use a placeholder approach
        // self.clocks.entry(e.author_id).or_insert_with(|| VectorClock::new(0, HashMap::new()));
        todo!("Need author_id and timestamp in XaeroEvent")
    }
}

impl Sort {
    pub fn to_operator(
        self,
    ) -> Arc<dyn Fn(&XaeroEvent, &XaeroEvent) -> std::cmp::Ordering + Send + Sync> {
        match self {
            Sort::VectorClock => Arc::new(
                move |e1: &XaeroEvent, e2: &XaeroEvent| -> std::cmp::Ordering {
                    e1.evt.ts.cmp(&e2.evt.ts)
                },
            ),
            _ => panic!("not supported yet"),
        }
    }
}

#[derive(Clone)]
pub enum Fold {
    /// Last-Writer-Wins Register
    LWWRegister,
    /// Observed-Remove Set
    ORSet,
    /// G-Counter (increment-only)
    GCounter,
    /// PN-Counter (increment/decrement)
    PNCounter,
}

impl Fold {
    pub fn to_operator(
        self,
    ) -> Arc<dyn Fn(XaeroEvent, Vec<XaeroEvent>) -> XaeroEvent + Send + Sync> {
        match self {
            Fold::LWWRegister => Arc::new(|acc: XaeroEvent, events: Vec<XaeroEvent>| -> XaeroEvent {
                // Find the latest write operation
                let latest_write = events
                    .into_iter()
                    .filter(|e| matches!(e.evt.event_type, EventType::ApplicationEvent(CRDT_REGISTER_WRITE)))
                    .max_by_key(|event| event.evt.ts)
                    .unwrap_or(acc);

                // Return the winning write as final register state
                XaeroEvent {
                    evt: Event::new(latest_write.evt.data, CRDT_REGISTER_STATE),
                    merkle_proof: latest_write.merkle_proof,
                    author_id: None,
                    latest_ts: None,
                }
            }),

            Fold::ORSet => Arc::new(|_acc: XaeroEvent, events: Vec<XaeroEvent>| -> XaeroEvent {
                use std::collections::HashSet;

                let mut final_set = HashSet::new();
                let mut removed_elements = HashSet::new();

                // Process all add/remove operations
                for event in events {
                    match event.evt.event_type {
                        EventType::ApplicationEvent(CRDT_SET_ADD) => {
                            final_set.insert(event.evt.data.clone());
                        }
                        EventType::ApplicationEvent(CRDT_SET_REMOVE) => {
                            removed_elements.insert(event.evt.data.clone());
                        }
                        _ => {} // Ignore non-set operations
                    }
                }

                // Remove elements that were explicitly removed
                for removed in &removed_elements {
                    final_set.remove(removed);
                }

                // Serialize final set state using rkyv for Vec<Vec<u8>>
                let final_elements: Vec<Vec<u8>> = final_set.into_iter().collect();
                let serialized = rkyv::to_bytes::<Failure>(&final_elements).unwrap_or_default();

                XaeroEvent {
                    evt: Event::new(serialized.to_vec(), CRDT_SET_STATE),
                    merkle_proof: None,
                    author_id: None,
                    latest_ts: None,
                }
            }),

            Fold::GCounter => Arc::new(|_acc: XaeroEvent, events: Vec<XaeroEvent>| -> XaeroEvent {
                let mut counter_value: i64 = 0;

                // Sum all increment operations
                for event in events {
                    match event.evt.event_type {
                        EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT) => {
                            // Simple: treat event data as raw i64 bytes
                            if event.evt.data.len() >= 8 {
                                let bytes: [u8; 8] = event.evt.data[0..8].try_into().unwrap_or([0; 8]);
                                counter_value += i64::from_le_bytes(bytes);
                            }
                        }
                        _ => {} // G-Counter only increments
                    }
                }

                // Store as raw i64 bytes
                let serialized = counter_value.to_le_bytes().to_vec();
                XaeroEvent {
                    evt: Event::new(serialized, CRDT_COUNTER_STATE),
                    merkle_proof: None,
                    author_id: None,
                    latest_ts: None,
                }
            }),

            Fold::PNCounter => Arc::new(|_acc: XaeroEvent, events: Vec<XaeroEvent>| -> XaeroEvent {
                let mut counter_value: i64 = 0;

                // Process increment and decrement operations
                for event in events {
                    match event.evt.event_type {
                        EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT) => {
                            if event.evt.data.len() >= 8 {
                                let bytes: [u8; 8] = event.evt.data[0..8].try_into().unwrap_or([0; 8]);
                                counter_value += i64::from_le_bytes(bytes);
                            }
                        }
                        EventType::ApplicationEvent(CRDT_COUNTER_DECREMENT) => {
                            if event.evt.data.len() >= 8 {
                                let bytes: [u8; 8] = event.evt.data[0..8].try_into().unwrap_or([0; 8]);
                                counter_value -= i64::from_le_bytes(bytes);
                            }
                        }
                        _ => {}
                    }
                }

                // Store as raw i64 bytes
                let serialized = counter_value.to_le_bytes().to_vec();
                XaeroEvent {
                    evt: Event::new(serialized, CRDT_COUNTER_STATE),
                    merkle_proof: None,
                    author_id: None,
                    latest_ts: None,
                }
            }),
        }
    }
}

#[derive(Clone)]
pub enum Reduce {
    /// Reduce to final counter value
    CounterValue,
    /// Reduce to final set contents
    SetContents,
    /// Reduce to final register value
    RegisterValue,
}

impl Reduce {
    pub fn to_operator(self) -> Arc<dyn Fn(Vec<XaeroEvent>) -> Vec<u8> + Send + Sync> {
        match self {
            Reduce::CounterValue => Arc::new(|events: Vec<XaeroEvent>| -> Vec<u8> {
                // Extract the final counter value from the state event
                for event in events {
                    if matches!(event.evt.event_type, EventType::ApplicationEvent(CRDT_COUNTER_STATE)) {
                        return event.evt.data;
                    }
                }
                // Default to 0 if no state found
                0i64.to_le_bytes().to_vec()
            }),

            Reduce::SetContents => Arc::new(|events: Vec<XaeroEvent>| -> Vec<u8> {
                // Extract the final set contents from the state event
                for event in events {
                    if matches!(event.evt.event_type, EventType::ApplicationEvent(CRDT_SET_STATE)) {
                        return event.evt.data;
                    }
                }
                // Default to empty set
                let empty_set: Vec<Vec<u8>> = Vec::new();
                rkyv::to_bytes::<Failure>(&empty_set).unwrap_or_default().to_vec()
            }),

            Reduce::RegisterValue => Arc::new(|events: Vec<XaeroEvent>| -> Vec<u8> {
                // Extract the final register value from the state event
                for event in events {
                    if matches!(event.evt.event_type, EventType::ApplicationEvent(CRDT_REGISTER_STATE)) {
                        return event.evt.data;
                    }
                }
                // Default to empty value
                Vec::new()
            }),
        }
    }
}