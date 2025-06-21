use std::sync::Arc;

use rkyv::rancor::Failure;
use rusted_ring::{AllocationError, EventAllocator};
use xaeroflux_core::{
    date_time::emit_secs,
    event::{
        CRDT_COUNTER_DECREMENT, CRDT_COUNTER_INCREMENT, CRDT_COUNTER_STATE, CRDT_REGISTER_STATE,
        CRDT_REGISTER_WRITE, CRDT_SET_ADD, CRDT_SET_REMOVE, CRDT_SET_STATE, EventType, XaeroEvent,
    },
};

#[derive(Clone)]
pub enum Sort {
    /// Sort by vector clock (true causal ordering)
    VectorClock,
    /// Sort by operation ID (deterministic global order)
    OperationId,
    /// Sort by custom lamport timestamp
    LamportTimestamp,
}

#[allow(clippy::type_complexity)]
impl Sort {
    pub fn to_operator(
        self,
        _ea: &'static EventAllocator,
    ) -> Arc<dyn Fn(&Arc<XaeroEvent>, &Arc<XaeroEvent>) -> std::cmp::Ordering + Send + Sync> {
        match self {
            Sort::VectorClock => Arc::new(
                move |e1: &Arc<XaeroEvent>, e2: &Arc<XaeroEvent>| -> std::cmp::Ordering {
                    e1.latest_ts.cmp(&e2.latest_ts)
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

#[allow(clippy::type_complexity)]
impl Fold {
    pub fn to_operator(
        self,
        ea: &'static EventAllocator,
    ) -> Arc<
        dyn Fn(Arc<XaeroEvent>, Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError>
            + Send
            + Sync,
    > {
        match self {
            Fold::LWWRegister => Arc::new(
                move |acc: Arc<XaeroEvent>,
                      events: Vec<Arc<XaeroEvent>>|
                      -> Result<Arc<XaeroEvent>, AllocationError> {
                    // Find the latest write operation
                    let latest_write = events
                        .into_iter()
                        .filter(|e| {
                            matches!(
                                EventType::from_u8(e.event_type()),
                                EventType::ApplicationEvent(CRDT_REGISTER_WRITE)
                            )
                        })
                        .max_by_key(|event| event.latest_ts)
                        .unwrap_or(acc);

                    // Create new state event with latest write data
                    let allocated_event =
                        ea.allocate_event(latest_write.data(), CRDT_REGISTER_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: latest_write.author_id.clone(),
                        merkle_proof: latest_write.merkle_proof.clone(),
                        vector_clock: latest_write.vector_clock.clone(),
                        latest_ts: latest_write.latest_ts,
                    }))
                },
            ),

            Fold::ORSet => Arc::new(
                move |_acc: Arc<XaeroEvent>,
                      events: Vec<Arc<XaeroEvent>>|
                      -> Result<Arc<XaeroEvent>, AllocationError> {
                    use std::collections::HashSet;

                    // Use references to avoid copying data during processing
                    let mut final_set: HashSet<&[u8]> = HashSet::new();
                    let mut removed_elements: HashSet<&[u8]> = HashSet::new();

                    for event in &events {
                        match EventType::from_u8(event.event_type()) {
                            EventType::ApplicationEvent(CRDT_SET_ADD) => {
                                final_set.insert(event.data());
                            }
                            EventType::ApplicationEvent(CRDT_SET_REMOVE) => {
                                removed_elements.insert(event.data());
                            }
                            _ => {}
                        }
                    }

                    // Remove elements
                    for removed in &removed_elements {
                        final_set.remove(removed);
                    }

                    // Convert to owned data for serialization
                    let final_elements: Vec<Vec<u8>> =
                        final_set.iter().map(|data| data.to_vec()).collect();

                    // Use rkyv for serialization
                    let serialized = rkyv::to_bytes::<rkyv::rancor::Failure>(&final_elements)
                        .map_err(|_| AllocationError::EventCreation("Serialization failed"))?;

                    // Allocate in ring buffer using new API
                    let allocated_event = ea.allocate_event(&serialized, CRDT_SET_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: None,
                        merkle_proof: None,
                        vector_clock: None,
                        latest_ts: emit_secs(),
                    }))
                },
            ),

            Fold::GCounter => Arc::new(
                move |_acc: Arc<XaeroEvent>,
                      events: Vec<Arc<XaeroEvent>>|
                      -> Result<Arc<XaeroEvent>, AllocationError> {
                    let mut counter_value: i64 = 0;

                    // Sum all increment operations
                    for event in &events {
                        match EventType::from_u8(event.event_type()) {
                            EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT) => {
                                // Simple: treat event data as raw i64 bytes
                                let data = event.data();
                                if data.len() >= 8 {
                                    let bytes: [u8; 8] = data[0..8].try_into().unwrap_or([0; 8]);
                                    counter_value += i64::from_le_bytes(bytes);
                                }
                            }
                            _ => {} // G-Counter only increments
                        }
                    }

                    // Store as raw i64 bytes
                    let serialized = counter_value.to_le_bytes().to_vec();
                    let allocated_event =
                        ea.allocate_event(&serialized, CRDT_COUNTER_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: None,
                        merkle_proof: None,
                        vector_clock: None,
                        latest_ts: emit_secs(),
                    }))
                },
            ),

            Fold::PNCounter => Arc::new(
                move |_acc: Arc<XaeroEvent>,
                      events: Vec<Arc<XaeroEvent>>|
                      -> Result<Arc<XaeroEvent>, AllocationError> {
                    let mut counter_value: i64 = 0;

                    // Process increment and decrement operations
                    for event in &events {
                        let data = event.data();
                        match EventType::from_u8(event.event_type()) {
                            EventType::ApplicationEvent(CRDT_COUNTER_INCREMENT) => {
                                if data.len() >= 8 {
                                    let bytes: [u8; 8] = data[0..8].try_into().unwrap_or([0; 8]);
                                    counter_value += i64::from_le_bytes(bytes);
                                }
                            }
                            EventType::ApplicationEvent(CRDT_COUNTER_DECREMENT) => {
                                if data.len() >= 8 {
                                    let bytes: [u8; 8] = data[0..8].try_into().unwrap_or([0; 8]);
                                    counter_value -= i64::from_le_bytes(bytes);
                                }
                            }
                            _ => {}
                        }
                    }

                    // Store as raw i64 bytes
                    let serialized = counter_value.to_le_bytes().to_vec();
                    let allocated_event =
                        ea.allocate_event(&serialized, CRDT_COUNTER_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: None,
                        merkle_proof: None,
                        vector_clock: None,
                        latest_ts: emit_secs(),
                    }))
                },
            ),
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

#[allow(clippy::type_complexity)]
impl Reduce {
    pub fn to_operator(
        self,
        ea: &'static EventAllocator,
    ) -> Arc<dyn Fn(Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError> + Send + Sync>
    {
        match self {
            Reduce::CounterValue => Arc::new(
                move |events: Vec<Arc<XaeroEvent>>| -> Result<Arc<XaeroEvent>, AllocationError> {
                    // Extract the final counter value from the state event
                    for event in &events {
                        if matches!(
                            EventType::from_u8(event.event_type()),
                            EventType::ApplicationEvent(CRDT_COUNTER_STATE)
                        ) {
                            // Return the counter state as a new event
                            let allocated_event =
                                ea.allocate_event(event.data(), CRDT_COUNTER_STATE as u32)?;
                            let evt = allocated_event.into_pooled_event_ptr();

                            return Ok(Arc::new(XaeroEvent {
                                evt,
                                author_id: None,
                                merkle_proof: None,
                                vector_clock: None,
                                latest_ts: emit_secs(),
                            }));
                        }
                    }

                    // Default to 0 if no state found
                    let default_value = 0i64.to_le_bytes().to_vec();
                    let allocated_event =
                        ea.allocate_event(&default_value, CRDT_COUNTER_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: None,
                        merkle_proof: None,
                        vector_clock: None,
                        latest_ts: emit_secs(),
                    }))
                },
            ),

            Reduce::SetContents => Arc::new(
                move |events: Vec<Arc<XaeroEvent>>| -> Result<Arc<XaeroEvent>, AllocationError> {
                    // Extract the final set contents from the state event
                    for event in &events {
                        if matches!(
                            EventType::from_u8(event.event_type()),
                            EventType::ApplicationEvent(CRDT_SET_STATE)
                        ) {
                            // Return the set state as a new event
                            let allocated_event =
                                ea.allocate_event(event.data(), CRDT_SET_STATE as u32)?;
                            let evt = allocated_event.into_pooled_event_ptr();

                            return Ok(Arc::new(XaeroEvent {
                                evt,
                                author_id: None,
                                merkle_proof: None,
                                vector_clock: None,
                                latest_ts: emit_secs(),
                            }));
                        }
                    }

                    // Default to empty set
                    let empty_set: Vec<Vec<u8>> = Vec::new();
                    let serialized = rkyv::to_bytes::<Failure>(&empty_set)
                        .map_err(|_| AllocationError::EventCreation("Serialization failed"))?;

                    let allocated_event = ea.allocate_event(&serialized, CRDT_SET_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: None,
                        merkle_proof: None,
                        vector_clock: None,
                        latest_ts: emit_secs(),
                    }))
                },
            ),

            Reduce::RegisterValue => Arc::new(
                move |events: Vec<Arc<XaeroEvent>>| -> Result<Arc<XaeroEvent>, AllocationError> {
                    // Extract the final register value from the state event
                    for event in &events {
                        if matches!(
                            EventType::from_u8(event.event_type()),
                            EventType::ApplicationEvent(CRDT_REGISTER_STATE)
                        ) {
                            // Return the register state as a new event
                            let allocated_event =
                                ea.allocate_event(event.data(), CRDT_REGISTER_STATE as u32)?;
                            let evt = allocated_event.into_pooled_event_ptr();

                            return Ok(Arc::new(XaeroEvent {
                                evt,
                                author_id: None,
                                merkle_proof: None,
                                vector_clock: None,
                                latest_ts: emit_secs(),
                            }));
                        }
                    }

                    // Default to empty value
                    let allocated_event = ea.allocate_event(&[], CRDT_REGISTER_STATE as u32)?;
                    let evt = allocated_event.into_pooled_event_ptr();

                    Ok(Arc::new(XaeroEvent {
                        evt,
                        author_id: None,
                        merkle_proof: None,
                        vector_clock: None,
                        latest_ts: emit_secs(),
                    }))
                },
            ),
        }
    }
}
