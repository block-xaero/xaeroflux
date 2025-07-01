use std::collections::BTreeMap;

use rusted_ring_new::RingBuffer;

#[repr(C, align(64))]
pub struct Node {
    pub id: [u8; 32],
    pub children: BTreeMap<[u8; 16], Box<Node>>,
}
