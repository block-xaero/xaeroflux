use bytemuck::{Pod, Zeroable};
use rkyv::Archive;

pub const PAGE_SIZE: usize = 16 * 1024; // 16384
const HEADER_SIZE: usize = 4 + 1 + 7 + 8 + 8 + 8; // = 36
const NODE_SIZE: usize = 32 + 1 + 7; // = 40
const NODES_PER_PAGE: usize = (PAGE_SIZE - HEADER_SIZE) / NODE_SIZE; // 408

#[repr(C)]
#[derive(Clone, Copy, Archive)]
pub struct XaeroOnDiskNode {
    pub hash: [u8; 32],
    pub flags: u8,
    _pad: [u8; 7], // 32+1+7 = 40
}
unsafe impl Zeroable for XaeroOnDiskNode {}
unsafe impl Pod for XaeroOnDiskNode {}

#[repr(C)]
#[derive(Clone, Copy, Archive)]
pub struct XaeroOnDiskPage {
    pub marker: [u8; 4],                          // "XAER"
    pub event_type: u8,                           // 1 byte
    pub _pad1: [u8; 3],                           // align `version`
    pub version: u64,                             // 8
    pub leaf_start: u64,                          // 8
    pub total_nodes: u64,                         // 8
    pub nodes: [XaeroOnDiskNode; NODES_PER_PAGE], // 408*40 = 16320
    _pad2: [u8; 32],                              // Adjusted padding to ensure correct size
}
unsafe impl Zeroable for XaeroOnDiskPage {}
unsafe impl Pod for XaeroOnDiskPage {}

/// NOTE: DO NOT REMOVE THIS ASSERTION EVER - THIS NEEDS TO LINE UP PROPERLY
const _: () = assert!(std::mem::size_of::<XaeroOnDiskPage>() == PAGE_SIZE);
