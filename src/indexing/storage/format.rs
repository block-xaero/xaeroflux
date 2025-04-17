use crate::indexing::merkle_tree::XaeroMerkleNode;

pub const PAGE_SIZE: usize = 16_384;
pub const PAGE_HEADER_SIZE: usize = 4 + 1 + 7 + 8; // marker + event_type + version

/// The on-disk representation of a Merkle node, which is used for storage.
/// This is a C-compatible struct, so it can be used for FFI or direct memory access.
/// It contains a hash, flags, and padding to ensure the struct is the correct size.
/// The `hash` field is a 32-byte array that stores the hash of the node.
/// The `flags` field is a single byte that indicates the status of the node.
/// The `_pad` field is used for padding to ensure the struct is the correct size.
/// The `XaeroMerkleOnDiskNode` struct is used to represent a node in the Merkle tree
/// in a format that can be stored on disk.
#[repr(C)]
pub struct XaeroMerkleOnDiskNode {
    pub hash: [u8; 32], // 32 bytes
    pub flags: u8,      // 1 byte
    _pad: [u8; 7],      // 7 bytes
}

/// The on-disk representation of a page header, which contains metadata about the page.
/// This is a C-compatible struct, so it can be used for FFI or direct memory access.
/// It contains a magic number for sanity checking, an event type, a version number,
/// and a total number of nodes.
/// The `leaf_start` field indicates the starting index of the leaf nodes in the page.
/// The `event_type` field indicates the type of event that this page is associated with.
/// The `version` field is a monotonic version number that is incremented with each change.
/// The `_reserved` field is used for padding to ensure the struct is the correct size.
#[repr(C)]
pub struct XaeroMerkleOnDiskPageHeader {
    pub leaf_start: u64,
    pub total_nodes: u64,
    pub event_type: u8,
    pub version: u64,
    _reserved: [u8; PAGE_SIZE - 1 - 8 - 8 - 1],
}

/// The on-disk representation of a page, which is the raw format used for storage.
/// This is a C-compatible struct, so it can be used for FFI or direct memory access.
/// It contains a magic number for sanity checking, an event type, a version number,
/// and a fixed-size array of nodes.
#[repr(C)]
pub struct XaeroMerkleOnDiskPage {
    /// 4‑byte magic so we can sanity‑check
    pub marker: [u8; 4], // b"XAER"
    /// 1‑byte event type tag
    pub event_type: u8,
    /// 7 bytes reserved to align to 12 bytes so `version` hits a natural boundary
    pub _reserved1: [u8; 7],
    /// Monotonic page version (persisted)
    pub version: u64, // 8 bytes
    /// Node‑array follows immediately
    pub nodes: [u8; 16_384 - 4 - 1 - 7 - 8],
}

/// The in-memory representation of a page, which is a decoded version of the on-disk page
/// and is used for runtime operations.
pub struct XaeroMerklePage {
    pub header: XaeroMerkleOnDiskPageHeader, // marker + event_type + version
    pub nodes: Vec<XaeroMerkleNode>,         // decoded Merkle nodes
    pub is_dirty: bool,                      // runtime only—never persisted
}
