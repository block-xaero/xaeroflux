use std::any::Any;

use rs_merkle::{algorithms::Sha256, MerkleProof, MerkleTree};
pub struct XaeroMerkleTree {
    pub underlying: MerkleTree<Sha256>,
}

pub struct XaeroMerkleDiffSummary {
    pub is_equal: bool,                      // Fast flag for exact match
    pub differing_indices: Option<Vec<usize>>, // Optional: leaf indices that differ (if known)
    pub peer_leaf_count: usize,              // Peer's tree size
    pub local_leaf_count: usize,             // Local tree size
    pub root_hash_local: [u8; 32],           // Local root
    pub root_hash_peer: [u8; 32],            // Peer root
}

pub trait XaeroMerkleTreeOps {
    fn insert_leaf(&mut self, leaf: &[u8]);
    fn insert_leaves(&mut self, leaves: &[&[u8]]);
    fn remove_leaf(&mut self, leaf: &[u8]);
    fn remove_leaves(&mut self, leaves: &[&[u8]]);
    fn root(&self) -> Vec<u8>;
    fn generate_proof(&self, leaf_hash: &[u8]) -> Vec<u8>;
    fn verify_proof(leaf_hash: [u8; 32], proof: &MerkleProof<Sha256>, root: [u8; 32]) -> bool
    fn diff_root(peer_root: [u8; 32]) -> XaeroMerkleDiffSummary;
    fn leaf_count() -> usize;
}    