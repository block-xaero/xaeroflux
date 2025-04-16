use std::fmt::Debug;

use tracing::{trace, warn};

use super::hash::{sha_256_concat, sha_256_concat_hash};
use crate::logs::init_logging;
/// Models a node in the Merkle tree.
/// # Generic Parameters
#[derive(Clone)]
pub struct XaeroMerkleNode {
    pub node_hash: [u8; 32],
    pub is_left: bool,
}
impl Debug for XaeroMerkleNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroMerkleNode")
            .field("node_hash", &hex::encode(self.node_hash))
            .field("is_left", &self.is_left)
            .finish()
    }
}
impl XaeroMerkleNode {
    pub fn new(node_hash: [u8; 32], is_left: bool) -> Self {
        Self { node_hash, is_left }
    }
}
pub struct XaeroMerkleProofSegment {
    pub node_hash: [u8; 32],
    pub is_left: bool,
}

pub struct XaeroMerkleProof {
    pub value: Vec<XaeroMerkleProofSegment>,
}

pub struct XaeroMerkleTree {
    pub root_hash: [u8; 32],
    pub leaves: Vec<XaeroMerkleNode>,
}

impl Debug for XaeroMerkleTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroMerkleTree")
            .field("root_hash", &hex::encode(self.root_hash))
            .field(
                "leaves",
                &self
                    .leaves
                    .iter()
                    .map(|x| hex::encode(x.node_hash))
                    .collect::<Vec<String>>(),
            )
            .finish()
    }
}
impl XaeroMerkleTree {
    fn _traverse_update_hash(&mut self, idx: usize) {
        trace!("<<< _traverse_update_hash: {}", idx);
        let parent_hash: [u8; 32] = sha_256_concat(&self.leaves[idx - 1], &self.leaves[idx]);
        let parent_idx = (idx - 1) / 2; // parent index
        trace!(
            "<<< _traverse_update_hash: {:#?} parent_idx: {:#?}",
            idx, parent_idx
        );
        let parent_idx_f = if parent_idx == 0 { 0 } else { parent_idx };
        if parent_idx_f == 0 {
            self.root_hash = parent_hash;
            trace!(
                "<<< _traverse_update_hash: root_hash: {:#?}",
                self.root_hash
            );
            return;
        }
        self._traverse_update_hash(parent_idx_f)
    }

    fn _traverse_generate_proof<'a>(
        &mut self,
        mut idx: usize,
        proof: &'a mut XaeroMerkleProof,
    ) -> Option<&'a mut XaeroMerkleProof> {
        if idx == 0 {
            Some(proof)
        } else {
            let is_even: bool = idx % 2 == 0;
            let parent_idx = (idx - 1) / 2;
            let proof_segment = XaeroMerkleProofSegment {
                node_hash: self.leaves[parent_idx].node_hash,
                is_left: is_even,
            };
            proof.value.push(proof_segment);
            if is_even {
                idx = parent_idx;
            } else {
                idx = parent_idx + 1;
            }
            self._traverse_generate_proof(idx, proof)
        }
    }
}
pub trait XaeroMerkleTreeOps {
    /// initialize Merkle tree leaves
    fn init(leaves: Vec<[u8; 32]>) -> XaeroMerkleTree;
    /// merkle tree root hash
    fn root(&self) -> [u8; 32];
    /// prove that data is part of tree and return the path
    fn generate_proof(&mut self, data: [u8; 32]) -> Option<XaeroMerkleProof>;
    /// verify that proof is valid
    fn verify_proof(&self, proof: XaeroMerkleProof, data: [u8; 32]) -> bool;
}

impl XaeroMerkleTreeOps for XaeroMerkleTree {
    fn init(mut data_to_push: Vec<[u8; 32]>) -> XaeroMerkleTree {
        init_logging();
        let mut tree = XaeroMerkleTree {
            root_hash: [0; 32],
            leaves: Vec::<XaeroMerkleNode>::new(),
        };
        let len = data_to_push.len();
        if len == 0 {
            warn!("No data to insert");
            return tree;
        } else if len == 1 {
            trace!("Single data point to insert : {:#?}", data_to_push[0]);
            tree.root_hash = data_to_push[0];
            tree.leaves
                .push(XaeroMerkleNode::new(data_to_push[0], true));
            return tree;
        } else if len == 2 {
            trace!(
                "inserting two nodes from {:#?} , {:#?}",
                data_to_push[0], data_to_push[1]
            );
            let left_node = XaeroMerkleNode::new(data_to_push[0], true);
            let right_node = XaeroMerkleNode::new(data_to_push[1], false);
            tree.root_hash = sha_256_concat(&left_node, &right_node);
            tree.leaves.push(left_node);
            tree.leaves.push(right_node);
            return tree;
        } else {
            trace!("inserting more than two nodes");
            while let Some(data) = data_to_push.pop() {
                if tree.leaves.is_empty() {
                    tree.root_hash = data;
                    let mut node = XaeroMerkleNode::new(data, true);
                    node.is_left = true;
                    trace!("(0) inserting node {:#?}", data);
                    tree.leaves.push(node);
                    continue;
                } else if tree.leaves.len() == 1 {
                    let mut node = XaeroMerkleNode::new(data, true);
                    node.is_left = true;
                    trace!("(1) inserting data {:#?}", data);
                    tree.leaves.push(node);
                    continue;
                } else if tree.leaves.len() == 2 {
                    let mut node = XaeroMerkleNode::new(data, true);
                    node.is_left = false;
                    tree.leaves.push(node);
                    tree.root_hash = sha_256_concat(&tree.leaves[1], &tree.leaves[2]);
                    continue;
                } else {
                    let mut idx = tree.leaves.len();
                    let node = XaeroMerkleNode::new(data, true);
                    tree.leaves.push(node);
                    while idx > 0 {
                        if tree.leaves.len() % 2 == 0 {
                            trace!("(loop) inserting node {:#?}", data);
                            tree.leaves[idx].is_left = true;
                            tree._traverse_update_hash(idx);
                        } else {
                            tree.leaves[idx].is_left = false;
                            trace!("(loop) inserting node {:#?}", data);
                            tree._traverse_update_hash(idx);
                        }
                        idx -= 1;
                    }
                }
            }
        }
        tree
    }

    fn root(&self) -> [u8; 32] {
        self.root_hash
    }

    fn generate_proof(&mut self, data: [u8; 32]) -> Option<XaeroMerkleProof> {
        let mut proof = XaeroMerkleProof {
            value: Vec::<XaeroMerkleProofSegment>::new(),
        };
        let mut idx = usize::MAX;
        for i in 0..self.leaves.len() {
            if self.leaves[i].node_hash == data {
                idx = i;
                break;
            }
        }
        if idx == usize::MAX {
            println!("Data not found in the tree");
            return None;
        }
        self._traverse_generate_proof(idx, &mut proof);
        Some(proof)
    }

    fn verify_proof(&self, proof: XaeroMerkleProof, data: [u8; 32]) -> bool {
        let mut data_leaf_idx = 0;
        for i in 0..proof.value.len() {
            if data == proof.value[i].node_hash {
                data_leaf_idx = i;
                break;
            }
        }
        let mut idx = data_leaf_idx;
        let mut hash = [0; 32];
        while idx > 0 {
            if proof.value[idx].is_left {
                hash = sha_256_concat_hash(
                    &proof.value[idx].node_hash,
                    &proof.value[idx + 1].node_hash,
                );
            } else {
                hash = sha_256_concat_hash(
                    &proof.value[idx - 1].node_hash,
                    &proof.value[idx].node_hash,
                );
            }
            idx = (idx - 1) / 2;
        }
        hash == self.root_hash
    }
}

#[cfg(test)]
mod tests {
    use tracing::trace;

    use super::*;
    use crate::{indexing::hash::sha_256, logs::init_logging};
    #[test]
    fn test_insert_leaves() {
        init_logging();
        let data = vec![
            sha_256::<String>(&String::from("XAERO")),
            sha_256::<String>(&String::from("XAER0")),
            sha_256::<String>(&String::from("XAER1")),
            sha_256::<String>(&String::from("XAER2")),
        ];
        let tree = XaeroMerkleTree::init(data);
        trace!("Tree initialized with leaves: {:#?}", tree.leaves);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        assert_eq!(tree.leaves.len(), 4);
    }

    #[test]
    fn test_verify_proof_no_data() {
        init_logging();
    }

    #[test]
    fn test_verify_proof_happy() {}

    #[test]
    fn test_generate_proof_happy() {}

    #[test]
    fn test_generate_proof_no_data() {}

    #[test]
    fn test_empty_tree() {}

    #[test]
    fn test_zero_data_leaves() {}
}
