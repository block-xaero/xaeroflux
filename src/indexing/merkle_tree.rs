use super::hash::{sha_256_concat, sha_256_concat_hash};

/// Models a node in the Merkle tree.
/// # Generic Parameters
#[derive(Clone, Debug)]
pub struct XaeroMerkleNode {
    pub node_hash: [u8; 32],
    pub is_left: bool,
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

impl XaeroMerkleTree {
    fn _traverse_update_hash(&mut self, idx: usize) {
        if idx == 0 {
            self.root_hash = self.leaves[idx].node_hash;
            return;
        }
        let is_even: bool = idx % 2 == 0;
        let parent_hash: [u8; 32] = if is_even {
            sha_256_concat(&self.leaves[idx - 1], &self.leaves[idx])
        } else {
            sha_256_concat(&self.leaves[idx], &self.leaves[idx + 1])
        };
        let parent_idx = (idx - 1) / 2;
        self.leaves[parent_idx].node_hash = parent_hash;
        self._traverse_update_hash(parent_idx)
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
        if data_to_push.is_empty() {
            panic!("Cannot create a Merkle tree with no leaves");
        }
        let mut tree = XaeroMerkleTree {
            root_hash: [0; 32],
            leaves: Vec::<XaeroMerkleNode>::new(),
        };

        while let Some(data) = data_to_push.pop() {
            if tree.leaves.len() % 2 == 0 {
                let left = XaeroMerkleNode::new(data, true);
                let right = left.clone();
                tree.leaves.push(left);
                tree.leaves.push(right);
                tree._traverse_update_hash(tree.leaves.len() - 1);
            } else {
                let right = XaeroMerkleNode::new(data, false);
                tree.leaves.push(right);
                tree._traverse_update_hash(tree.leaves.len() - 1);
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
    use crate::logs::init_logging;

    #[test]
    fn test_insert_leaves() {
        init_logging();
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
