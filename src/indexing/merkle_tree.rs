use std::fmt::Debug;

use tracing::{trace, warn};

use super::hash::sha_256_concat_hash;
use crate::logs::init_logging;

/// Models a node in the Merkle tree.
#[derive(Clone, Default)]
pub struct XaeroMerkleNode {
    pub node_hash: [u8; 32],
    pub is_leaf: bool,
}
impl Debug for XaeroMerkleNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroMerkleNode")
            .field("node_hash", &hex::encode(self.node_hash))
            .field("is_leaf", &self.is_leaf)
            .finish()
    }
}
impl XaeroMerkleNode {
    pub fn new(node_hash: [u8; 32], is_leaf: bool) -> Self {
        Self { node_hash, is_leaf }
    }
}
pub struct XaeroMerkleProofSegment {
    pub node_hash: [u8; 32],
    pub is_left: bool,
}

impl Debug for XaeroMerkleProofSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroMerkleProofSegment")
            .field("node_hash", &hex::encode(self.node_hash))
            .field("is_left", &self.is_left)
            .finish()
    }
}

pub struct XaeroMerkleProof {
    pub value: Vec<XaeroMerkleProofSegment>,
}

impl Debug for XaeroMerkleProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroMerkleProof")
            .field(
                "value",
                &self
                    .value
                    .iter()
                    .map(|x| hex::encode(x.node_hash))
                    .collect::<Vec<String>>(),
            )
            .finish()
    }
}

pub struct XaeroMerkleTree {
    pub root_hash: [u8; 32],
    pub nodes: Vec<XaeroMerkleNode>,
    pub leaf_start: usize,
    pub total_size: usize,
}

impl Debug for XaeroMerkleTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroMerkleTree")
            .field("root_hash", &hex::encode(self.root_hash))
            .field(
                "nodes",
                &self
                    .nodes
                    .iter()
                    .map(|x| hex::encode(x.node_hash))
                    .collect::<Vec<String>>(),
            )
            .finish()
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
        trace!(">>> data_to_push was: {:#?}", data_to_push);
        if data_to_push.is_empty() {
            return XaeroMerkleTree {
                leaf_start: 0,
                root_hash: [0; 32],
                nodes: Vec::new(),
                total_size: 0,
            };
        }
        // symmetrically add the last node if odd number of leaves
        if data_to_push.len() % 2 != 0 {
            let n = *data_to_push.last().expect("Last node should exist");
            data_to_push.push(n);
            trace!("Added dummy node: {:#?}", hex::encode(n));
        }
        // TODO: Timeseries sort before initializing??
        let input_data_size = data_to_push.len();
        let tree_size = 2 * input_data_size - 1; // L + L/2 + L4 + ... + 1
        trace!("Tree size: {}", tree_size);
        let mut nodes = Vec::<XaeroMerkleNode>::with_capacity(tree_size);
        nodes.resize_with(tree_size, XaeroMerkleNode::default);
        let leaf_start = tree_size - input_data_size;
        trace!("Leaf start set as  {}", leaf_start);
        trace!("building tree now with {} leaves", input_data_size);
        for (i, data) in data_to_push.iter().enumerate() {
            nodes[leaf_start + i] = XaeroMerkleNode::new(*data, true);
            trace!(
                "Leaf node: {:#?} at index: {}",
                hex::encode(data),
                leaf_start + i
            );
        }
        let mut tree = XaeroMerkleTree {
            root_hash: [0; 32],
            nodes,
            leaf_start,
            total_size: tree_size, // initialize with leaves on the end (from leaf_start)
        };
        // re-build the "tree"
        // interior nodes occupy 0 .. leaf_start
        for i in (0..leaf_start).rev() {
            let l = &tree.nodes[2 * i + 1].node_hash;
            let r = &tree.nodes[2 * i + 2].node_hash;
            let h = sha_256_concat_hash(l, r);
            tree.nodes[i] = XaeroMerkleNode::new(h, false);
        }
        tree.root_hash = tree.nodes[0].node_hash;
        trace!("<<< init: {:#?}", tree);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        trace!("Leaf start: {}", tree.leaf_start);
        trace!("Total size: {}", tree.total_size);
        trace!("Nodes: {:#?}", tree.nodes);
        trace!("Leaves: {:#?}", tree.leaf_start);
        trace!(
            "Tree size (leaves + internal nodes) : {:#?}",
            tree.nodes.len()
        );
        tree
    }

    fn root(&self) -> [u8; 32] {
        self.root_hash
    }

    fn generate_proof(&mut self, data: [u8; 32]) -> Option<XaeroMerkleProof> {
        trace!(">>> generate_proof: {:#?}", data);
        let mut proof = XaeroMerkleProof { value: Vec::new() };
        let found_node = self.nodes[self.leaf_start..]
            .iter()
            .enumerate()
            .find(|(_, node)| node.node_hash == data);
        match found_node {
            Some((i, node)) => {
                trace!("Found node: {:#?} at index: {}", node, self.leaf_start + i);
                let mut idx = self.leaf_start + i;
                while idx > 0 {
                    if idx % 2 == 0 {
                        // even index, left child
                        let segment = XaeroMerkleProofSegment {
                            node_hash: self.nodes[idx - 1].node_hash,
                            is_left: true,
                        };
                        proof.value.push(segment);
                    } else {
                        // odd index, right child
                        let segment = XaeroMerkleProofSegment {
                            node_hash: self.nodes[idx + 1].node_hash,
                            is_left: false,
                        };
                        proof.value.push(segment);
                    }
                    idx = (idx - 1) / 2; // move to parent
                    trace!(
                        "Moving to parent node: {:#?} at index: {}",
                        self.nodes[idx], idx
                    );
                    if idx == 0 {
                        break;
                    }
                }
                trace!("<<< generate_proof: {:#?}", proof);
                Some(proof)
            }
            None => {
                warn!("Data not found in tree");
                None
            }
        }
    }

    fn verify_proof(&self, proof: XaeroMerkleProof, data: [u8; 32]) -> bool {
        trace!(">>> verify_proof: {:#?}", proof);
        let mut running_hash = data;
        for segment in &proof.value {
            if segment.is_left {
                running_hash = sha_256_concat_hash(&segment.node_hash, &running_hash)
            } else {
                running_hash = sha_256_concat_hash(&running_hash, &segment.node_hash)
            }
        }
        trace!("<<< verify_proof: {:#?}", hex::encode(running_hash));
        running_hash == self.root_hash
    }
}

#[cfg(test)]
mod tests {
    use tracing::trace;

    use super::*;
    use crate::{indexing::hash::sha_256, logs::init_logging};

    #[test]
    fn test_empty_tree() {
        init_logging();
        let data = vec![];
        let tree = XaeroMerkleTree::init(data);
        trace!("Tree initialized with leaves: {:#?}", tree.nodes);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        assert_eq!(tree.nodes.len(), 0);
        assert_eq!(tree.root_hash, [0; 32]);
    }

    #[test]
    fn test_single_node_tree() {
        init_logging();
        let data = vec![sha_256::<String>(&String::from("XAERO"))];
        let tree = XaeroMerkleTree::init(data);
        trace!("Tree initialized with leaves: {:#?}", tree.nodes);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        assert_eq!(tree.nodes.len(), 3);
    }
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
        trace!("Tree initialized with leaves: {:#?}", tree.nodes);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        assert_eq!(tree.nodes.len(), 7);
    }

    #[test]
    fn test_verify_proof_no_data() {
        init_logging();
        let data = vec![
            sha_256::<String>(&String::from("XAERO")),
            sha_256::<String>(&String::from("XAER0")),
            sha_256::<String>(&String::from("XAER1")),
            sha_256::<String>(&String::from("XAER2")),
        ];
        let mut tree = XaeroMerkleTree::init(data);
        trace!("Tree initialized with leaves: {:#?}", tree.nodes);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        let data_to_prove = sha_256::<String>(&String::from("XAER0"));
        let proof = tree.generate_proof(data_to_prove);
        trace!("XAER3 Proof: {:#?}", proof);

        // client side
        let data = vec![
            sha_256::<String>(&String::from("XAER3")),
            sha_256::<String>(&String::from("XAER4")),
            sha_256::<String>(&String::from("XAER5")),
            sha_256::<String>(&String::from("XAER6")),
        ];
        let xaer3_tree = XaeroMerkleTree::init(data);
        trace!("XAER3: Tree initialized with leaves: {:#?}", tree.nodes);
        assert!(
            !xaer3_tree.verify_proof(proof.expect("Proof provided was invalid"), data_to_prove)
        );
    }

    #[test]
    fn test_verify_proof_happy() {
        init_logging();
        init_logging();
        let data = vec![
            sha_256::<String>(&String::from("XAERO")),
            sha_256::<String>(&String::from("XAER0")),
            sha_256::<String>(&String::from("XAER1")),
            sha_256::<String>(&String::from("XAER2")),
        ];
        let mut tree = XaeroMerkleTree::init(data);
        trace!("Tree initialized with leaves: {:#?}", tree.nodes);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        let data_to_prove = sha_256::<String>(&String::from("XAER0"));
        let proof = tree.generate_proof(data_to_prove);
        trace!("XAER3 Proof: {:#?}", proof);

        // client side
        let data = vec![
            sha_256::<String>(&String::from("XAER3")),
            sha_256::<String>(&String::from("XAER4")),
            sha_256::<String>(&String::from("XAER5")),
            sha_256::<String>(&String::from("XAER6")),
        ];
        let xaer3_tree = XaeroMerkleTree::init(data);
        trace!("XAER3: Tree initialized with leaves: {:#?}", tree.nodes);
        assert!(
            !xaer3_tree.verify_proof(proof.expect("Proof provided was invalid"), data_to_prove)
        );
    }

    #[test]
    fn test_generate_proof() {
        init_logging();
        let data = vec![
            sha_256::<String>(&String::from("XAERO")),
            sha_256::<String>(&String::from("XAER0")),
            sha_256::<String>(&String::from("XAER1")),
            sha_256::<String>(&String::from("XAER2")),
        ];
        let mut tree = XaeroMerkleTree::init(data);
        let data_to_prove = sha_256::<String>(&String::from("XAER0"));
        let proof = tree.generate_proof(data_to_prove);
        trace!("XAER0 Proof: {:#?}", proof);
        assert!(proof.is_some());
        let is_valid = tree.verify_proof(proof.expect("proof expected"), data_to_prove);
        trace!("XAER0 is_valid: {:#?}", is_valid);
        assert!(is_valid);
        let data_to_prove = sha_256::<String>(&String::from("XAER3"));
        let proof = tree.generate_proof(data_to_prove);
        trace!("XAER3 Proof: {:#?}", proof);
        assert!(proof.is_none());
        let data_to_prove = sha_256::<String>(&String::from("XAER2"));
        let proof = tree.generate_proof(data_to_prove);
        trace!("XAER3 Proof: {:#?}", proof);
    }

    #[test]
    fn test_generate_proof_no_data() {
        init_logging();
        let data = vec![
            sha_256::<String>(&String::from("XAERO")),
            sha_256::<String>(&String::from("XAER0")),
            sha_256::<String>(&String::from("XAER1")),
            sha_256::<String>(&String::from("XAER2")),
        ];
        let mut tree = XaeroMerkleTree::init(data);
        let proof = tree.generate_proof(sha_256(&String::from("XAER3")));
        trace!("XAER3 Proof: {:#?}", proof);
        assert!(proof.is_none());
    }
}
