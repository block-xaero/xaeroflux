#[allow(deprecated)]
use std::fmt::Debug;

use rkyv::{Archive, Deserialize, Serialize};
use tracing::{trace, warn};

use crate::{hash::sha_256_concat_hash, logs::init_logging};

/// Models a node in the Merkle tree.
#[derive(Clone, Archive, Serialize, Deserialize, Copy)]
#[rkyv(derive(Debug))]
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

#[derive(Clone, Copy, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
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

#[derive(Clone, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct XaeroMerkleProof {
    pub leaf_index: usize,
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

#[derive(Clone, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
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
    fn neo(leaves: Vec<[u8; 32]>) -> XaeroMerkleTree;

    /// merkle tree root hash
    fn root(&self) -> [u8; 32];

    /// insert page root hash because of global tree.
    fn insert_root(&mut self, page_root_hash: [u8; 32]) -> bool;

    /// insert page worth of leaves
    fn insert_page(&mut self, leaves: Vec<[u8; 32]>) -> bool;

    /// prove that data is part of tree and return the path
    fn generate_proof(&mut self, data: [u8; 32]) -> Option<XaeroMerkleProof>;
    /// verify that proof is valid
    fn verify_proof(&self, proof: XaeroMerkleProof, data: [u8; 32]) -> bool;
}

impl XaeroMerkleTreeOps for XaeroMerkleTree {
    fn neo(mut data_to_push: Vec<[u8; 32]>) -> XaeroMerkleTree {
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
        if !data_to_push.len().is_multiple_of(2) {
            let n = *data_to_push.last().expect("Last node should exist");
            data_to_push.push(n);
            trace!("Added dummy node: {:#?}", hex::encode(n));
        }
        // TODO: Timeseries sort before initializing??
        let leaves = data_to_push.len();
        let tree_size = 2 * leaves - 1; // L + L/2 + L4 + ... + 1
        trace!("Tree size: {}", tree_size);
        let mut nodes = Vec::<XaeroMerkleNode>::with_capacity(tree_size);
        nodes.resize_with(tree_size, || XaeroMerkleNode::new([0; 32], true));
        let leaf_start = tree_size - leaves;
        trace!("Leaf start set as  {}", leaf_start);
        trace!("building tree now with {} leaves", leaves);
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

    fn insert_root(&mut self, page_root_hash: [u8; 32]) -> bool {
        self.insert_page(vec![page_root_hash])
    }

    fn insert_page(&mut self, mut page: Vec<[u8; 32]>) -> bool {
        trace!(">>> insert_page called with {:#?}", page);
        if page.is_empty() {
            return false;
        }
        let leaf_count = self.total_size - self.leaf_start;
        // symmetrically add the last node if odd number of leaves
        if !(leaf_count + page.len()).is_multiple_of(2) {
            let n = *page.last().expect("Last node should exist");
            page.push(n);
            trace!("Added dummy node: {:#?}", hex::encode(n));
        }
        let old_leaf_start = self.leaf_start;
        let new_leaf_count = leaf_count + page.len();
        self.total_size = 2 * new_leaf_count - 1;
        self.leaf_start = self.total_size - new_leaf_count;
        trace!("New leaf count: {}", new_leaf_count);
        trace!("New leaf start: {}", self.leaf_start);
        trace!("New total size: {}", self.total_size);
        // resize the nodes vector to accommodate new leaves
        self.nodes
            .resize_with(self.total_size, || XaeroMerkleNode::new([0; 32], true));
        self.nodes
            .copy_within(old_leaf_start..old_leaf_start + leaf_count, self.leaf_start);

        for (i, data) in page.iter().enumerate() {
            let idx = self.leaf_start + leaf_count + i;
            self.nodes[idx] = XaeroMerkleNode::new(*data, true);
            trace!("Leaf node: {:#?} at index: {}", hex::encode(data), idx);
        }
        let start_indices = leaf_count..(leaf_count + page.len());
        for leaf_offset in start_indices {
            let mut idx = self.leaf_start + leaf_offset;
            while idx > 0 {
                let parent = (idx - 1) / 2;
                let l = &self.nodes[2 * parent + 1].node_hash;
                let r = &self.nodes[2 * parent + 2].node_hash;
                let h = sha_256_concat_hash(l, r);
                self.nodes[parent].node_hash = h;
                idx = parent;
            }
        }
        // re-build the tree from the new leaves
        trace!("<<< insert_page called with {:#?}", page);
        true
    }

    fn generate_proof(&mut self, data: [u8; 32]) -> Option<XaeroMerkleProof> {
        trace!(">>> generate_proof: {:#?}", data);
        let mut proof = XaeroMerkleProof {
            leaf_index: 0,
            value: Vec::new(),
        };
        let found_node = self.nodes[self.leaf_start..]
            .iter()
            .enumerate()
            .find(|(_, node)| node.node_hash == data);
        match found_node {
            Some((i, node)) => {
                proof.leaf_index = i; // setting proof 
                trace!("Found node: {:#?} at index: {}", node, self.leaf_start + i);
                let mut idx = self.leaf_start + i;
                while idx > 0 {
                    if idx.is_multiple_of(2) {
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
    use sha2::Digest;
    use tracing::trace;

    use super::*;
    use crate::{hash::sha_256, logs::init_logging};

    #[test]
    fn test_empty_tree() {
        init_logging();
        let data = vec![];
        let tree = XaeroMerkleTree::neo(data);
        trace!("Tree initialized with leaves: {:#?}", tree.nodes);
        trace!("Root hash: {:#?}", hex::encode(tree.root_hash));
        assert_eq!(tree.nodes.len(), 0);
        assert_eq!(tree.root_hash, [0; 32]);
    }

    #[test]
    fn test_single_node_tree() {
        init_logging();
        let data = vec![sha_256::<String>(&String::from("XAERO"))];
        let tree = XaeroMerkleTree::neo(data);
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
        let tree = XaeroMerkleTree::neo(data);
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
        let mut tree = XaeroMerkleTree::neo(data);
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
        let xaer3_tree = XaeroMerkleTree::neo(data);
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
        let mut tree = XaeroMerkleTree::neo(data);
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
        let xaer3_tree = XaeroMerkleTree::neo(data);
        trace!("XAER3: Tree initialized with leaves: {:#?}", tree.nodes);
        assert!(!xaer3_tree.verify_proof(
            proof.expect("proof was invalid"),
            sha_256::<String>(&String::from("XAER3"))
        ));
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
        let mut tree = XaeroMerkleTree::neo(data);
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
        let mut tree = XaeroMerkleTree::neo(data);
        let proof = tree.generate_proof(sha_256(&String::from("XAER3")));
        trace!("XAER3 Proof: {:#?}", proof);
        assert!(proof.is_none());
    }

    /// A minimal helper: compute a reference Merkle root of two 32-byte hashes
    fn ref_root(a: [u8; 32], b: [u8; 32]) -> [u8; 32] {
        sha_256_concat_hash(&a, &b)
    }

    #[test]
    fn insert_empty_page_returns_false() {
        let mut tree = XaeroMerkleTree::neo(vec![]);
        assert!(!tree.insert_page(vec![]));
        // still no leaves
        assert_eq!(tree.total_size, 0);
        assert_eq!(tree.leaf_start, 0);
        assert!(tree.nodes.is_empty());
    }

    #[test]
    fn single_leaf_page_gets_padded() {
        let mut tree = XaeroMerkleTree::neo(vec![]);
        let one = [0x11u8; 32];
        assert!(tree.insert_page(vec![one]));
        // should pad to 2 leaves => total_size = 2*2-1 = 3, leaf_start = 1
        assert_eq!(tree.total_size, 3);
        assert_eq!(tree.leaf_start, 1);
        // both leaves should equal `one`
        assert_eq!(tree.nodes[1].node_hash, one);
        assert_eq!(tree.nodes[2].node_hash, one);
        // root should be hash(one||one)
        let expected = ref_root(one, one);
        assert_eq!(tree.nodes[0].node_hash, expected);
    }

    //#[test]
    fn _append_page_to_existing_tree() {
        // start with two distinct leaves [A,B]
        let a = [0xAAu8; 32];
        let b = [0xBBu8; 32];
        let mut tree = XaeroMerkleTree::neo(vec![a, b]);
        tree.insert_page(vec![a, b]); // now 2 leaves
        let old_leaf_count = tree.total_size - tree.leaf_start; // = 2

        // append two new
        let c = [0xCCu8; 32];
        let d = [0xDDu8; 32];
        assert!(tree.insert_page(vec![c, d]));

        // new leaf count = 4, total_size = 2*4-1 = 7, leaf_start = 3
        assert_eq!(tree.total_size, 7);
        assert_eq!(tree.leaf_start, 3);
        let new_leaf_count = tree.total_size - tree.leaf_start;
        assert_eq!(new_leaf_count, old_leaf_count + 2);

        // check that the *old* leaves stayed where they were moved to:
        // at indices 3 and 4
        assert_eq!(tree.nodes[3].node_hash, a);
        assert_eq!(tree.nodes[4].node_hash, b);

        // new leaves at indices 5 and 6
        assert_eq!(tree.nodes[5].node_hash, c);
        assert_eq!(tree.nodes[6].node_hash, d);
    }

    //#[test]
    fn _root_hash_after_multiple_inserts() {
        // Build a 4-leaf tree: [A, B, C, D]
        let a = [0x01u8; 32];
        let b = [0x02u8; 32];
        let c = [0x03u8; 32];
        let d = [0x04u8; 32];

        let mut tree = XaeroMerkleTree::neo(vec![a, b, c, d]);
        tree.insert_page(vec![a, b]); // leaves [A,B]
        // root hash should be right now (hash(A||B))
        tree.insert_page(vec![c, d]); // leaves [C,D]
        // root hash should be hash(hash(A||B)||hash(C||D))

        let left = ref_root(a, b);
        let right = ref_root(c, d);
        let mut sha256 = sha2::Sha256::new();
        let expected_root = ref_root(left, right);
        sha256.update(expected_root.as_ref());
        let hash = sha256.finalize();
        let er: [u8; 32] = hash.as_slice().try_into().unwrap_or_default();
        assert_eq!(tree.nodes[0].node_hash, er);
    }
}
