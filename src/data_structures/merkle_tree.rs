use crate::data_structures::hash::sha_256;
use std::any::Any;

use super::hash::concat_sha_256;

/// Models a node in the Merkle tree.
/// # Generic Parameters
/// * `T` - The type of the data stored in the node.
#[derive(Clone, Debug)]
pub struct XaeroMerkleNode<T>
where
    T: Any + Send + Sync + AsRef<[u8]> + AsMut<[u8]> + std::fmt::Debug + Clone,
{
    pub node_hash: [u8; 32],
    pub node_data: Option<T>,
    pub is_left: bool,
}
impl<T> XaeroMerkleNode<T>
where
    T: Any + Send + Sync + AsRef<[u8]> + AsMut<[u8]> + std::fmt::Debug + Clone,
{
    pub fn new(node_data: Option<T>, is_left: bool) -> Self {
        let hash = match node_data {
            Some(ref data) => {
                let node_hash: [u8; 32] = sha_256(data);
                node_hash
            }
            None => [0; 32],
        };
        Self {
            node_hash: hash,
            node_data,
            is_left,
        }
    }
}
pub struct XaeroMerkleProofSegment {
    pub node_hash: [u8; 32],
    pub is_left: bool,
}

pub struct XaeroMerkleProof {
    pub value: Vec<XaeroMerkleProofSegment>,
}

pub struct XaeroMerkleTree<T>
where
    T: Any + Send + Sync + AsRef<[u8]> + AsMut<[u8]> + std::fmt::Debug + Clone,
{
    pub root_hash: [u8; 32],
    pub leaves: Vec<XaeroMerkleNode<T>>,
}

trait XaeroMerkleTreeOps<T>
where
    T: Any + Send + Sync + AsRef<[u8]> + AsMut<[u8]> + std::fmt::Debug + Clone,
{
    /// initialize Merkle tree leaves
    fn init(leaves: Vec<T>) -> XaeroMerkleTree<T>;
    /// merkle tree root hash
    fn root(&self) -> [u8; 32];
    /// prove that data is part of tree and return the path
    fn generate_proof(&self, data: &T) -> Option<XaeroMerkleProof>;
    /// verify that proof is valid
    fn verify_proof(&self, proof: XaeroMerkleProof, data: &T) -> bool;
}

impl<T> XaeroMerkleTreeOps<T> for XaeroMerkleTree<T>
where
    T: Any + Send + Sync + AsRef<[u8]> + AsMut<[u8]> + std::fmt::Debug + Clone,
{
    /// initialize Merkle tree leaves
    /// # Arguments
    /// * `leaves` - vector of leaves to be added to the tree
    /// # Returns
    /// * `XaeroMerkleTree<T>` - initialized Merkle tree with leaves
    /// # Example
    /// ```
    /// let leaves = vec![1, 2, 3, 4];
    /// let tree = XaeroMerkleTree::init(leaves);
    /// ```
    ///  Gotchas: Leaves are cloned to balance the tree if necessary.
    fn init(mut data_to_push: Vec<T>) -> XaeroMerkleTree<T> {
        if data_to_push.is_empty() {
            panic!("Cannot create a Merkle tree with no leaves");
        }
        let mut tree = XaeroMerkleTree {
            root_hash: [0; 32],
            leaves: Vec::<XaeroMerkleNode<T>>::new(),
        };
        if data_to_push.len() == 1 {
            let left = XaeroMerkleNode::new(data_to_push.pop(), true);
            tree.root_hash = left.node_hash;
            let mut right = left.clone();
            right.is_left = false;
            tree.leaves.push(left);
            tree.leaves.push(right);
            return tree;
        } else if data_to_push.len() == 2 {
            let left = XaeroMerkleNode::new(data_to_push.pop(), true);
            let right = XaeroMerkleNode::new(data_to_push.pop(), false);
            tree.root_hash = concat_sha_256(&left, &right);
            tree.leaves.push(left);
            tree.leaves.push(right);
            return tree;
        } else {
            while let Some(node) = data_to_push.pop() {
                let mut idx = tree.leaves.len();
                let len = idx;
                while idx > 0 {
                    let is_current_tree_even = idx % 2 == 0;
                    let n = &node;
                    if is_current_tree_even {
                        let left = XaeroMerkleNode::new(Some(n), true);
                        let mut right = left.clone();
                        right.is_left = false;
                        let concat_hash = concat_sha_256(&left, &right);
                        tree.leaves.push(left);
                        tree.leaves.push(right);
                        idx = (idx - 1) / 2;
                        tree.leaves[idx].node_hash = concat_hash;
                        tree.leaves[idx].is_left = false; // reset is_left for parent node
                        if idx == 0 {
                            tree.root_hash = tree.leaves[idx].node_hash;
                        }
                    } else {
                        let right_sibling_to_cur_idx = &XaeroMerkleNode::new(Some(n), false);
                        tree.leaves[idx - 1].is_left = true;
                        idx = (idx - 1) / 2;
                        tree.leaves[idx].node_hash =
                            concat_sha_256(&tree.leaves[idx - 1], right_sibling_to_cur_idx);
                        tree.leaves[idx].is_left = false; // reset is_left for parent node
                        if idx == 0 {
                            tree.root_hash = tree.leaves[idx].node_hash;
                        }
                    }
                }
            }
        }
        tree
    }

    fn root(&self) -> [u8; 32] {
        self.root_hash
    }

    fn generate_proof(&self, data: &T) -> Option<XaeroMerkleProof> {
        todo!()
    }

    fn verify_proof(&self, proof: XaeroMerkleProof, data: &T) -> bool {
        todo!()
    }
}
