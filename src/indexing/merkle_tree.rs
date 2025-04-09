use std::any::Any;

use crate::core::hash::sha_256;

use super::hash::sha_256_concat;

pub trait MerkleData:
    Any
    + Send
    + Sync
    + AsRef<[u8]>
    + AsMut<[u8]>
    + std::fmt::Debug
    + Clone
    + std::cmp::PartialEq
    + Default
{
}
/// Models a node in the Merkle tree.
/// # Generic Parameters
/// * `T` - The type of the data stored in the node.
#[derive(Clone, Debug)]
pub struct XaeroMerkleNode<T>
where
    T: MerkleData,
{
    pub node_hash: [u8; 32],
    pub node_data: Option<T>,
    pub is_left: bool,
}
impl<T> XaeroMerkleNode<T>
where
    T: MerkleData,
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
    T: MerkleData,
{
    pub root_hash: [u8; 32],
    pub leaves: Vec<XaeroMerkleNode<T>>,
}

impl<T> XaeroMerkleTree<T>
where
    T: MerkleData,
{
    fn _traverse_update_hash(&mut self, idx: usize)
    where
        T: Any + Send + Sync + AsRef<[u8]> + AsMut<[u8]> + std::fmt::Debug + Clone,
    {
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
            return Some(proof);
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
            return self._traverse_generate_proof(idx, proof);
        }
    }
}
trait XaeroMerkleTreeOps<T>
where
    T: MerkleData,
{
    /// initialize Merkle tree leaves
    fn init(leaves: Vec<T>) -> XaeroMerkleTree<T>;
    /// merkle tree root hash
    fn root(&self) -> [u8; 32];
    /// prove that data is part of tree and return the path
    fn generate_proof(&mut self, data: T) -> Option<XaeroMerkleProof>;
    /// verify that proof is valid
    fn verify_proof(&self, proof: XaeroMerkleProof, data: T) -> bool;
}

impl<T> XaeroMerkleTreeOps<T> for XaeroMerkleTree<T>
where
    T: MerkleData,
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

        while let Some(data) = data_to_push.pop() {
            if tree.leaves.len() % 2 == 0 {
                let left = XaeroMerkleNode::new(Some(data), true);
                let right = left.clone();
                tree.leaves.push(left);
                tree.leaves.push(right);
                tree._traverse_update_hash(tree.leaves.len() - 1);
            } else {
                let right = XaeroMerkleNode::new(Some(data), false);
                tree.leaves.push(right);
                tree._traverse_update_hash(tree.leaves.len() - 1);
            }
        }
        tree
    }

    fn root(&self) -> [u8; 32] {
        self.root_hash
    }

    fn generate_proof(&mut self, data: T) -> Option<XaeroMerkleProof> {
        let mut proof = XaeroMerkleProof {
            value: Vec::<XaeroMerkleProofSegment>::new(),
        };
        let mut idx = usize::MAX;
        for i in 0..self.leaves.len() {
            if self.leaves[i]
                .node_data
                .as_ref()
                .map_or(false, |node_data| node_data == &data)
            {
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

    fn verify_proof(&self, proof: XaeroMerkleProof, data: T) -> bool {
        // hash the data 
        // check if 
        let mut idx = 0;
        while idx < proof.value.len(){
            let proof_segment = &proof.value[idx];

            idx += 1;
        }
        true
    }
}
