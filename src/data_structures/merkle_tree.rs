pub struct MerkleNode<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    pub left: Option<Box<MerkleNode<T>>>,
    pub right: Option<Box<MerkleNode<T>>>,
    pub hash: [u8; 32],
    pub data: Option<T>,
}

trait MerkleTree<T>
where
    T: Any + Send + Sync + bincode::Decode<()> + bincode::Encode,
{
    fn new() -> Self;
    fn insert(&mut self, data: T);
    fn get_root_hash(&self) -> [u8; 32];
    fn get_node(&self, index: usize) -> Option<&MerkleNode<T>>;
    fn get_node_mut(&mut self, index: usize) -> Option<&mut MerkleNode<T>>;
    fn get_leaf(&self, index: usize) -> Option<&T>;
    fn get_leaf_mut(&mut self, index: usize) -> Option<&mut T>;
}

