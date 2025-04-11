use sha2::Digest;

use super::merkle_tree::XaeroMerkleNode;
use crate::core::XaeroData;

/// Sha256 hash function for any type of data.
/// # Generic Parameters
/// * `T` - The type of the data to be hashed.
/// # Arguments
/// * `n` - The data to be hashed.
/// # Returns
/// * A 32-byte array representing the SHA-256 hash of the input data.
pub fn sha_256<T>(n: &T) -> [u8; 32]
where
    T: XaeroData + AsRef<[u8]> + std::fmt::Debug,
{
    let mut sha256 = sha2::Sha256::new();
    sha256.update(n.as_ref());
    let hash = sha256.finalize();
    let node_hash: [u8; 32] = hash.as_slice().try_into().unwrap();
    node_hash
}

pub fn sha_256_concat(left: &XaeroMerkleNode, right: &XaeroMerkleNode) -> [u8; 32] {
    let mut combined: [u8; 64] = [0; 64];
    combined[0..32].copy_from_slice(&left.node_hash);
    combined[32..64].copy_from_slice(&right.node_hash);
    let mut sha256 = sha2::Sha256::new();
    sha256.update(combined);
    let hash = sha256.finalize();
    hash.as_slice().try_into().unwrap_or_default()
}
