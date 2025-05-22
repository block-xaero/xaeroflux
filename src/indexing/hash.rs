use std::convert::TryInto;

use sha2::Digest;

use super::merkle_tree::XaeroMerkleNode;
use crate::core::XaeroData;

///// Trait for hashing data in the Xaero system.
/// This trait defines a method for hashing data of type `T`
/// to produce a 32-byte hash. The `T` type must implement the `XaeroData` trait,
/// as well as `AsRef<[u8]>` and `std::fmt::Debug`.
///  # Generic Parameters
/// * `T` - The type of the data to be hashed.
pub trait XaeroHasher<T>
where
    T: XaeroData + AsRef<[u8]> + std::fmt::Debug,
{
    fn hash(&self, t: &T) -> [u8; 32];
}

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
    let node_hash: [u8; 32] = hash.as_slice().try_into().unwrap_or_default();
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

pub fn sha_256_concat_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut combined: [u8; 64] = [0; 64];
    combined[0..32].copy_from_slice(&left[..]);
    combined[32..64].copy_from_slice(&right[..]);
    let mut sha256 = sha2::Sha256::new();
    sha256.update(combined);
    let hash = sha256.finalize();
    hash.as_slice().try_into().unwrap_or_default()
}

pub fn sha_256_hash(n: Vec<u8>) -> [u8; 32] {
    let mut sha256 = sha2::Sha256::new();
    sha256.update(n);
    let hash = sha256.finalize();
    hash.as_slice().try_into().unwrap_or_default()
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_hash_by_non_zero_node() {}

    #[test]
    fn test_hash_by_zero_node() {}

    #[test]
    fn test_non_existent_node_hash() {}

    #[test]
    fn test_alternate_node_non_existent_concat() {}

    #[test]
    fn test_concatenation_hash() {}

    #[test]
    fn test_concat_with_zero_nodes() {}

    #[test]
    fn test_concat_hash_invalid_array() {}
}
