use std::convert::TryInto;

use sha2::Digest;

use crate::pool::XaeroEventSized;

/// Primary hashing interface for XaeroEvent in the ring buffer architecture
pub fn hash_xaero_event<const TSHIRT_SIZE: usize>(
    xaero_event: XaeroEventSized<TSHIRT_SIZE>,
) -> [u8; 32] {
    sha_256_slice(bytemuck::bytes_of(&xaero_event))
}

/// Hash a byte slice - core implementation
pub fn sha_256_slice(data: &[u8]) -> [u8; 32] {
    let mut sha256 = sha2::Sha256::new();
    sha256.update(data);
    let hash = sha256.finalize();
    hash.as_slice().try_into().unwrap_or_default()
}

/// Hash with const size optimization for compile-time known sizes
/// This is optimized for ring buffer slots and fixed-size data
pub fn sha_256_const<const N: usize>(data: &[u8; N]) -> [u8; 32] {
    let mut sha256 = sha2::Sha256::new();
    sha256.update(data.as_ref());
    let hash = sha256.finalize();
    hash.as_slice().try_into().unwrap_or_default()
}

/// Hash with const size from slice with bounds checking
/// Returns None if slice length doesn't match const size
pub fn sha_256_const_checked<const N: usize>(data: &[u8]) -> Option<[u8; 32]> {
    if data.len() != N {
        return None;
    }
    let mut sha256 = sha2::Sha256::new();
    sha256.update(data);
    let hash = sha256.finalize();
    Some(hash.as_slice().try_into().unwrap_or_default())
}

/// Concatenated hash for Merkle tree operations
pub fn sha_256_concat_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut combined: [u8; 64] = [0; 64];
    combined[0..32].copy_from_slice(&left[..]);
    combined[32..64].copy_from_slice(&right[..]);
    sha_256_const(&combined)
}

/// Hash multiple byte arrays in sequence (for batch operations)
pub fn sha_256_multi_hash(data_arrays: &[&[u8]]) -> [u8; 32] {
    let mut sha256 = sha2::Sha256::new();
    for data in data_arrays {
        sha256.update(data);
    }
    let hash = sha256.finalize();
    hash.as_slice().try_into().unwrap_or_default()
}

/// BLAKE3 hash for strings (faster alternative for some use cases)
pub fn blake_hash(n: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(n.as_bytes());
    h.finalize().into()
}

/// BLAKE3 hash for byte slices
pub fn blake_hash_slice(data: &[u8]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(data);
    h.finalize().into()
}

/// BLAKE3 hash with const size optimization
pub fn blake_hash_const<const N: usize>(data: &[u8; N]) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(data.as_ref());
    h.finalize().into()
}

// Legacy compatibility functions - can be removed in future versions
#[deprecated(note = "Use sha_256_slice instead")]
pub fn sha_256_hash(n: Vec<u8>) -> [u8; 32] {
    sha_256_slice(&n)
}

#[allow(clippy::ptr_arg)]
#[deprecated(note = "Use sha_256_slice instead")]
pub fn sha_256_hash_b(n: &Vec<u8>) -> [u8; 32] {
    sha_256_slice(n)
}

#[deprecated(note = "Use sha_256_slice with n.as_ref() instead")]
pub fn sha_256<T>(n: &T) -> [u8; 32]
where
    T: AsRef<[u8]> + std::fmt::Debug,
{
    sha_256_slice(n.as_ref())
}

/// Hasher utility for stateful hashing operations
pub struct XaeroHasher<const TSHIRT_SIZE: usize> {
    hasher: blake3::Hasher,
}

impl<const TSHIRT_SIZE: usize> XaeroHasher<TSHIRT_SIZE> {
    /// Create a new hasher instance
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    /// Update hasher with XaeroEvent data
    pub fn update_event(&mut self, xaero_event: XaeroEventSized<TSHIRT_SIZE>) {
        self.hasher.update(bytemuck::bytes_of(&xaero_event));
    }

    /// Update hasher with byte slice
    pub fn update_slice(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    /// Update hasher with const-size data
    pub fn update_const<const N: usize>(&mut self, data: &[u8; N]) {
        self.hasher.update(data.as_ref());
    }

    /// Finalize and return hash
    pub fn finalize(self) -> [u8; 32] {
        let hash = self.hasher.finalize();
        hash.try_into().unwrap_or_default()
    }

    /// Reset hasher for reuse
    pub fn reset(&mut self) {
        self.hasher = blake3::Hasher::new();
    }
}

impl<const TSHIRT_SIZE: usize> Default for XaeroHasher<TSHIRT_SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha_256_slice() {
        let data = b"hello world";
        let hash = sha_256_slice(data);
        assert_eq!(hash.len(), 32);
        // Should be deterministic
        let hash2 = sha_256_slice(data);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_sha_256_const() {
        let data: [u8; 11] = *b"hello world";
        let hash1 = sha_256_const(&data);
        let hash2 = sha_256_slice(&data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_sha_256_const_checked() {
        let data = b"hello world";

        // Correct size
        let hash = sha_256_const_checked::<11>(data);
        assert!(hash.is_some());

        // Wrong size
        let hash = sha_256_const_checked::<10>(data);
        assert!(hash.is_none());
    }

    #[test]
    fn test_concat_hash() {
        let left = [1u8; 32];
        let right = [2u8; 32];
        let hash = sha_256_concat_hash(&left, &right);
        assert_eq!(hash.len(), 32);

        // Should be different from individual hashes
        let left_hash = sha_256_const(&left);
        let right_hash = sha_256_const(&right);
        assert_ne!(hash, left_hash);
        assert_ne!(hash, right_hash);
    }

    #[test]
    fn test_multi_hash() {
        let data1 = b"hello";
        let data2 = b"world";
        let multi_hash = sha_256_multi_hash(&[data1, data2]);

        // Should be same as concatenated data
        let mut combined = Vec::new();
        combined.extend_from_slice(data1);
        combined.extend_from_slice(data2);
        let combined_hash = sha_256_slice(&combined);

        assert_eq!(multi_hash, combined_hash);
    }

    #[test]
    fn test_blake_hash() {
        let data = "hello world";
        let hash = blake_hash(data);
        assert_eq!(hash.len(), 32);

        // Compare with slice version
        let hash2 = blake_hash_slice(data.as_bytes());
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_legacy_compatibility() {
        let data = vec![1, 2, 3, 4, 5];
        #[allow(deprecated)]
        let hash1 = sha_256_hash(data.clone());
        let hash2 = sha_256_slice(&data);
        assert_eq!(hash1, hash2);
    }
}
