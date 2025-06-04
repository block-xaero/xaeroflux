use std::convert::Infallible;

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

#[repr(C)]
pub struct Ed25519Keypair {
    pub secret_key: [u8; 32],
    pub public_key: [u8; 32],
}
impl Ed25519Keypair {
    pub fn new(secret_key: [u8; 32], public_key: [u8; 32]) -> Self {
        Ed25519Keypair {
            secret_key,
            public_key,
        }
    }
}
/// Generates a new Ed25519 keypair for signing.
pub fn generate_ed25519_keypair() -> Result<Ed25519Keypair, Infallible> {
    let mut csprng = OsRng;
    let sk = SigningKey::generate(&mut csprng);
    let secret_key = sk.to_bytes();
    let public_key = sk.verifying_key();
    Ed25519Keypair::new(secret_key, public_key.to_bytes()).try_into()
}
