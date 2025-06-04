use anyhow::Error;
use iroh::{Endpoint, NodeAddr, SecretKey};
use xaeroflux_core::keys::Ed25519Keypair;

use super::p2p::XaeroP2PError;

pub fn setup_endpoint_using_seeds(
    bootstrap_nodes: Vec<NodeAddr>,
) -> Result<impl std::future::Future<Output = Result<Endpoint, Error>>, XaeroP2PError> {
    let key_pair: Ed25519Keypair = xaeroflux_core::keys::generate_ed25519_keypair()
        .expect("Failed to generate Ed25519 keypair");
    Ok(Endpoint::builder()
        .secret_key(SecretKey::from_bytes(&key_pair.secret_key))
        .relay_mode(iroh::RelayMode::Default)
        .discovery_local_network()
        .discovery_dht()
        .discovery_n0()
        .known_nodes(bootstrap_nodes)
        .bind())
}
