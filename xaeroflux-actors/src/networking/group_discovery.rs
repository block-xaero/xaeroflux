use futures::Stream;

use super::p2p::{Peer, XaeroP2PError};

pub type Group = [u8; 32];

pub trait GroupPeerDiscovery {
    type DiscoveryStream: Stream<Item = Result<Peer, XaeroP2PError>> + Send + 'static;
    fn discover_peers(&self, group: Group) -> Self::DiscoveryStream;
}
