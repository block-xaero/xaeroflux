use std::fmt::{Debug, Formatter};

use futures_core::stream::BoxStream;
use futures_lite::stream::Boxed;
use iroh::{
    NodeId,
    discovery::{Discovery, DiscoveryError, DiscoveryItem, NodeData, pkarr::dht::DhtDiscovery},
};

pub struct XaeroDiscovery {
    pub dht: DhtDiscovery,
}

impl Debug for XaeroDiscovery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XaeroDiscovery").finish()
    }
}

impl Discovery for XaeroDiscovery {
    fn publish(&self, _data: &NodeData) {}

    fn subscribe(&self) -> Option<Boxed<DiscoveryItem>> {
        todo!()
    }
}
