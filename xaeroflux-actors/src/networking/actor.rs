use iroh::Endpoint;
use crate::networking::p2p::{XaeroPeer, XaeroPeerState};

pub struct P2PActor{
    pub peer: XaeroPeer,
}

impl P2PActor {
    pub fn new() -> Self {
        Endpoint::builder()
            .discovery_local_network()
            .discovery_dht()
        P2PActor{
            peer: Arc::new(XaeroPeer::new()),
        }
    }
}