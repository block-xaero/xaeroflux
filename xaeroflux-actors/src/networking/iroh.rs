use std::sync::Arc;

use crossbeam::channel::Receiver;
use iroh::{Endpoint, PublicKey, SecretKey};
use iroh_gossip::{net::Gossip, proto::TopicId};
use xaeroflux_core::{keys::generate_ed25519_keypair, P2P_RUNTIME};

use super::{
    control_plane::ControlPlane,
    p2p::{
        NetworkPayload, XaeroControlPlane, XaeroPlaneKind, XaeroTopic,
        XaeroTopicGroup,
    },
};
use crate::aof::storage::format::SegmentMeta;
pub struct IrohControlPlane {
    pub topic_groups: XaeroTopicGroup,
    pub endpoint: Arc<Endpoint>,
    pub control_plane: Arc<ControlPlane>,
}
impl From<XaeroTopic> for TopicId {
    fn from(value: XaeroTopic) -> Self {
        TopicId::from_bytes(value.name)
    }
}
impl IrohControlPlane {
    pub fn new(control_plane: Arc<ControlPlane>, _bootstrap: Option<Vec<PublicKey>>) -> Self {
        let kp = generate_ed25519_keypair().expect("Failed to generate Ed25519 keypair");
        let runtime = P2P_RUNTIME.get().expect("p2p_runtime_not_initialized");
        let ep = runtime
            .block_on({
                Endpoint::builder()
                    .secret_key(SecretKey::from_bytes(&kp.secret_key))
                    .discovery_n0()
                    .discovery_local_network()
                    .discovery_dht()
                    .bind()
            })
            .expect("Failed to create Iroh endpoint");
        let epc = ep.clone();
        let _gossip = runtime
            .block_on(async { Gossip::builder().spawn(ep).await })
            .expect("Failed to spawn Iroh gossip");
        let _xtc = XaeroTopic::new("main", XaeroPlaneKind::ControlPlane);
        let xtg = XaeroTopicGroup::new("xaeroflux-actors-control", XaeroPlaneKind::ControlPlane);
        Self {
            topic_groups: xtg,
            endpoint: Arc::new(epc),
            control_plane,
        }
    }
}

impl XaeroControlPlane for IrohControlPlane {
    fn announce_peaks(
        &self,
        peaks: Vec<crate::indexing::storage::mmr::Peak>,
        root_hash: [u8; 32],
    ) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn publish_segment_rolled_over(
        &self,
        seg_meta: SegmentMeta,
    ) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn publish_segment_rolled_over_failed(
        &self,
        seg_meta: SegmentMeta,
        error_code: u16,
    ) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn subscribe_control(&self) -> Receiver<NetworkPayload> {
        todo!()
    }
}
