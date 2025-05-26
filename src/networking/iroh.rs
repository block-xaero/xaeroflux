use super::p2p::XaeroControlPlane;

pub struct IrohControlPlane{
    // add fields as needed
}
impl XaeroControlPlane for IrohControlPlane {
    fn announce_peaks(&self, peaks: Vec<crate::indexing::storage::mmr::Peak>) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn publish_segment_meta(&self, seg_meta: crate::core::meta::SegmentMeta) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn on_peaks<F>(&self, handler: std::sync::Arc<dyn Fn(&super::p2p::Peer, Vec<crate::indexing::storage::mmr::Peak>) + Send + Sync + 'static>) {
        todo!()
    }

    fn on_segment_meta<F>(&self, handler: std::sync::Arc<dyn Fn(&super::p2p::Peer, crate::core::meta::SegmentMeta) + Send + Sync + 'static>) {
        todo!()
    }
}