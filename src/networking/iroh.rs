use super::p2p::{PeaksHandler, SegmentMetaHandler, XaeroControlPlane};
use crate::core::aof::storage::format::SegmentMeta;

pub struct IrohControlPlane {
    // add fields as needed
}
impl XaeroControlPlane for IrohControlPlane {
    fn announce_peaks(
        &self,
        _peaks: Vec<crate::indexing::storage::mmr::Peak>,
    ) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn publish_segment_meta(
        &self,
        _seg_meta: SegmentMeta,
    ) -> Result<(), super::p2p::XaeroP2PError> {
        todo!()
    }

    fn on_peaks(&self, _handler: std::sync::Arc<PeaksHandler>) {
        todo!()
    }

    fn on_segment_meta(&self, _handler: std::sync::Arc<SegmentMetaHandler>) {
        todo!()
    }
}
