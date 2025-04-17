use super::format::{XaeroMerkleOnDiskPage, XaeroMerklePage};

pub trait PageCodec {
    fn encode(&self, page: &XaeroMerklePage) -> anyhow::Result<XaeroMerkleOnDiskPage>;
    fn decode(&self, data: XaeroMerkleOnDiskPage) -> anyhow::Result<XaeroMerklePage>;
}
