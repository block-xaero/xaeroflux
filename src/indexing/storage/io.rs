use super::format::XaeroMerklePage;

pub trait XaeroMerklePageCache {
    fn read_page(&mut self, idx: usize) -> anyhow::Result<XaeroMerklePage>;
    fn write_page(&mut self, page: &XaeroMerklePage) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<()>;
}
