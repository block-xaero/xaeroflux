use crate::indexing::storage::format::XaeroOnDiskPage;

pub trait XaeroMerklePageCache {
    fn read_page(&mut self, idx: usize) -> anyhow::Result<XaeroOnDiskPage>;
    fn write_page(&mut self, page: &XaeroOnDiskPage) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<()>;
}
