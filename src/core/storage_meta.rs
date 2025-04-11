use std::sync::Arc;

pub struct XaeroHeader {
    pub magic: [u8; 4],
    pub version: u32,
}

pub struct XaeroFooter {
    pub magic: [u8; 4],
    pub version: u32,
}
pub struct XaeroStorageMeta {
    pub header: XaeroHeader,
    pub footer: XaeroFooter,
}

pub trait HasXaeroStorageMeta {
    fn header(&self) -> Arc<XaeroHeader>;
    fn footer(&self) -> Arc<XaeroFooter>;
}
