use bytemuck::{Pod, Zeroable};

#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct XaeroFileHeader {
    pub magic: [u8; 4],
    pub size: u64,
    pub crc32: u64,
}

unsafe impl Pod for XaeroFileHeader {}
unsafe impl Zeroable for XaeroFileHeader {}
