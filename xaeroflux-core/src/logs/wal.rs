use std::cmp::min;

use crate::sys::*;
pub struct Wal {
    pub mmap: memmap2::MmapMut,
    pub offset: u32,
    pub page_size: u32,
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.mmap.flush().expect("Failed to flush mmap");
    }
}
impl Wal {
    pub fn new(f: &str) -> Self {
        let mmap = mm(f);
        let page_size = get_page_size();
        Wal {
            mmap,
            offset: 0,
            page_size,
        }
    }
}

pub trait WalOps {
    fn write(&mut self, raw_buffer: &[u8]);
    fn read(&self) -> Vec<u8>;
    fn flush(&mut self);
    fn sync(&mut self);
    fn close(&mut self);
    fn truncate(&mut self);
}

impl WalOps for Wal {
    fn write(&mut self, raw_buffer: &[u8]) {
        let mut offset = 0;
        while offset < raw_buffer.len() {
            let remaining = raw_buffer.len() - offset;
            // advance only min of page size and remaining
            // this is to avoid writing more than page size
            let write_size = min(remaining, self.page_size as usize);
            self.mmap[self.offset as usize..self.offset as usize + write_size as usize]
                .copy_from_slice(&raw_buffer[offset..offset + write_size]);
            offset += write_size;
            self.offset += write_size as u32;
            if offset + write_size >= self.mmap.len() {
                offset = 0;
                self.mmap.flush().expect("Failed to flush mmap");
            }
        }
    }

    fn read(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.offset as usize);
        buffer.extend_from_slice(&self.mmap[..self.offset as usize]);
        buffer
    }

    fn flush(&mut self) {
        self.mmap.flush().expect("Failed to flush mmap");
    }

    fn sync(&mut self) {
        self.mmap.flush().expect("Failed to sync mmap");
    }

    fn close(&mut self) {
        self.mmap.flush().expect("Failed to truncate");
    }

    fn truncate(&mut self) {
        self.mmap.flush().expect("Failed to truncate");
        self.offset = 0;
    }
}
