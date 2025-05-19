use std::fs::OpenOptions;

use bytemuck::from_bytes;
use memmap2::Mmap;

use crate::{
    core::event::ArchivedEvent,
    indexing::storage::format::{
        EVENT_HEADER_SIZE, PAGE_SIZE, XAERO_MAGIC, XaeroOnDiskEventHeader, unarchive,
    },
};

/// Reads a segment file from disk and returns a read-only memory map.
pub fn read_segment_file(path: &str) -> std::io::Result<Mmap> {
    let file = OpenOptions::new().read(true).open(path)?;
    // Safety: we never write through this map
    unsafe { Mmap::map(&file) }
}

/// Iterator over archived events in a single fixed-size page.
pub struct PageEventIterator<'a> {
    page: &'a [u8],
    offset: usize,
}

impl<'a> PageEventIterator<'a> {
    /// Create a new iterator for one page.
    pub fn new(page: &'a [u8]) -> Self {
        Self { page, offset: 0 }
    }
}

impl<'a> Iterator for PageEventIterator<'a> {
    type Item = &'a ArchivedEvent<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        // 1) Need room for header?
        if self.offset + EVENT_HEADER_SIZE > self.page.len() {
            return None;
        }
        // 2) Read header
        let hdr: &XaeroOnDiskEventHeader =
            from_bytes(&self.page[self.offset..self.offset + EVENT_HEADER_SIZE]);
        // 3) Stop on padding
        if hdr.marker != XAERO_MAGIC {
            return None;
        }
        // 4) Full frame length
        let frame_len = EVENT_HEADER_SIZE + hdr.len as usize;
        if self.offset + frame_len > self.page.len() {
            return None;
        }
        // 5) Unarchive
        let frame = &self.page[self.offset..self.offset + frame_len];
        let (_hdr, archived) = unarchive(frame);
        // 6) Advance and yield
        self.offset += frame_len;
        Some(archived)
    }
}

/// Iterate all ArchivedEvent frames across every page in the segment.
pub fn iter_all_events(mmap: &Mmap) -> impl Iterator<Item = &'_ ArchivedEvent<Vec<u8>>> + '_ {
    mmap.chunks_exact(PAGE_SIZE)
        .flat_map(PageEventIterator::new)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use memmap2::MmapMut;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        core::{
            event::{Event, EventType},
            initialize,
        },
        indexing::storage::format,
    };

    /// Create a temp dir + segment file of exactly n_pages*PAGE_SIZE bytes,
    /// and return (TempDir, path, mutable mmap).
    fn make_segment_file(n_pages: usize) -> (TempDir, PathBuf, MmapMut) {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("test.seg");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("open file");
        file.set_len((n_pages * PAGE_SIZE) as u64)
            .expect("failed_to_wrap");
        let mm = unsafe { MmapMut::map_mut(&file).expect("failed_to_wrap") };
        (tmp, path, mm)
    }

    #[test]
    fn test_read_segment_file_not_found() {
        let err = read_segment_file("no-such.seg").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_page_event_iterator_single_event() {
        initialize();
        let (_tmp, path, mut mm) = make_segment_file(1);
        let payload = vec![1, 2, 3];
        let evt = Event::new(payload.clone(), EventType::ApplicationEvent(1).to_u8());
        let frame = format::archive(&evt);
        mm[0..frame.len()].copy_from_slice(&frame);
        mm.flush().expect("failed_to_wrap");

        let mmap =
            read_segment_file(path.to_str().expect("failed_to_wrap")).expect("failed_to_wrap");
        let page = &mmap[..PAGE_SIZE];
        let mut iter = PageEventIterator::new(page);

        let archived = iter.next().expect("one event");
        assert_eq!(archived.data.as_slice(), &payload);
        // assert_eq!(archived.event_type, EventType::ApplicationEvent(1).to_u8());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_iter_all_events_multiple_pages() {
        initialize();
        let (_tmp, path, mut mm) = make_segment_file(2);
        let e1 = Event::new(vec![4], EventType::ApplicationEvent(1).to_u8());
        let e2 = Event::new(vec![5, 6], EventType::ApplicationEvent(2).to_u8());
        let f1 = format::archive(&e1);
        let f2 = format::archive(&e2);
        mm[0..f1.len()].copy_from_slice(&f1);
        mm[PAGE_SIZE..PAGE_SIZE + f2.len()].copy_from_slice(&f2);
        mm.flush().expect("failed_to_wrap");

        let mmap =
            read_segment_file(path.to_str().expect("failed_to_wrap")).expect("failed_to_wrap");
        let all: Vec<&ArchivedEvent<Vec<u8>>> = iter_all_events(&mmap).collect();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].data.as_slice(), &[4]);
        assert_eq!(all[1].data.as_slice(), &[5, 6]);
    }

    #[test]
    fn test_iter_all_events_back_to_back_in_one_page() {
        initialize();
        let (_tmp, path, mut mm) = make_segment_file(1);
        let e1 = Event::new(vec![7], EventType::ApplicationEvent(1).to_u8());
        let e2 = Event::new(vec![8, 9], EventType::ApplicationEvent(2).to_u8());
        let f1 = format::archive(&e1);
        let f2 = format::archive(&e2);
        mm[0..f1.len()].copy_from_slice(&f1);
        mm[f1.len()..f1.len() + f2.len()].copy_from_slice(&f2);
        mm.flush().expect("failed_to_wrap");

        let mmap =
            read_segment_file(path.to_str().expect("failed_to_wrap")).expect("failed_to_wrap");
        let all: Vec<&ArchivedEvent<Vec<u8>>> = iter_all_events(&mmap).collect();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].data.as_slice(), &[7]);
        assert_eq!(all[1].data.as_slice(), &[8, 9]);
    }
}
