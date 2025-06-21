//! IO utilities for reading and iterating archived events from segment files.
//!
//! This module provides:
//! - `read_segment_file`: memory-map a segment file for read-only access.
//! - `PageEventIterator`: iterate over archived event bytes within a single page.
//! - `iter_all_events`: helper to iterate across all pages in a segment.

use std::fs::OpenOptions;

use bytemuck::from_bytes;
use memmap2::Mmap;

use crate::indexing::storage::format::{EVENT_HEADER_SIZE, PAGE_SIZE, XAERO_MAGIC, XaeroOnDiskEventHeader};

/// Memory-map a segment file at the given path for read-only access.
///
/// Opens the file in read-only mode and returns a `Mmap` for zero-copy access
/// to its contents. Returns an `Err` if the file cannot be opened or mapped.
pub fn read_segment_file(path: &str) -> std::io::Result<Mmap> {
    let file = OpenOptions::new().read(true).open(path)?;
    // Safety: we never write through this map
    unsafe { Mmap::map(&file) }
}

/// Iterator over archived event bytes contained within a single fixed-size page.
///
/// `PageEventIterator` parses event headers and yields raw event bytes
/// that can be processed with `unarchive_to_xaero_event()` or `unarchive_to_raw_data()`.
pub struct PageEventIterator<'a> {
    page: &'a [u8],
    offset: usize,
}

impl<'a> PageEventIterator<'a> {
    /// Create a new iterator for a single page buffer.
    ///
    /// # Arguments
    ///
    /// * `page` - byte slice representing one fixed-size page from a segment.
    pub fn new(page: &'a [u8]) -> Self {
        Self { page, offset: 0 }
    }
}

impl<'a> Iterator for PageEventIterator<'a> {
    type Item = &'a [u8];

    /// Advance to the next archived event bytes in the page.
    ///
    /// Reads the event header, validates magic bytes, checks frame boundaries,
    /// and returns the complete event frame (header + data) if valid.
    /// Returns `None` when no more events are found.
    fn next(&mut self) -> Option<Self::Item> {
        // 1) Need room for header?
        if self.offset + EVENT_HEADER_SIZE > self.page.len() {
            return None;
        }

        // 2) Read header safely - copy to aligned buffer to avoid alignment issues
        let mut header_bytes = [0u8; EVENT_HEADER_SIZE];
        header_bytes.copy_from_slice(&self.page[self.offset..self.offset + EVENT_HEADER_SIZE]);
        let hdr: XaeroOnDiskEventHeader = *bytemuck::from_bytes(&header_bytes);

        // 3) Stop on padding or invalid magic
        if hdr.marker != XAERO_MAGIC {
            return None;
        }

        // 4) Calculate full frame length (header + data)
        let frame_len = EVENT_HEADER_SIZE + hdr.len as usize;
        if self.offset + frame_len > self.page.len() {
            // Invalid frame - extends beyond page boundary
            tracing::warn!("Event frame extends beyond page boundary: offset={}, frame_len={}, page_len={}",
                          self.offset, frame_len, self.page.len());
            return None;
        }

        // 5) Extract complete frame (header + data)
        let frame = &self.page[self.offset..self.offset + frame_len];

        // 6) Advance offset and yield frame
        self.offset += frame_len;
        Some(frame)
    }
}

/// Iterate all archived event bytes across every page in the mapped segment.
///
/// Splits the mapped memory into fixed-size pages and applies
/// `PageEventIterator` to each, flattening into a single event byte stream.
/// Each yielded `&[u8]` contains a complete event frame that can be processed
/// with `unarchive_to_xaero_event()` or `unarchive_to_raw_data()`.
pub fn iter_all_events(mmap: &Mmap) -> impl Iterator<Item = &'_ [u8]> + '_ {
    mmap.chunks_exact(PAGE_SIZE).flat_map(PageEventIterator::new)
}

/// Helper function to validate event frame integrity
/// Returns the header by value to avoid lifetime issues
pub fn validate_event_frame(frame: &[u8]) -> Result<XaeroOnDiskEventHeader, &'static str> {
    if frame.len() < EVENT_HEADER_SIZE {
        return Err("Frame too small for header");
    }

    // Copy header bytes to aligned buffer to avoid alignment issues
    let mut header_bytes = [0u8; EVENT_HEADER_SIZE];
    header_bytes.copy_from_slice(&frame[0..EVENT_HEADER_SIZE]);
    let header: XaeroOnDiskEventHeader = *bytemuck::from_bytes(&header_bytes);

    if header.marker != XAERO_MAGIC {
        return Err("Invalid magic number");
    }

    let expected_frame_len = EVENT_HEADER_SIZE + header.len as usize;
    if frame.len() != expected_frame_len {
        return Err("Frame length mismatch");
    }

    Ok(header)
}

/// Extract event data from a validated frame
pub fn extract_event_data(frame: &[u8]) -> Result<(XaeroOnDiskEventHeader, &[u8]), &'static str> {
    let header = validate_event_frame(frame)?;
    let data = &frame[EVENT_HEADER_SIZE..];
    Ok((header, data))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use memmap2::MmapMut;
    use tempfile::TempDir;
    use xaeroflux_core::{
        initialize,
        XaeroPoolManager,
        event::{EventType, XaeroEvent},
        date_time::emit_secs,
    };

    use super::*;
    use crate::indexing::storage::format::{archive_xaero_event, unarchive_to_raw_data, unarchive_to_xaero_event};

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
        file.set_len((n_pages * PAGE_SIZE) as u64).expect("failed_to_wrap");
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
        XaeroPoolManager::init();

        let (_tmp, path, mut mm) = make_segment_file(1);

        // Create XaeroEvent using new architecture
        let payload = vec![1, 2, 3];
        let xaero_event = XaeroPoolManager::create_xaero_event(
            &payload,
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            tracing::error!("Pool allocation failed: {:?}", pool_error);
            panic!("Cannot create test event - ring buffer pool exhausted");
        });

        // Archive using new format
        let frame = archive_xaero_event(&xaero_event);
        mm[0..frame.len()].copy_from_slice(&frame);
        mm.flush().expect("failed_to_wrap");

        // Read back and iterate
        let mmap = read_segment_file(path.to_str().expect("failed_to_wrap")).expect("failed_to_wrap");
        let page = &mmap[..PAGE_SIZE];
        let mut iter = PageEventIterator::new(page);

        // Get first event frame
        let event_frame = iter.next().expect("one event");

        // Unarchive using new format
        let (header, data) = unarchive_to_raw_data(event_frame);
        assert_eq!(data, &payload);
        assert_eq!(header.event_type, EventType::ApplicationEvent(1).to_u8());

        // Should be no more events
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_iter_all_events_multiple_pages() {
        initialize();
        XaeroPoolManager::init();

        let (_tmp, path, mut mm) = make_segment_file(2);

        // Create two XaeroEvents
        let e1 = XaeroPoolManager::create_xaero_event(
            &[4],
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            panic!("Cannot create test event 1: {:?}", pool_error);
        });

        let e2 = XaeroPoolManager::create_xaero_event(
            &[5, 6],
            EventType::ApplicationEvent(2).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            panic!("Cannot create test event 2: {:?}", pool_error);
        });

        // Archive both events
        let f1 = archive_xaero_event(&e1);
        let f2 = archive_xaero_event(&e2);

        // Place in different pages
        mm[0..f1.len()].copy_from_slice(&f1);
        mm[PAGE_SIZE..PAGE_SIZE + f2.len()].copy_from_slice(&f2);
        mm.flush().expect("failed_to_wrap");

        // Read back and iterate all events
        let mmap = read_segment_file(path.to_str().expect("failed_to_wrap")).expect("failed_to_wrap");
        let all_frames: Vec<&[u8]> = iter_all_events(&mmap).collect();

        assert_eq!(all_frames.len(), 2);

        // Verify first event
        let (_, data1) = unarchive_to_raw_data(all_frames[0]);
        assert_eq!(data1, &[4]);

        // Verify second event
        let (_, data2) = unarchive_to_raw_data(all_frames[1]);
        assert_eq!(data2, &[5, 6]);
    }

    #[test]
    fn test_iter_all_events_back_to_back_in_one_page() {
        initialize();
        XaeroPoolManager::init();

        let (_tmp, path, mut mm) = make_segment_file(1);

        // Create two XaeroEvents
        let e1 = XaeroPoolManager::create_xaero_event(
            &[7],
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            panic!("Cannot create test event 1: {:?}", pool_error);
        });

        let e2 = XaeroPoolManager::create_xaero_event(
            &[8, 9],
            EventType::ApplicationEvent(2).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            panic!("Cannot create test event 2: {:?}", pool_error);
        });

        // Archive both events
        let f1 = archive_xaero_event(&e1);
        let f2 = archive_xaero_event(&e2);

        // Place back-to-back in same page
        mm[0..f1.len()].copy_from_slice(&f1);
        mm[f1.len()..f1.len() + f2.len()].copy_from_slice(&f2);
        mm.flush().expect("failed_to_wrap");

        // Read back and iterate
        let mmap = read_segment_file(path.to_str().expect("failed_to_wrap")).expect("failed_to_wrap");
        let all_frames: Vec<&[u8]> = iter_all_events(&mmap).collect();

        assert_eq!(all_frames.len(), 2);

        // Verify both events
        let (_, data1) = unarchive_to_raw_data(all_frames[0]);
        assert_eq!(data1, &[7]);

        let (_, data2) = unarchive_to_raw_data(all_frames[1]);
        assert_eq!(data2, &[8, 9]);
    }

    #[test]
    fn test_validate_event_frame() {
        initialize();
        XaeroPoolManager::init();

        // Create a valid event
        let xaero_event = XaeroPoolManager::create_xaero_event(
            &[1, 2, 3],
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            panic!("Cannot create test event: {:?}", pool_error);
        });

        let frame = archive_xaero_event(&xaero_event);

        // Valid frame should pass validation
        let header = validate_event_frame(&frame).expect("valid frame should pass");
        assert_eq!(header.marker, XAERO_MAGIC);
        assert_eq!(header.len, 3); // payload length

        // Test extract_event_data
        let (header, data) = extract_event_data(&frame).expect("should extract data");
        assert_eq!(data, &[1, 2, 3]);
        assert_eq!(header.event_type, EventType::ApplicationEvent(1).to_u8());

        // Invalid frame (too short) should fail
        let short_frame = &frame[0..5]; // Less than EVENT_HEADER_SIZE
        assert!(validate_event_frame(short_frame).is_err());
    }

    #[test]
    fn test_roundtrip_with_new_format() {
        initialize();
        XaeroPoolManager::init();

        // Create original event
        let original_data = b"roundtrip test data";
        let original_event = XaeroPoolManager::create_xaero_event(
            original_data,
            EventType::ApplicationEvent(42).to_u8(),
            None, None, None,
            emit_secs(),
        ).unwrap_or_else(|pool_error| {
            panic!("Cannot create test event: {:?}", pool_error);
        });

        // Archive it
        let archived_frame = archive_xaero_event(&original_event);

        // Unarchive to new XaeroEvent
        let reconstructed_event = unarchive_to_xaero_event(&archived_frame)
            .expect("should reconstruct event");

        // Verify roundtrip integrity
        assert_eq!(reconstructed_event.data(), original_data);
        assert_eq!(reconstructed_event.event_type(), EventType::ApplicationEvent(42).to_u8());

        // Verify raw data access also works
        let (header, raw_data) = unarchive_to_raw_data(&archived_frame);
        assert_eq!(raw_data, original_data);
        assert_eq!(header.event_type, EventType::ApplicationEvent(42).to_u8());
    }
}