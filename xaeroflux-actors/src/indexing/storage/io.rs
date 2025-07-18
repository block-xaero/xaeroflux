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
            tracing::warn!(
                "Event frame extends beyond page boundary: offset={}, frame_len={}, page_len={}",
                self.offset,
                frame_len,
                self.page.len()
            );
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
    mmap.chunks_exact(PAGE_SIZE as usize).flat_map(PageEventIterator::new)
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
