//! Configuration of page and segment sizes for xaeroflux-actors.
//!
//! This module provides:
//! - `PAGE_SIZE`: the system's memory page size (bytes).
//! - `SEGMENT_SIZE`: total segment size as `PAGE_SIZE * PAGES_PER_SEGMENT`.
//! - Functions to initialize these values at program startup.

use std::sync::OnceLock;

use crate::sys;

/// Global, lazily-initialized system page size in bytes.
///
/// Uses `OnceLock` to ensure the page size is determined exactly once
/// via `sys::get_page_size()`.
pub static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
/// Number of pages that constitute a single segment.
///
/// A segment comprises this many pages for storage partitioning.
const PAGES_PER_SEGMENT: usize = 1_024;
/// Global, lazily-initialized segment size in bytes.
///
/// Calculated as `PAGE_SIZE * PAGES_PER_SEGMENT` and stored once.
pub static SEGMENT_SIZE: OnceLock<usize> = OnceLock::new();

/// Initialize the global `PAGE_SIZE`.
///
/// Calls `sys::get_page_size()` to determine the system's memory page size.
/// Panics if attempted to reinitialize after the first call.
pub fn init_page_size() {
    PAGE_SIZE.get_or_init(sys::get_page_size);
}

/// Initialize the global `SEGMENT_SIZE`.
///
/// Ensures `PAGE_SIZE` is set, then computes `SEGMENT_SIZE` as
/// `PAGE_SIZE * PAGES_PER_SEGMENT`. Panics if reinitialization is attempted.
pub fn init_segment_size() {
    // ensure PAGE_SIZE is set, then initialize SEGMENT_SIZE exactly once
    let page = *PAGE_SIZE.get_or_init(sys::get_page_size);
    SEGMENT_SIZE.get_or_init(|| page * PAGES_PER_SEGMENT);
}

/// Convenience method to initialize both page and segment sizes.
///
/// Should be called once at program startup to set up size constants.
pub fn init() {
    init_page_size();
    init_segment_size();
}
