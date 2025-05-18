use std::sync::OnceLock;

use crate::sys;

pub static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
const PAGES_PER_SEGMENT: usize = 1_024;
pub static SEGMENT_SIZE: OnceLock<usize> = OnceLock::new();

/// Initialize the page size. This function should be called once at the start of the program.
/// It sets the `PAGE_SIZE` to the system's page size using `sys::get_page_size()`.
/// The page size is expected to be 16KB.
/// If `PAGE_SIZE` is already initialized, it will panic with an error message.
pub fn init_page_size() {
    PAGE_SIZE.get_or_init(sys::get_page_size);
}

pub fn init_segment_size() {
    // ensure PAGE_SIZE is set, then initialize SEGMENT_SIZE exactly once
    let page = *PAGE_SIZE.get_or_init(sys::get_page_size);
    SEGMENT_SIZE.get_or_init(|| page * PAGES_PER_SEGMENT);
}
//#[cfg(not(test))]
pub fn init() {
    init_page_size();
    init_segment_size();
}
