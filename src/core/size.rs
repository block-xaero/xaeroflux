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
    PAGE_SIZE
        .set(sys::get_page_size()) // 16KB
        .expect("PAGE_SIZE already initialized");
}

pub fn init_segment_size() {
    SEGMENT_SIZE
        .set(PAGE_SIZE.get().expect("failed_to_get_page_size") * PAGES_PER_SEGMENT)
        .expect("SEGMENT_SIZE already initialized");
}
#[cfg(not(test))]
pub fn init() {
    init_page_size();
    init_segment_size();
}
