///! This file is part of the `sys` module, which provides system-level functionality
/// //! for the event buffer library.
/// //! It includes functions to get the page size and the number of CPU cores.
use libc;
use memmap2::MmapMut;
pub const FILE_SIZE: usize = 1024 * 1024;

pub fn get_page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE).try_into().unwrap() }
}

/// MMapMut creates a memory-mapped file for reading and writing.
pub fn mm(f: &str) -> MmapMut {
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(f)
        .unwrap();
    unsafe {
        f.set_len(FILE_SIZE as u64)
            .expect("Failed to set file length");
        let mr = memmap2::MmapMut::map_mut(&f);
        match mr {
            Ok(mr) => mr,
            Err(e) => {
                panic!("Error mapping file: {}", e);
            }
        }
    }
}
