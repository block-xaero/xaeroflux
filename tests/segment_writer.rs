// tests/segment_writer.rs

use std::{fs::OpenOptions, io::Read};

use memmap2::MmapMut;
use tempfile::tempdir;

const TEST_PAGE_SIZE: usize = 16;
const TEST_PAGES_PER_SEGMENT: usize = 4;

/// 1) Test the page/segment math in isolation
#[test]
fn test_page_segment_math() {
    // (page_index, expected_seg_id, expected_local_page, expected_offset)
    let cases = vec![
        (0, 0, 0, 0),
        (1, 0, 1, TEST_PAGE_SIZE),
        (3, 0, 3, TEST_PAGE_SIZE * 3),
        (4, 1, 0, 0),
        (5, 1, 1, TEST_PAGE_SIZE),
        (7, 1, 3, TEST_PAGE_SIZE * 3),
        (8, 2, 0, 0),
    ];

    for (page_idx, exp_seg, exp_local, exp_offset) in cases {
        let seg = page_idx / TEST_PAGES_PER_SEGMENT;
        let local = page_idx % TEST_PAGES_PER_SEGMENT;
        let offset = local * TEST_PAGE_SIZE;
        assert_eq!(seg, exp_seg, "seg mismatch for page {}", page_idx);
        assert_eq!(local, exp_local, "local mismatch for page {}", page_idx);
        assert_eq!(offset, exp_offset, "offset mismatch for page {}", page_idx);
    }
}

/// 2) Test that `flush_range` persists the written bytes to the file
#[test]
fn test_mmap_flush_range_persists() {
    let dir = tempdir().expect("failed_to_unwrap_value");
    let path = dir.path().join("flush_test.seg");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)
        .expect("failed_to_unwrap_value");

    // make file 2 pages long
    file.set_len((TEST_PAGE_SIZE * 2) as u64)
        .expect("failed_to_unwrap_value");

    // map it
    let mut mm = unsafe { MmapMut::map_mut(&file).expect("failed_to_unwrap_value") };

    // write 4 bytes at byte offset 8
    mm[8..12].copy_from_slice(&[10, 20, 30, 40]);
    // flush just that 4-byte range
    mm.flush_range(8, 4).expect("failed_to_unwrap_value");
    // drop the mapping so OS writes back fully
    drop(mm);

    // read back the file bytes
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).expect("failed_to_unwrap_value");
    assert_eq!(&buf[8..12], &[10, 20, 30, 40]);
}

/// 3) Integration‐style test for segment rollover
#[test]
fn test_segment_rollover() {
    let dir = tempdir().expect("failed_to_unwrap_value");
    let segment_bytes = (TEST_PAGE_SIZE * TEST_PAGES_PER_SEGMENT) as u64;

    // We'll write TEST_PAGES_PER_SEGMENT + 1 pages worth of 1 byte each,
    // forcing one rollover into the second segment.

    // Track where we wrote each byte
    let mut writes = Vec::new();

    // INITIAL SEGMENT 0
    let mut seg_id = 0;
    let mut page_index = 0;
    let mut local_page = page_index % TEST_PAGES_PER_SEGMENT;
    let mut filename = dir.path().join(format!("seg{:04}.bin", seg_id));
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&filename)
        .expect("failed_to_unwrap_value");
    f.set_len(segment_bytes).expect("failed_to_unwrap_value");
    let mut mm = unsafe { MmapMut::map_mut(&f).expect("failed_to_unwrap_value") };

    for i in 0..(TEST_PAGES_PER_SEGMENT + 1) {
        // one byte per page
        let byte = (i as u8) + 1;
        let start = local_page * TEST_PAGE_SIZE;
        mm[start] = byte;
        writes.push((seg_id, local_page, byte));

        // flush this page
        mm.flush_range(start, TEST_PAGE_SIZE)
            .expect("failed_to_unwrap_value");

        // prepare next
        page_index += 1;
        local_page = page_index % TEST_PAGES_PER_SEGMENT;
        if local_page == 0 {
            // rollover to next segment
            seg_id = page_index / TEST_PAGES_PER_SEGMENT;
            // drop old mapping, open/map new
            drop(mm);
            filename = dir.path().join(format!("seg{:04}.bin", seg_id));
            let f2 = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&filename)
                .expect("failed_to_unwrap_value");
            f2.set_len(segment_bytes).expect("failed_to_unwrap_value");
            mm = unsafe { MmapMut::map_mut(&f2).expect("failed_to_unwrap_value") };
        }
    }

    // Drop final mapping
    drop(mm);

    // Now verify each write landed in the right file & offset
    for (seg, page, byte) in writes {
        let path = dir.path().join(format!("seg{:04}.bin", seg));
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("failed_to_unwrap_value");
        let mut buf = vec![0u8; (page * TEST_PAGE_SIZE) + 1];
        // Read up to the offset+1
        f.read_exact(&mut buf).expect("failed_to_unwrap_value");
        assert_eq!(
            buf[page * TEST_PAGE_SIZE],
            byte,
            "seg{} page{} had wrong byte",
            seg,
            page
        );
    }
}
