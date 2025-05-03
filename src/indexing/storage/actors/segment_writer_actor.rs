use std::{fs::OpenOptions, sync::Arc, thread};

use crossbeam::channel::{self, Receiver, Sender};
use memmap2::MmapMut;

use crate::{
    core::{event::Event, listeners::EventListener, size::PAGE_SIZE},
    indexing::storage::format::archive,
};

/// Configuration for paging and segmentation
#[derive(Clone)]
pub struct SegmentConfig {
    pub page_size: usize,
    pub pages_per_segment: usize,
    pub prefix: String,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        let page_size = *PAGE_SIZE.get().expect("PAGE_SIZE_NOT_SET");
        let pages_per_segment = crate::indexing::storage::format::PAGES_PER_SEGMENT;
        Self {
            page_size,
            pages_per_segment,
            prefix: "xaeroflux".into(),
        }
    }
}

/// Actor that writes archived events into paged, segment-backed files
pub struct SegmentWriterActor {
    /// Send archived event blobs to this inbox
    pub inbox: Sender<Vec<u8>>,
    _listener: EventListener<Vec<u8>>,
    _handle: thread::JoinHandle<()>,
}

impl Default for SegmentWriterActor {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentWriterActor {
    /// Create with default config (16KiB pages, 1024 pages per segment)
    pub fn new() -> Self {
        Self::new_with_config(SegmentConfig::default())
    }

    /// Create with custom config (for testability)
    pub fn new_with_config(config: SegmentConfig) -> Self {
        let (tx, rx) = channel::unbounded::<Vec<u8>>();
        let txc = tx.clone();
        let listener = EventListener::new(
            "segment_writer_actor",
            Arc::new(move |e: Event<Vec<u8>>| {
                let framed = archive(&e);
                txc.send(framed).expect("failed to send event");
            }),
            None,
            Some(1), // single-threaded handler
        );

        // spawn writer-loop with owned config and receiver
        let cfg = config.clone();
        let handle = thread::spawn(move || {
            run_writer_loop(rx, cfg);
        });

        SegmentWriterActor {
            inbox: tx,
            _listener: listener,
            _handle: handle,
        }
    }
}

/// Core writer loop separated for testability
fn run_writer_loop(rx: Receiver<Vec<u8>>, config: SegmentConfig) {
    let page_size = config.page_size;
    let segment_bytes = (config.pages_per_segment * page_size) as u64;

    // initial state
    let mut page_index = 0;
    let mut write_pos = 0;

    // calculate segment and page offsets
    let mut seg_id = page_index / config.pages_per_segment;
    let mut local_page_idx = page_index % config.pages_per_segment;
    let mut byte_offset = local_page_idx * page_size;

    // open first segment file
    let mut filename = format!("{}-{:04}.seg", config.prefix, seg_id);
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&filename)
        .expect("segment_file_not_found");
    file.set_len(segment_bytes).expect("failed to set length");
    let mut mm = unsafe { MmapMut::map_mut(&file).expect("mmap_failed") };

    // event processing loop
    while let Ok(data) = rx.recv() {
        let write_len = data.len();

        // page-boundary check
        if write_pos + write_len > page_size {
            // flush full page
            mm.flush_range(byte_offset, page_size)
                .expect("flush failed");

            // advance to next page
            page_index += 1;
            local_page_idx = page_index % config.pages_per_segment;

            // segment-boundary rollover
            if local_page_idx == 0 {
                seg_id = page_index / config.pages_per_segment;
                filename = format!("{}-{:04}.seg", config.prefix, seg_id);
                let f_new = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&filename)
                    .expect("segment_file_not_found");
                f_new.set_len(segment_bytes).expect("failed to set length");
                let new_mm = unsafe { MmapMut::map_mut(&f_new).expect("mmap_failed") };
                let old_mm = std::mem::replace(&mut mm, new_mm);
                drop(old_mm);
            }

            // reset for new page
            byte_offset = local_page_idx * page_size;
            write_pos = 0;
        }

        // copy data into current page
        let start = byte_offset + write_pos;
        let end = start + write_len;
        mm[start..end].copy_from_slice(&data);
        write_pos += write_len;
    }

    // final flush if any data
    if write_pos > 0 {
        mm.flush_range(byte_offset, page_size)
            .expect("final flush failed");
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Read};

    use crossbeam::channel;
    use tempfile::tempdir;

    use super::*;

    /// Pure page/segment math
    #[test]
    fn test_page_segment_math() {
        const PAGE_SIZE: usize = 16;
        const PAGES_PER_SEGMENT: usize = 4;
        let cases = [
            (0, 0, 0, 0),
            (1, 0, 1, 16),
            (3, 0, 3, 48),
            (4, 1, 0, 0),
            (5, 1, 1, 16),
            (7, 1, 3, 48),
            (8, 2, 0, 0),
        ];
        for (idx, seg, local, offset) in cases {
            assert_eq!(idx / PAGES_PER_SEGMENT, seg);
            assert_eq!(idx % PAGES_PER_SEGMENT, local);
            assert_eq!(local * PAGE_SIZE, offset);
        }
    }

    /// flush_range actually writes to disk
    #[test]
    fn test_flush_range_persists() {
        let dir = tempdir().expect("failed_to_unwrap_value");
        let file_path = dir.path().join("flush.bin");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("failed_to_unwrap_value");
        file.set_len(32).expect("failed_to_unwrap_value");
        let mut mm = unsafe { MmapMut::map_mut(&file).expect("failed_to_unwrap_value") };
        mm[4..8].copy_from_slice(&[1, 2, 3, 4]);
        mm.flush_range(4, 4).expect("failed_to_unwrap_value");
        drop(mm);
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).expect("failed_to_unwrap_value");
        assert_eq!(&buf[4..8], &[1, 2, 3, 4]);
    }

    /// test run_writer_loop page boundary behavior
    #[test]
    fn test_run_writer_loop_page_boundary() {
        let dir = tempdir().expect("failed_to_unwrap_value");
        let cfg = SegmentConfig {
            page_size: 4,
            pages_per_segment: 10,
            prefix: dir.path().display().to_string(),
        };
        let (tx, rx) = channel::unbounded::<Vec<u8>>();
        // send exactly 4-byte event
        tx.send(vec![9; 4]).expect("failed_to_unwrap_value");
        // send 1-byte event to trigger page flush
        tx.send(vec![7; 1]).expect("failed_to_unwrap_value");
        drop(tx);
        run_writer_loop(rx, cfg.clone());
        // read the file seg_id 0
        let fname = format!("{}-0000.seg", cfg.prefix);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&fname)
            .expect("failed_to_unwrap_value");
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).expect("failed_to_unwrap_value");
        assert_eq!(&buf[0..4], &[9, 9, 9, 9]);
        assert_eq!(buf[4], 7);
    }

    /// test run_writer_loop segment rollover
    #[test]
    fn test_run_writer_loop_segment_rollover() {
        let dir = tempdir().expect("failed_to_unwrap_value");
        let cfg = SegmentConfig {
            page_size: 2,
            pages_per_segment: 2,
            prefix: dir.path().display().to_string(),
        };
        let (tx, rx) = channel::unbounded::<Vec<u8>>();
        // will produce 4 events -> roll after 2 pages
        tx.send(vec![1; 2]).expect("failed_to_unwrap_value");
        tx.send(vec![2; 2]).expect("failed_to_unwrap_value");
        tx.send(vec![3; 2]).expect("failed_to_unwrap_value");
        tx.send(vec![4; 2]).expect("failed_to_unwrap_value");
        drop(tx);
        run_writer_loop(rx, cfg.clone());
        // seg 0 should have events 1 & 2 in pages 0 & 1
        let f0 = format!("{}-0000.seg", cfg.prefix);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&f0)
            .expect("failed_to_unwrap_value");
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).expect("failed_to_unwrap_value");
        assert_eq!(&buf[0..2], &[1, 1]);
        assert_eq!(&buf[2..4], &[2, 2]);
        // seg 1 should have events 3 & 4
        let f1 = format!("{}-0001.seg", cfg.prefix);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&f1)
            .expect("failed_to_unwrap_value");
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).expect("failed_to_unwrap_value");
        assert_eq!(&buf[0..2], &[3, 3]);
        assert_eq!(&buf[2..4], &[4, 4]);
    }
}
