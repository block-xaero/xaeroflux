//! Segment Writer Actor for xaeroflux-actors.
//!
//! This module provides:
//! - `SegmentConfig`: configuration parameters for paging and segmentation.
//! - `SegmentWriterActor`: an actor that listens for archived event blobs and writes them into
//!   fixed-size pages within segment files.
//! - `run_writer_loop`: core loop logic for handling page boundaries, file rollover, and metadata
//!   events.
//! - Unit tests verifying segment math, flush behavior, and rollover/resume logic.

use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};

use crossbeam::channel::{self, Receiver, Sender};
use memmap2::MmapMut;
use xaeroflux_core::{
    XAERO_DISPATCHER_POOL,
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{Event, EventType, SystemEventKind},
    hash::sha_256_hash_b,
    listeners::EventListener,
    size::PAGE_SIZE,
    system_paths::{emit_control_path_with_subject_hash, emit_data_path_with_subject_hash},
};

use crate::{
    BusKind, Pipe, XaeroEvent,
    aof::storage::{
        format::SegmentMeta,
        lmdb::{LmdbEnv, push_event},
        meta::iterate_segment_meta_by_range,
    },
    indexing::storage::format::archive,
    subject::SubjectHash,
    system_payload::SystemPayload,
};

pub static NAME_PREFIX: &str = "segment_writer";
/// Configuration for paged segment storage.
///
/// Fields:
/// - `page_size`: number of bytes per page within a segment.
/// - `pages_per_segment`: number of pages that make up a single segment file.
/// - `prefix`: filename prefix for segment files.
/// - `segment_dir`: directory path where segment files are stored.
/// - `lmdb_env_path`: directory path for the LMDB environment for metadata.
#[derive(Clone)]
pub struct SegmentConfig {
    pub page_size: usize,
    pub pages_per_segment: usize,
    pub prefix: String,
    pub segment_dir: String,
    pub lmdb_env_path: String,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        let page_size = *PAGE_SIZE.get().expect("PAGE_SIZE_NOT_SET");
        let pages_per_segment = crate::indexing::storage::format::PAGES_PER_SEGMENT;
        Self {
            page_size,
            pages_per_segment,
            prefix: "xf".into(),
            lmdb_env_path: "xf-aof".into(),
            segment_dir: "xf-segments".into(),
        }
    }
}

/// Actor responsible for writing serialized event frames into segment files.
///
/// This actor sets up:
/// - An `EventListener<Vec<u8>>` that archives incoming `Event<Vec<u8>>` values and sends their
///   byte frames to `inbox`.
/// - A background thread (`_handle`) running `run_writer_loop` to consume from `inbox`.
/// - An LMDB environment (`meta_db`) to record segment rollover metadata.
/// - The active `SegmentConfig` used for page size and directory settings.
///
/// Fields:
/// - `inbox`: channel sender for serialized event frames.
/// - `_listener`: keeps the event subscription alive.
/// - `meta_db`: metadata database for segment rollovers.
/// - `_handle`: handle to the spawned writer thread.
/// - `segment_config`: configuration parameters for this writer.
pub struct SegmentWriterActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    /// Send archived event blobs to this inbox
    pub meta_db: Arc<Mutex<LmdbEnv>>,
    pub _handle: thread::JoinHandle<()>,
    pub segment_config: SegmentConfig,
}

impl SegmentWriterActor {
    /// Create a `SegmentWriterActor` with default settings.
    ///
    /// Uses `SegmentConfig::default()` which sets 16KiB pages,
    /// 1024 pages per segment, and default directories.
    pub fn new(name: SubjectHash, pipe: Arc<Pipe>) -> Self {
        Self::new_with_config(name, pipe, SegmentConfig::default())
    }

    /// Create a `SegmentWriterActor` with a custom configuration.
    ///
    /// # Arguments
    /// - `config`: custom paging and directory parameters.
    ///
    /// Behavior:
    /// - Ensures the LMDB directory exists and initializes `LmdbEnv`.
    /// - Spawns an `EventListener` to serialize and forward events.
    /// - Starts a background thread running `run_writer_loop` to handle writing.
    pub fn new_with_config(name: SubjectHash, pipe: Arc<Pipe>, config: SegmentConfig) -> Self {
        std::fs::create_dir_all(&config.lmdb_env_path).expect("failed to create directory");
        let meta_db = match pipe.sink.kind {
            BusKind::Control => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_control_path_with_subject_hash(
                        config.lmdb_env_path.as_str(),
                        name.0,
                        NAME_PREFIX,
                    )
                    .as_str(),
                    BusKind::Data,
                )
                .expect("failed_to_create_lmdb_env"),
            )),
            BusKind::Data => Arc::new(Mutex::new(
                LmdbEnv::new(
                    emit_data_path_with_subject_hash(
                        config.lmdb_env_path.as_str(),
                        name.0,
                        NAME_PREFIX,
                    )
                    .as_str(),
                    BusKind::Data,
                )
                .expect("failed_to_create_lmdb_env"),
            )),
        };
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
        let txc = tx.clone();
        let pipe_clone0 = pipe.clone();
        XAERO_DISPATCHER_POOL
            .get()
            .expect("XAERO_DISPATCHER_POOL::get")
            .execute(move || {
                while let Ok(framed) = pipe_clone0.source.rx.recv() {
                    listener
                        .inbox
                        .send(framed.evt)
                        .expect("failed to send event");
                }
            });
        // spawn writer-loop with owned config and receiver
        let cfg = config.clone();
        let meta_db_c = meta_db.clone();
        let pipe_clone1 = pipe.clone();
        let handle: thread::JoinHandle<()> = thread::Builder::new()
            .name("xaeroflux-actors-segment-writer".into())
            .spawn(move || {
                run_writer_loop(pipe_clone1, &meta_db_c, rx, cfg);
            })
            .expect("failed to spawn thread");
        SegmentWriterActor {
            name,
            pipe,
            _handle: handle,
            meta_db,
            segment_config: config.clone(),
        }
    }
}

/// Core loop for writing archived event frames into page-aligned segment files.
///
/// This function:
/// 1. Initializes page and segment boundaries based on `config`.
/// 2. Iterates existing `SegmentMeta` entries to resume at the latest segment/page.
/// 3. Opens (or creates) the current segment file, memory-maps it.
/// 4. On each `rx.recv()`:
///    - If the incoming frame does not fit in the current page, flushes the page, advances
///      `page_index`, and handles segment rollover (including emitting a `MetaEvent` into LMDB and
///      remapping a new file).
///    - Copies the frame bytes into the current page at `byte_offset + write_pos`.
///    - Updates `write_pos`.
/// 5. On channel close, performs a final flush of any unwritten bytes.
///
/// # Arguments
/// - `meta_db`: LMDB environment for recording segment metadata.
/// - `rx`: receiver of serialized event frames (`Vec<u8>`).
/// - `config`: segment configuration including page size and directories.
fn run_writer_loop(
    pipe: Arc<Pipe>,
    meta_db: &Arc<Mutex<LmdbEnv>>,
    rx: Receiver<Vec<u8>>,
    config: SegmentConfig,
) {
    let page_size = config.page_size;
    let segment_bytes = (config.pages_per_segment * page_size) as u64;

    let (start_of_day, end_of_day) = day_bounds_from_epoch_ms(emit_secs());
    let segment_meta_iter = iterate_segment_meta_by_range(meta_db, start_of_day, Some(end_of_day))
        .expect("failed to iterate segment meta");
    let (mut page_index, mut write_pos, ts_start, mut seg_id) = segment_meta_iter
        .iter()
        .map(|seg_meta| {
            (
                seg_meta.page_index,
                seg_meta.write_pos,
                seg_meta.ts_start,
                seg_meta.segment_index,
            )
        })
        .max_by_key(|(_, _, ts, _)| *ts)
        .unwrap_or((0, 0, emit_secs(), 0));

    // initial state
    // let mut page_index = 0;
    // let mut write_pos = 0;

    // // calculate segment and page offsets
    // let mut seg_id = page_index / config.pages_per_segment;
    let mut local_page_idx = page_index % config.pages_per_segment;
    let mut byte_offset = local_page_idx * page_size;

    // open first segment file
    // ensure segment files live under the configured directory
    let mut filename: PathBuf = Path::new(&config.segment_dir)
        .join(format!("{}-{}-{:04}.seg", config.prefix, ts_start, seg_id));
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
        let leaf_hash = sha_256_hash_b(&data);
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
                let new_ts_start = emit_secs();
                filename = Path::new(&config.segment_dir).join(format!(
                    "{}-{}-{:04}.seg",
                    config.prefix, new_ts_start, seg_id
                ));
                let f_new = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&filename)
                    .expect("segment_file_not_found");
                f_new.set_len(segment_bytes).expect("failed to set length");
                let seg_meta = SegmentMeta {
                    page_index,
                    segment_index: seg_id,
                    write_pos,
                    byte_offset,
                    latest_segment_id: seg_id,
                    ts_start: new_ts_start,
                    ts_end: new_ts_start,
                };
                let data_b_segment_meta = bytemuck::bytes_of(&seg_meta);
                push_event(
                    meta_db,
                    &Event::new(
                        data_b_segment_meta.to_vec(),
                        EventType::MetaEvent(1).to_u8(),
                    ),
                )
                .expect("failed to push segment meta event");
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
        mm[start..end].copy_from_slice(data.as_slice());
        write_pos += write_len;
        tracing::debug!(
            "Wrote {} bytes to segment {} page {} at offset {}",
            write_len,
            seg_id,
            local_page_idx,
            start
        );
        tracing::debug!("sending message to control bus");
        let bytes_of_payload_written =
            bytemuck::bytes_of::<SystemPayload>(&SystemPayload::PayloadWritten {
                leaf_hash,
                meta: SegmentMeta {
                    page_index,
                    segment_index: seg_id,
                    write_pos,
                    byte_offset,
                    latest_segment_id: seg_id,
                    ts_start,
                    ts_end: emit_secs(),
                },
            })
            .to_vec();
        let payload_written_msg_sent_ack = pipe.sink.tx.send(XaeroEvent {
            evt: Event::new(
                bytes_of_payload_written,
                EventType::SystemEvent(SystemEventKind::PayloadWritten).to_u8(),
            ),
            merkle_proof: None,
        });
        match payload_written_msg_sent_ack {
            Ok(_) => tracing::debug!("Payload written message sent successfully"),
            Err(e) => {
                // FIXME: SHOULD WE PANIC HERE??
                tracing::error!("Failed to send PayloadWritten message: {}", e)
            }
        }
    }
    // final flush if any data
    if write_pos > 0 {
        mm.flush_range(byte_offset, page_size)
            .expect("final flush failed");
    }
}

/// Unit tests for the segment writer logic.
///
/// Tests include:
/// - Initial `SegmentMeta` storage and retrieval.
/// - Page/segment index math correctness.
/// - `flush_range` persisting data to disk.
/// - `run_writer_loop` handling page boundaries, rollover, and resume behavior.
#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Read, sync::Mutex};

    use crossbeam::channel;
    use tempfile::tempdir;
    use xaeroflux_core::event::EventType;

    use super::*;
    use crate::{
        aof::storage::meta::get_meta_val,
        core::initialize,
        indexing::storage::format::{self},
    };

    #[test]
    fn test_segment_meta_initial() {
        initialize();
        let dir = tempdir().expect("failed to unpack tempdir");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(
                dir.path().to_str().expect("failed_to_unwrap"),
                BusKind::Data,
            )
            .expect("failed_to_unwrap"),
        ));
        // fire a dummy segment_meta event
        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: 0,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 0,
            ts_start: emit_secs(),
            ts_end: emit_secs(),
        };
        let bytes = bytemuck::bytes_of(&seg_meta).to_vec();
        let ev = Event::new(bytes.clone(), EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &ev).expect("failed_to_unwrap");
        // raw SegmentMeta bytes are stored under the static key
        let raw = unsafe { get_meta_val(&arc_env, b"segment_meta") };
        // cast directly to SegmentMeta
        let got_meta: SegmentMeta = *bytemuck::from_bytes(&raw);
        let g_p_idx = got_meta.page_index;
        let g_s_idx = got_meta.segment_index;
        let g_w_pos = got_meta.write_pos;
        let g_b_off = got_meta.byte_offset;
        let g_l_s_id = got_meta.latest_segment_id;
        assert_eq!(g_p_idx, 0);
        assert_eq!(g_s_idx, 0);
        assert_eq!(g_w_pos, 0);
        assert_eq!(g_b_off, 0);
        assert_eq!(g_l_s_id, 0);
    }

    /// Pure page/segment math
    #[test]
    fn test_page_segment_math() {
        initialize();
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
        initialize();
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

    /// Test that page‐boundary flush & rollover work when you frame with HEADER+payload
    #[test]
    fn test_run_writer_loop_page_boundary() {
        initialize();
        let tmp = tempdir().expect("failed_to_unwrap");
        // switch to temp dir so segment files are isolated
        std::env::set_current_dir(tmp.path()).expect("failed to set cwd");
        // capture start timestamp for segment filename
        let ts_start = emit_secs();

        // Build
        // Create two events with different payload sizes:
        let ev1 = Event::new(vec![9; 4], EventType::ApplicationEvent(1).to_u8());
        let ev2 = Event::new(vec![7; 1], EventType::ApplicationEvent(1).to_u8());

        // Archive both so we know their exact on‐disk frame sizes:
        let framed1 = archive(&ev1);
        let framed2 = archive(&ev2);

        // page_size must hold the *entire* first frame
        let page_size = framed1.len().max(framed2.len());
        let cfg = SegmentConfig {
            page_size,
            pages_per_segment: 2,
            prefix: "test".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };

        // Set up arc_env (now that tmp and cwd are set)
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(
                tmp.path().to_str().expect("failed_to_unwrap"),
                BusKind::Control,
            )
            .expect("failed_to_unwrap"),
        ));

        // Send them
        let (tx, rx) = channel::unbounded::<Vec<u8>>();
        tx.send(framed1.clone()).expect("failed_to_unwrap");
        tx.send(framed2.clone()).expect("failed_to_unwrap");
        drop(tx);
        let control_pipe = Pipe::new(BusKind::Control, None);
        // Run the writer
        run_writer_loop(control_pipe, &arc_env, rx, cfg.clone());

        // Read back segment 0
        let buf0 = std::fs::read(format!(
            "{}/{}-{}-0000.seg",
            cfg.segment_dir, cfg.prefix, ts_start
        ))
        .expect("failed_to_unwrap");

        // Page 0: slice out the first `page_size` bytes
        {
            let slice = &buf0[0..cfg.page_size];
            let (_hdr, archived) = format::unarchive(slice);
            assert_eq!(archived.data.as_slice(), &[9; 4]);
        }

        // Page 1: slice the *next* `page_size` bytes
        {
            let start = cfg.page_size;
            let slice = &buf0[start..start + cfg.page_size];
            let (_hdr, archived) = format::unarchive(slice);
            assert_eq!(archived.data.as_slice(), &[7]);
        }
    }

    /// Test that after PAGES_PER_SEGMENT pages you roll into segment_0001.seg
    #[test]
    fn test_run_writer_loop_segment_rollover() {
        initialize();
        let tmp = tempdir().expect("failed_to_unwrap");
        std::env::set_current_dir(tmp.path()).expect("failed to set cwd");
        // capture start timestamp for segment filename
        let ts_start = emit_secs();

        // Build four events up front so we can see how big each framed message is:
        let payload_len = 2;
        let evs: Vec<_> = (1u8..=4)
            .map(|v| Event::new(vec![v; payload_len], EventType::ApplicationEvent(1).to_u8()))
            .collect();
        // Archive the *first* one to measure frame size (they'll all be roughly the same):
        let first_framed = archive(&evs[0]);
        let second_framed = archive(&evs[1]);
        let page_size = first_framed.len().max(second_framed.len()); // exact bytes needed
        let pages_per_segment = 2; // rollover after 2 pages

        let cfg = SegmentConfig {
            page_size,
            pages_per_segment,
            prefix: "test-rollover".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };

        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(
                tmp.path().to_str().expect("failed_to_unwrap"),
                BusKind::Control,
            )
            .expect("failed_to_unwrap"),
        ));

        // Send all four framed events into the writer:
        let (tx, rx) = channel::unbounded();
        for ev in &evs {
            let f = archive(ev);
            assert_eq!(f.len(), page_size, "expected uniform frame size");
            tx.send(f).expect("failed_to_unwrap");
        }
        drop(tx);
        let control_pipe = Pipe::new(BusKind::Control, None);
        run_writer_loop(control_pipe, &arc_env, rx, cfg.clone());

        // Closure to read & unarchive one page, closing over ts_start
        let assert_page = |seg: usize, page_idx: usize, expected_byte: u8| {
            let path = format!(
                "{}/{}-{}-{:04}.seg",
                cfg.segment_dir, cfg.prefix, ts_start, seg
            );
            tracing::debug!("######### reading segment file {}", path);
            let buf = std::fs::read(&path).expect("failed to read segment file");
            let start = page_idx * cfg.page_size;
            let slice = &buf[start..start + cfg.page_size];
            let (_hdr, archived) = format::unarchive(slice);
            assert!(
                archived.data.as_slice().iter().all(|&b| b == expected_byte),
                "segment {} page {} had {:?}, expected all {}",
                seg,
                page_idx,
                archived.data.as_slice(),
                expected_byte
            );
        };

        // Pages 0&1 → segment_0000; Pages 2&3 → segment_0001
        assert_page(0, 0, 1);
        assert_page(0, 1, 2);
        assert_page(1, 0, 3);
        assert_page(1, 1, 4);
    }

    /// Test that the writer resumes on the "hot" segment file named from the latest SegmentMeta
    #[test]
    fn test_resume_segment_meta_initialization() {
        use std::{sync::Arc, thread::sleep, time::Duration};

        use crate::{BusKind, Pipe, XaeroEvent};

        initialize();
        // Setup a temporary LMDB environment and insert a SegmentMeta
        let tmp = tempdir().expect("failed to create tempdir");
        std::env::set_current_dir(tmp.path()).expect("failed to set cwd");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(
                tmp.path().to_str().expect("failed_to_unwrap"),
                BusKind::Control,
            )
            .expect("failed to create LmdbEnv"),
        ));
        let now = emit_secs();
        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: 5,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 5,
            ts_start: now,
            ts_end: now,
        };
        let bytes = bytemuck::bytes_of(&seg_meta).to_vec();
        let meta_ev = Event::new(bytes, EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &meta_ev).expect("failed to push segment_meta event");

        // Prepare a single application event to write
        let app_ev = Event::new(vec![42; 4], EventType::ApplicationEvent(1).to_u8());
        let framed = format::archive(&app_ev);

        // Configure the writer to use a small page so the entire frame fits
        let cfg = SegmentConfig {
            page_size: framed.len(),
            pages_per_segment: 2,
            prefix: "test-resume".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };

        // Prepare pipe and actor
        let pipe = Pipe::new(BusKind::Data, None);
        let subject_hash = SubjectHash::from([0u8; 32]);
        let _actor = SegmentWriterActor::new(subject_hash, pipe.clone());

        // Send the framed event through the pipe as a XaeroEvent
        pipe.sink
            .tx
            .send(XaeroEvent {
                evt: Event::new(framed.clone(), EventType::ApplicationEvent(1).to_u8()),
                merkle_proof: None,
            })
            .expect("failed to send XaeroEvent");

        // Wait briefly for background thread to write the segment
        sleep(Duration::from_millis(100));

        // Verify that the hot segment file is named correctly
        let sm_sid = seg_meta.segment_index;
        let filename = format!(
            "{}/{}-{}-{:04}.seg",
            cfg.segment_dir, cfg.prefix, now, sm_sid
        );
        assert!(
            std::fs::metadata(&filename).is_ok(),
            "expected hot segment file {} to exist",
            filename
        );

        // Read back and confirm our event payload is written correctly on page 0
        let buf = std::fs::read(&filename).expect("failed to read segment file");
        let slice = &buf[0..cfg.page_size];
        let (_hdr, archived) = format::unarchive(slice);
        assert_eq!(
            archived.data.as_slice(),
            &[42; 4],
            "payload mismatch in resumed segment file"
        );
    }
}
