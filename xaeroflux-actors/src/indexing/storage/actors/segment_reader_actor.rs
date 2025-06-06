//! SegmentReaderActor: Replays persisted segments on-demand.
//!
//! This module defines:
//! - `SegmentReaderActor`: actor that listens for replay triggers, reads segment metadata from
//!   LMDB, loads segment files from disk, deserializes events, and forwards them to a consumer
//!   `Sink`.
//! - End-to-end replay logic for system-level replay events.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use rkyv::rancor::Failure;
use xaeroflux_core::{
    XAERO_DISPATCHER_POOL,
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{Event, SystemEventKind},
    listeners::EventListener,
    system_paths::{emit_control_path_with_subject_hash, emit_data_path_with_subject_hash},
};

use crate::{
    BusKind, Pipe, ScanWindow, XaeroEvent,
    aof::storage::{lmdb::LmdbEnv, meta::iterate_segment_meta_by_range},
    indexing::storage::{actors::segment_writer_actor::SegmentConfig, io},
    subject::SubjectHash,
};

pub static NAME_PREFIX: &str = "segment_reader";
/// Actor for replaying events from historical segments.
///
/// Fields:
/// - `inbox`: channel sender for incoming replay trigger events.
/// - `_listener`: listens for system replay events and feeds `inbox`.
/// - `meta_db`: LMDB environment containing segment metadata.
/// - `jh`: handle to the background thread processing replay requests.
pub struct SegmentReaderActor {
    pub name: SubjectHash,
    pub pipe: Arc<Pipe>,
    pub jh: std::thread::JoinHandle<()>,
}

impl SegmentReaderActor {
    /// Create a new `SegmentReaderActor`.
    ///
    /// Spawns a thread that:
    /// 1. Receives `SystemEvent::Replay` events on `inbox`.
    /// 2. Queries LMDB for segments within today's bounds.
    /// 3. For each segment:
    ///    - Builds the segment file path from `segment_dir`, timestamp, and index.
    ///    - Memory-maps the file and iterates pages to extract serialized events.
    ///    - Deserializes each event and wraps it into `XaeroEvent` with optional Merkle proof.
    ///    - Sends the event to the provided `sink`.
    ///
    /// # Arguments
    /// - `config`: directory and LMDB path settings for segment files and metadata.
    /// - `sink`: consumer of replayed events.
    ///
    /// # Panics    ///  if LMDB environment initialization fails.
    pub fn new(name: SubjectHash, pipe: Arc<Pipe>, config: SegmentConfig) -> Arc<Self> {
        let (tx, rx) = crossbeam::channel::bounded::<Event<Vec<u8>>>(0);
        let dir_path = match pipe.sink.kind {
            BusKind::Control =>
                emit_control_path_with_subject_hash(&config.segment_dir, name.0, NAME_PREFIX),
            BusKind::Data =>
                emit_data_path_with_subject_hash(&config.segment_dir, name.0, NAME_PREFIX),
        };
        // initialize LMDB environment from config
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(dir_path.as_str(), BusKind::Data).expect("failed to create LmdbEnv"),
        ));

        let segment_dir = dir_path;
        let pipe_clone = Arc::clone(&pipe);
        let pipe_rx = pipe_clone.source.rx.clone();
        let txc = tx.clone();
        let listener = EventListener::new(
            "segment_reader_actor_listener",
            Arc::new({
                move |e: Event<Vec<u8>>| {
                    let res = txc.send(e);
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("Failed to send event to inbox: {}", e);
                        }
                    }
                }
            }),
            None,
            Some(1), // FIXME: This should be a proper thread pool CONFIG
        );
        XAERO_DISPATCHER_POOL
            .get()
            .expect("xaero pool not initialized!")
            .execute(move || {
                while let Ok(xae) = pipe_rx.recv() {
                    let res = listener.inbox.send(xae.evt);
                    match res {
                        Ok(_) => {
                            tracing::info!("xae sent!")
                        }
                        Err(err_xae) => {
                            tracing::error!("xae failed due to {:?}", err_xae)
                        }
                    }
                }
            });
        let (start_of_day, end_of_day) = day_bounds_from_epoch_ms(emit_secs());
        let txc = tx.clone();
        let txc_sr_init = txc.clone();
        let mdbc = meta_db.clone();
        let jh = std::thread::spawn(move || {
            // ensure we run with the project root as our working directory,
            // so relative segment file paths resolve correctly in tests
            // env::set_current_dir(env!("CARGO_MANIFEST_DIR")).expect("failed to set cwd to project
            // root");
            while let Ok(e) = rx.recv() {
                tracing::info!("Received event: {:?}", e);
                // Process the event
                match e.event_type {
                    xaeroflux_core::event::EventType::SystemEvent(SystemEventKind::Replay) => {
                        let sw = bytemuck::try_from_bytes::<ScanWindow>(e.data.as_slice());
                        let window = match sw {
                            Ok(scan_window) => {
                                // Use the scan window to determine the range of segments to read
                                tracing::info!(
                                    "ScanWindow parsed successfully: {:?}, {:?}",
                                    scan_window.start,
                                    scan_window.end
                                );
                                let sod = scan_window.start;
                                (sod, None)
                            }
                            Err(e) => {
                                tracing::error!("Failed to parse ScanWindow: {}", e);
                                (start_of_day, Some(end_of_day))
                            }
                        };
                        let segment_meta_iter =
                            iterate_segment_meta_by_range(&mdbc, window.0, window.1);
                        let segment_metas = match segment_meta_iter {
                            Ok(r) => {
                                tracing::info!("Segment meta iterator created successfully.");
                                r
                            }
                            Err(e) => {
                                // something is messed up when lmdb is in-accessible.
                                panic!("Failed to create segment meta iterator: {}", e);
                            }
                        };
                        for segment_meta in segment_metas {
                            // Copy fields into locals to avoid unaligned references on packed
                            // struct
                            let ts_start = segment_meta.ts_start;
                            let seg_idx = segment_meta.segment_index;
                            tracing::info!("Processing segment meta: {:?}", segment_meta);
                            // Process the segment meta data
                            let file_path = Path::new(&segment_dir)
                                .join(format!("xaeroflux-actors-{}-{}.seg", ts_start, seg_idx));
                            let fr = io::read_segment_file(
                                file_path.to_str().expect("failed_to_unwrap"),
                            );
                            match fr {
                                Ok(mmap) => {
                                    tracing::info!("Segment file read successfully.");
                                    let page_iter = io::PageEventIterator::new(&mmap);
                                    for event in page_iter {
                                        tracing::info!("deserializing : {:?}", event);
                                        let er = rkyv::api::high::deserialize::<
                                            Event<Vec<u8>>,
                                            Failure,
                                        >(event);
                                        match er {
                                            Ok(e) => {
                                                tracing::info!(
                                                    "Deserialized event successfully - sending to \
                                                     Sink NOW!"
                                                );
                                                let xaero_event = XaeroEvent {
                                                    evt: e,
                                                    merkle_proof: None,
                                                };
                                                let res = tx.send(xaero_event.evt);
                                                match res {
                                                    Ok(_) => {
                                                        tracing::info!(
                                                            "Event sent to sink successfully."
                                                        );
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(
                                                            "Failed to send event to sink: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "Failed to deserialize event: {}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to read segment file: {}", e);
                                }
                            }
                            tracing::info!("Processing segment meta: {:?}", segment_meta);
                        }
                    }
                    _ => tracing::info!("not processing event"),
                }
            }
        });
        Arc::new(SegmentReaderActor { name, pipe, jh })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytemuck::bytes_of;
    use crossbeam::channel::Receiver;
    use iroh_blobs::store::bao_tree::blake3;
    use memmap2::MmapMut;
    use tempfile::tempdir;
    use xaeroflux_core::{
        date_time::emit_secs,
        event::{Event, EventType, SystemEventKind},
    };

    use crate::{
        BusKind, Pipe, XaeroEvent,
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, push_event},
        },
        core::initialize,
        indexing::storage::{
            actors::{
                segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentConfig,
            },
            format::{PAGE_SIZE, archive},
        },
        subject::SubjectHash,
    };

    /// Sending a Replay system event should cause SegmentReaderActor to read the segment file
    /// and re‐emit the "hello" application event on its pipe.
    #[test]
    fn system_event_triggers_replay() {
        initialize();
        // Create a temp directory for segment file and LMDB
        let tmp = tempdir().expect("failed to create tempdir");

        // Write a fake segment file in the temp directory
        let ts = emit_secs();
        let idx = 0;
        let seg_file = format!(
            "{}/xaeroflux-actors-{}-{}.seg",
            tmp.path().display(),
            ts,
            idx
        );
        {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .read(true)
                .open(&seg_file)
                .expect("failed to open segment file");
            file.set_len(PAGE_SIZE as u64)
                .expect("failed to set length");
            let mut mmap = unsafe { MmapMut::map_mut(&file).expect("failed to map") };
            let e = Event::new(b"hello".to_vec(), EventType::ApplicationEvent(1).to_u8());
            let frame = archive(&e);
            mmap[0..frame.len()].copy_from_slice(&frame);
            mmap.flush().expect("failed to flush");
        }

        // Insert corresponding SegmentMeta into LMDB under tmp directory
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(tmp.path().to_str().unwrap(), BusKind::Data)
                .expect("failed to create LMDB"),
        ));
        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: idx,
            ts_start: ts,
            ts_end: ts,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 0,
        };
        let meta_bytes = bytes_of(&seg_meta).to_vec();
        let meta_ev = Event::new(meta_bytes, EventType::MetaEvent(1).to_u8());
        push_event(&meta_db, &meta_ev).expect("failed to push segment_meta");

        // Build config for reader
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().to_string(),
            lmdb_env_path: tmp.path().to_string_lossy().to_string(),
        };

        // Compute SubjectHash as blake3("xaeroflux-actors")
        let mut hasher = blake3::Hasher::new();
        hasher.update("xaeroflux-actors".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        // Create a control pipe and subscribe to its rx for replayed events
        let pipe = Pipe::new(BusKind::Control, None);
        let rx_out: Receiver<XaeroEvent> = pipe.source.rx.clone();

        // Construct the actor with subject_hash and pipe
        let actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Send a Replay system event into actor.inbox
        let replay_evt = Event::new(
            Vec::new(),
            EventType::SystemEvent(SystemEventKind::Replay).to_u8(),
        );
        actor
            .pipe
            .sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
            })
            .unwrap();

        // Expect to receive the "hello" app event back on the pipe
        let got = rx_out
            .recv_timeout(Duration::from_secs(1))
            .expect("expected a replayed application event");
        assert_eq!(got.evt.data, b"hello".to_vec());
    }

    /// Sending a non‐system (application) event should *not* trigger replay.
    #[test]
    fn non_system_events_do_not_replay() {
        initialize();
        let tmp = tempdir().expect("failed to create tempdir");
        env::set_current_dir(tmp.path()).unwrap();

        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().to_string(),
            lmdb_env_path: tmp.path().to_string_lossy().to_string(),
        };

        // Compute SubjectHash
        let mut hasher = blake3::Hasher::new();
        hasher.update("xaeroflux-actors".as_bytes());
        let subject_hash = SubjectHash(*hasher.finalize().as_bytes());

        // Create control pipe and subscribe
        let pipe = Pipe::new(BusKind::Control, None);
        let rx_out: Receiver<XaeroEvent> = pipe.source.rx.clone();

        // Construct SegmentReaderActor
        let actor = SegmentReaderActor::new(subject_hash, pipe.clone(), config);

        // Send an application-level event into inbox (not a replay)
        let app_ev = Event::new(b"nope".to_vec(), EventType::ApplicationEvent(0).to_u8());
        actor
            .pipe
            .sink
            .tx
            .send(XaeroEvent {
                evt: app_ev,
                merkle_proof: None,
            })
            .unwrap();

        // There should be no event replayed
        assert!(rx_out.recv_timeout(Duration::from_millis(200)).is_err());
    }
}
