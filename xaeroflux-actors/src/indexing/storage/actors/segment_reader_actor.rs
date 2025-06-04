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
    date_time::{day_bounds_from_epoch_ms, emit_secs},
    event::{Event, SystemEventKind},
    listeners::EventListener,
};

use crate::{
    ScanWindow, Sink, XaeroEvent,
    aof::storage::{lmdb::LmdbEnv, meta::iterate_segment_meta_by_range},
    indexing::storage::{actors::segment_writer_actor::SegmentConfig, io},
    system::control_bus::ControlBus,
};

/// Actor for replaying events from historical segments.
///
/// Fields:
/// - `inbox`: channel sender for incoming replay trigger events.
/// - `_listener`: listens for system replay events and feeds `inbox`.
/// - `meta_db`: LMDB environment containing segment metadata.
/// - `jh`: handle to the background thread processing replay requests.
pub struct SegmentReaderActor {
    pub cb: Arc<ControlBus>,
    pub inbox: crossbeam::channel::Sender<Event<Vec<u8>>>,
    /// Only listens to Replay events and rejects all others.
    pub _listener: EventListener<Vec<u8>>,
    pub meta_db: Arc<Mutex<LmdbEnv>>,
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
    /// # Panics
    /// Panics if LMDB environment initialization fails.
    pub fn new(cb: Arc<ControlBus>, config: SegmentConfig, sink: Arc<Sink>) -> Arc<Self> {
        // initialize LMDB environment from config
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(&config.lmdb_env_path).expect("failed to create LmdbEnv"),
        ));
        let segment_dir = config.segment_dir.clone();

        let (start_of_day, end_of_day) = day_bounds_from_epoch_ms(emit_secs());
        let (tx, rx) = crossbeam::channel::bounded::<Event<Vec<u8>>>(0);
        let txc = tx.clone();
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
                                                let res = sink.tx.send(xaero_event);
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
        Arc::new(SegmentReaderActor {
            cb,
            _listener: EventListener::new(
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
            ),
            meta_db,
            inbox: tx,
            jh,
        })
    }
}

/// Unit tests for `SegmentReaderActor`.
///
/// Tests:
/// - `system_event_triggers_replay`: verifies that a `Replay` system event causes the actor to read
///   a prepared segment file and send the deserialized "hello" event to the sink.
/// - `non_system_events_do_not_replay`: ensures that non-system events are ignored.
#[cfg(test)]
mod tests {
    use std::{
        env,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use crossbeam::channel::unbounded;
    use memmap2::MmapMut;
    use tempfile::tempdir;
    use xaeroflux_core::{
        date_time::emit_secs,
        event::{Event, EventType, SystemEventKind},
    };

    use crate::{
        Sink, XaeroEvent,
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
    };

    /// If you send a Replay‐system‐event, you should get back exactly the one 'hello' from disk.
    #[test]
    fn system_event_triggers_replay() {
        // use a temp dir for segment file and LMDB
        let tmp = tempfile::tempdir().expect("failed_to_unwrap");
        let cb = Arc::new(crate::system::control_bus::ControlBus::new());
        initialize();

        // 2) write a fake segment file *in* project_root
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
                .expect("failed_to_unwrap");
            file.set_len(PAGE_SIZE as u64).expect("failed_to_unwrap");
            let mut mmap = unsafe { MmapMut::map_mut(&file).expect("failed_to_unwrap") };
            let e = Event::new(b"hello".to_vec(), EventType::ApplicationEvent(1).to_u8());
            let frame = archive(&e);
            mmap[0..frame.len()].copy_from_slice(&frame);
            mmap.flush().expect("failed_to_unwrap");
        }

        // 3) push the matching SegmentMeta into your LMDB (that still lives in tmpdir)
        let meta_db = Arc::new(Mutex::new(
            LmdbEnv::new(tmp.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap"),
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
        let ev = Event::new(
            bytemuck::bytes_of(&seg_meta).to_vec(),
            EventType::MetaEvent(1).to_u8(),
        );
        push_event(&meta_db, &ev).expect("failed_to_unwrap");

        // build config for reader
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().to_string(),
            lmdb_env_path: tmp.path().to_string_lossy().to_string(),
        };

        // 4) hook up the actor against the same project_root
        let (tx, rx) = unbounded::<XaeroEvent>();
        let sink = Arc::new(Sink::new(tx));
        let actor = SegmentReaderActor::new(cb, config, sink.clone());

        // 5) fire the Replay system event
        let replay_evt = Event::new(
            Vec::new(),
            EventType::SystemEvent(SystemEventKind::Replay).to_u8(),
        );
        actor
            ._listener
            .inbox
            .send(replay_evt)
            .expect("failed_to_unwrap");

        // 6) now it will find the file you wrote in project_root
        let got = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("should have seen our replayed application‐event");
        assert_eq!(got.evt.data, b"hello".to_vec());
    }

    /// An ApplicationEvent should *not* trigger any replay.
    #[test]
    fn non_system_events_do_not_replay() {
        initialize();
        let tmp = tempdir().expect("failed_to_unwrap");
        env::set_current_dir(tmp.path()).expect("failed_to_unwrap");

        // build config for reader
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().to_string(),
            lmdb_env_path: tmp.path().to_string_lossy().to_string(),
        };

        let lmdb =
            LmdbEnv::new(tmp.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap");
        let _meta_db = Arc::new(Mutex::new(lmdb));

        let (tx, rx) = unbounded::<XaeroEvent>();
        let sink = Arc::new(Sink::new(tx));
        let cb: Arc<crate::system::control_bus::ControlBus> =
            Arc::new(crate::system::control_bus::ControlBus::new());
        let actor = SegmentReaderActor::new(cb, config, sink.clone());

        // send an application‐level event
        let app = Event::new(b"nope".to_vec(), EventType::ApplicationEvent(0).to_u8());
        actor._listener.inbox.send(app).expect("failed_to_unwrap");

        // we should see *nothing*
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
    }
}
