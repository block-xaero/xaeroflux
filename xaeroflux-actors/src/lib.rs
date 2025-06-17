extern crate core;

pub mod aof;
pub mod indexing;
pub mod materializer;
pub mod networking;
pub mod pipe;

mod pipeline_parser;
pub mod subject;
mod system_actors;
mod system_payload;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use bytemuck::{Pod, Zeroable};
use xaeroflux_core::event::Event;

use crate::{
    pipe::{BusKind, Pipe},
    subject::Subscription,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a unique, thread-safe `u64` ID.
fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}
use xaeroflux_core::event::{ScanWindow, XaeroEvent};
/// Holds your system actors and subscription to keep them alive.
pub struct XFluxHandle {
    /// Composite subscription for core events.
    pub _sys_sub_data: Arc<Subscription>,
    pub _sys_sub_control: Arc<Subscription>,
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use bytemuck::bytes_of;
    use crossbeam::channel::unbounded;
    use iroh_blobs::store::bao_tree::blake3;
    use memmap2::MmapMut;
    use tempfile::tempdir;
    use xaeroflux_core::{
        date_time::emit_secs,
        event::{Event, EventType, SystemEventKind},
        init_xaero_pool, initialize, shutdown_all_pools,
        system_paths::emit_data_path_with_subject_hash,
    };
    use xaeroflux_macros::subject;

    use super::*;
    use crate::{
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, push_event},
        },
        indexing::storage::{
            actors::{segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentConfig},
            format::{PAGE_SIZE, archive},
        },
        subject::SubjectHash,
    };

    #[test]
    fn test_subject_macro() {
        initialize();
        use xaeroflux_core::event::{EventType, SystemEventKind};
        let expected_sha256: [u8; 32] = xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let subject = subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");

        // FIX: If macro sends to sink.tx, then we should receive from sink.rx
        let workspace_created_event = subject
            .control
            .sink  // Changed from source to sink
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed")
            .evt;

        assert_eq!(
            workspace_created_event.event_type,
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated)
        );
        assert_eq!(expected_sha256.as_ref(), subject.workspace_id.as_slice());
    }
    #[test]
    fn test_segment_reader_replay_then_live() {
        initialize();
        init_xaero_pool();
        let tmp = tempdir().expect("failed to create tempdir");

        let subject_hash = SubjectHash([0; 32]);
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };

        // Create the correct directory structure that the actor expects
        let data_path = emit_data_path_with_subject_hash(&config.segment_dir, subject_hash.0, "segment_reader");
        std::fs::create_dir_all(&data_path).expect("create dir");

        let lmdb_path = emit_data_path_with_subject_hash(&config.lmdb_env_path, subject_hash.0, "segment_reader");
        std::fs::create_dir_all(&lmdb_path).expect("create lmdb dir");

        // Write segment file in the correct location
        let ts = emit_secs();
        let idx = 0;
        let seg_path = Path::new(&data_path).join(format!("{}-{}-{:04}.seg", config.prefix, ts, idx));

        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .read(true)
                .open(&seg_path)
                .expect("open seg file");
            file.set_len(PAGE_SIZE as u64).expect("set_len");
            let mut mmap = unsafe { MmapMut::map_mut(&file).expect("mmap") };
            let e = Event::new(b"hello".to_vec(), EventType::ApplicationEvent(1).to_u8());
            let frame = archive(&e);
            mmap[..frame.len()].copy_from_slice(&frame);
            mmap.flush().expect("flush");
        }

        // Create LMDB environment in the correct location
        let meta_env = Arc::new(Mutex::new(
            LmdbEnv::new(&lmdb_path, BusKind::Data).expect("create lmdb"),
        ));

        // Push metadata
        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: idx,
            ts_start: ts,
            ts_end: ts,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 0,
        };
        let ev = Event::new(bytes_of(&seg_meta).to_vec(), EventType::MetaEvent(1).to_u8());
        push_event(&meta_env, &ev).expect("push_event");

        // Create pipe and get receiver
        let pipe = Pipe::new(BusKind::Data, None);
        let rx = pipe.source.rx.clone();

        // Create actor
        let actor = SegmentReaderActor::new(subject_hash, pipe, config);

        // Send replay event with proper scan window
        let scan_window = ScanWindow {
            start: ts - 1000, // Start a bit before our timestamp
            end: ts + 1000,   // End a bit after
        };
        let replay_evt = Event::new(
            bytes_of(&scan_window).to_vec(), // Include scan window data
            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
        );

        actor
            .pipe
            .sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
                author_id: None,
                latest_ts: None,
            })
            .expect("send replay");

        // Give actor time to process
        std::thread::sleep(Duration::from_millis(200));

        // Receive and assert
        let first = rx.recv_timeout(Duration::from_secs(2)).expect("recv first");
        assert_eq!(first.evt.data, b"hello".to_vec());

        drop(actor);
        let res = shutdown_all_pools();
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Failed to shutdown: {:?}", e);
            }
        }
    }
    #[test]
    fn test_subject_macro_object_event_and_hash() {
        initialize();
        // Compute expected SHA-256 for workspace and object
        let expected_ws_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let expected_obj_sha: [u8; 32] =
            xaeroflux_core::hash::sha_256_hash("cyan_object_white_board_id_134".as_bytes().to_vec());

        // Now compute the combined Blake3 hash exactly as the macro does:
        let mut hasher_ws = blake3::Hasher::new();
        hasher_ws.update("cyan_workspace_123".as_bytes());
        let ws_blake = hasher_ws.finalize();
        let mut hasher_obj = blake3::Hasher::new();
        hasher_obj.update("cyan_object_white_board_id_134".as_bytes());
        let obj_blake = hasher_obj.finalize();
        let mut combined_hasher = blake3::Hasher::new();
        combined_hasher.update(ws_blake.as_bytes());
        combined_hasher.update(obj_blake.as_bytes());
        let expected_combined: [u8; 32] = *combined_hasher.finalize().as_bytes();

        // Invoke the macro
        let subject = subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");

        // 1) First event was WorkspaceCreated; drain it
        let ws_evt = subject.control.sink.rx.recv().expect("attempt_to_unwrap_failed").evt;
        assert_eq!(
            ws_evt.event_type,
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated)
        );
        // Its payload should be the Blake3 of workspace (not SHA-256)
        let mut hasher_ws_payload = blake3::Hasher::new();
        hasher_ws_payload.update("cyan_workspace_123".as_bytes());
        let ws_blake_payload = hasher_ws_payload.finalize();
        assert_eq!(ws_evt.data, ws_blake_payload.as_bytes().to_vec());

        // 2) Second event should be ObjectCreated
        let obj_evt = subject.control.sink.rx.recv().expect("attempt_to_unwrap_failed").evt;
        assert_eq!(
            obj_evt.event_type,
            EventType::SystemEvent(SystemEventKind::ObjectCreated)
        );
        // Its payload should be the Blake3 of object (not SHA-256)
        let mut hasher_obj_payload = blake3::Hasher::new();
        hasher_obj_payload.update("cyan_object_white_board_id_134".as_bytes());
        let obj_blake_payload = hasher_obj_payload.finalize();
        assert_eq!(obj_evt.data, obj_blake_payload.as_bytes().to_vec());

        // 3) Check that the Subject struct fields match expectations:
        //    - workspace_id field holds the SHA-256
        assert_eq!(subject.workspace_id.as_slice(), expected_ws_sha.as_ref());
        //    - object_id field holds the SHA-256
        assert_eq!(subject.object_id.as_slice(), expected_obj_sha.as_ref());
        //    - hash field (SubjectHash) is the combined Blake3
        assert_eq!(subject.hash.0, expected_combined);
    }

    #[test]
    fn test_subject_macro_name_and_topic_key_flags() {
        initialize();
        // Use a different workspace/object pair
        let workspace = "MyWS";
        let object = "MyObj";

        // Compute SHA-256 for both
        let ws_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash(workspace.as_bytes().to_vec());
        let obj_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash(object.as_bytes().to_vec());
        // Combine via Blake3 as macro does
        let ws_blake = {
            let mut h = blake3::Hasher::new();
            h.update(workspace.as_bytes());
            h.finalize()
        };
        let obj_blake = {
            let mut h = blake3::Hasher::new();
            h.update(object.as_bytes());
            h.finalize()
        };
        let combined_blake = {
            let mut h = blake3::Hasher::new();
            h.update(ws_blake.as_bytes());
            h.update(obj_blake.as_bytes());
            *h.finalize().as_bytes()
        };

        let subject = subject!("workspace/MyWS/object/MyObj");
        // Name field should equal the literal used
        assert_eq!(subject.name, "workspace/MyWS/object/MyObj");
        // workspace_id and object_id bytes checked above in previous test
        assert_eq!(subject.workspace_id.as_slice(), ws_sha.as_ref());
        assert_eq!(subject.object_id.as_slice(), obj_sha.as_ref());
        // hash field must match combined Blake3
        assert_eq!(subject.hash.0, combined_blake);
    }
}
