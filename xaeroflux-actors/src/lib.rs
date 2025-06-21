extern crate core;

pub mod aof;
pub mod indexing;
pub mod materializer;
pub mod networking;

mod pipeline_parser;
pub mod subject;
mod system_actors;
mod system_payload;

use std::sync::{Arc, OnceLock};

use bytemuck::{Pod, Zeroable};
use xaeroflux_core::{
    event::{ScanWindow, XaeroEvent},
    pipe::{BusKind, Pipe},
    XaeroPoolManager,  // Import from xaeroflux_core
};

use crate::subject::Subscription;

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
        event::{EventType, SystemEventKind},
        init_xaero_pool, initialize, shutdown_all_pools,
        system_paths::emit_data_path_with_subject_hash,
    };
    use xaeroflux_macros::subject;

    use super::*;
    use crate::{
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, push_xaero_event},
        },
        indexing::storage::{
            actors::{segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentConfig},
            format::{PAGE_SIZE, archive_xaero_event},
        },
        subject::SubjectHash,
    };

    #[test]
    fn test_subject_macro() {
        initialize();

        // Initialize ring buffer pools
        XaeroPoolManager::init();

        use xaeroflux_core::event::{EventType, SystemEventKind};
        let expected_sha256: [u8; 32] = xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let subject = subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");

        // Receive events from control pipe - events are Arc<XaeroEvent>
        let workspace_created_event = subject
            .control
            .sink
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed");

        // Access event type through Arc<XaeroEvent>
        assert_eq!(
            workspace_created_event.event_type(),
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated).to_u8()
        );
        assert_eq!(expected_sha256.as_ref(), subject.workspace_id.as_slice());
    }

    #[test]
    fn test_segment_reader_replay_then_live() {
        initialize();
        init_xaero_pool();

        // Initialize ring buffer pools
        XaeroPoolManager::init();

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

        // Write segment file in the correct location using new archive format
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

            // Create XaeroEvent using new architecture
            let test_event = XaeroPoolManager::create_xaero_event(
                b"hello",
                EventType::ApplicationEvent(1).to_u8(),
                None, None, None,
                emit_secs(),
            ).expect("Failed to create test event");

            // Use new archive format
            let frame = archive_xaero_event(&test_event);
            mmap[..frame.len()].copy_from_slice(&frame);
            mmap.flush().expect("flush");
        }

        // Create LMDB environment in the correct location
        let meta_env = Arc::new(Mutex::new(
            LmdbEnv::new(&lmdb_path, BusKind::Data).expect("create lmdb"),
        ));

        // Push metadata using new XaeroEvent format
        let seg_meta = SegmentMeta {
            page_index: 0,
            segment_index: idx,
            ts_start: ts,
            ts_end: ts,
            write_pos: 0,
            byte_offset: 0,
            latest_segment_id: 0,
        };

        let meta_event = XaeroPoolManager::create_xaero_event(
            bytes_of(&seg_meta),
            EventType::MetaEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).expect("Failed to create meta event");

        push_xaero_event(&meta_env, &meta_event).expect("push_xaero_event");

        // Create pipe and get receiver
        let pipe = Pipe::new(BusKind::Data, None);
        let rx = pipe.source.rx.clone();

        // Create actor
        let actor = SegmentReaderActor::new(subject_hash, pipe, config);

        // Send replay event with proper scan window - use XaeroPoolManager
        let scan_window = ScanWindow {
            start: ts - 1000, // Start a bit before our timestamp
            end: ts + 1000,   // End a bit after
        };

        let replay_evt = XaeroPoolManager::create_xaero_event(
            bytes_of(&scan_window), // Include scan window data
            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        ).expect("Failed to create replay event");

        actor
            .pipe
            .sink
            .tx
            .send(replay_evt)
            .expect("send replay");

        // Give actor time to process
        std::thread::sleep(Duration::from_millis(200));

        // Receive and assert - handle Arc<XaeroEvent>
        let first = rx.recv_timeout(Duration::from_secs(2)).expect("recv first");
        assert_eq!(first.data(), b"hello");

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

        // Initialize ring buffer pools
        XaeroPoolManager::init();

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

        // 1) First event was WorkspaceCreated; drain it - handle Arc<XaeroEvent>
        let ws_evt = subject.control.sink.rx.recv().expect("attempt_to_unwrap_failed");
        assert_eq!(
            ws_evt.event_type(),
            EventType::SystemEvent(SystemEventKind::WorkspaceCreated).to_u8()
        );
        // Its payload should be the Blake3 of workspace (not SHA-256)
        let mut hasher_ws_payload = blake3::Hasher::new();
        hasher_ws_payload.update("cyan_workspace_123".as_bytes());
        let ws_blake_payload = hasher_ws_payload.finalize();
        assert_eq!(ws_evt.data(), ws_blake_payload.as_bytes());

        // 2) Second event should be ObjectCreated - handle Arc<XaeroEvent>
        let obj_evt = subject.control.sink.rx.recv().expect("attempt_to_unwrap_failed");
        assert_eq!(
            obj_evt.event_type(),
            EventType::SystemEvent(SystemEventKind::ObjectCreated).to_u8()
        );
        // Its payload should be the Blake3 of object (not SHA-256)
        let mut hasher_obj_payload = blake3::Hasher::new();
        hasher_obj_payload.update("cyan_object_white_board_id_134".as_bytes());
        let obj_blake_payload = hasher_obj_payload.finalize();
        assert_eq!(obj_evt.data(), obj_blake_payload.as_bytes());

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

        // Initialize ring buffer pools
        XaeroPoolManager::init();

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

    #[test]
    fn test_zero_copy_event_access() {
        initialize();
        XaeroPoolManager::init();

        // Test that we can access event data through PooledEventPtr without copying
        let test_data = b"zero-copy test data for ring buffer";
        let event = XaeroPoolManager::create_xaero_event(
            test_data,
            EventType::ApplicationEvent(42).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        ).expect("Failed to create test event");

        // Access data through zero-copy interface
        let accessed_data = event.data(); // This should be zero-copy access
        assert_eq!(accessed_data, test_data);
        assert_eq!(event.event_type(), EventType::ApplicationEvent(42).to_u8());
    }

    #[test]
    fn test_event_sharing_across_threads() {
        initialize();
        XaeroPoolManager::init();

        let test_data = b"shared event data";
        let event = XaeroPoolManager::create_xaero_event(
            test_data,
            EventType::ApplicationEvent(1).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        ).expect("Failed to create shared event");

        let event_clone = event.clone();

        // Share event across thread boundary
        let handle = std::thread::spawn(move || {
            // Access data in different thread - should work due to Arc
            let data = event_clone.data();
            assert_eq!(data, test_data);
            data.len()
        });

        let result = handle.join().expect("Thread join failed");
        assert_eq!(result, test_data.len());

        // Original event should still be accessible
        assert_eq!(event.data(), test_data);
    }

    #[test]
    fn test_ring_buffer_pool_usage() {
        initialize();
        XaeroPoolManager::init();

        // Test that events are allocated from ring buffer pools
        let small_data = b"small";
        let medium_data = vec![42u8; 512];
        let large_data = vec![13u8; 2048];

        // Create events of different sizes to test pool allocation
        let small_event = XaeroPoolManager::create_xaero_event(
            small_data,
            EventType::ApplicationEvent(1).to_u8(),
            None, None, None,
            emit_secs(),
        ).expect("Failed to create small event");

        let medium_event = XaeroPoolManager::create_xaero_event(
            &medium_data,
            EventType::ApplicationEvent(2).to_u8(),
            None, None, None,
            emit_secs(),
        ).expect("Failed to create medium event");

        let large_event = XaeroPoolManager::create_xaero_event(
            &large_data,
            EventType::ApplicationEvent(3).to_u8(),
            None, None, None,
            emit_secs(),
        ).expect("Failed to create large event");

        // Verify data integrity
        assert_eq!(small_event.data(), small_data);
        assert_eq!(medium_event.data(), &medium_data);
        assert_eq!(large_event.data(), &large_data);

        // Verify event types
        assert_eq!(small_event.event_type(), EventType::ApplicationEvent(1).to_u8());
        assert_eq!(medium_event.event_type(), EventType::ApplicationEvent(2).to_u8());
        assert_eq!(large_event.event_type(), EventType::ApplicationEvent(3).to_u8());
    }

    #[test]
    fn test_archive_format_integration() {
        initialize();
        XaeroPoolManager::init();

        // Test that the new archive format works correctly
        let test_data = b"archive format test data";
        let original_event = XaeroPoolManager::create_xaero_event(
            test_data,
            EventType::ApplicationEvent(100).to_u8(),
            None, None, None,
            emit_secs(),
        ).expect("Failed to create original event");

        // Archive the event
        let archived_bytes = archive_xaero_event(&original_event);
        assert!(!archived_bytes.is_empty(), "Archived bytes should not be empty");

        // Verify we can unarchive it back
        use crate::indexing::storage::format::{unarchive_to_xaero_event, unarchive_to_raw_data};

        // Test raw data extraction
        let (header, raw_data) = unarchive_to_raw_data(&archived_bytes);
        assert_eq!(raw_data, test_data);
        assert_eq!(header.event_type, EventType::ApplicationEvent(100).to_u8());

        // Test full reconstruction
        let reconstructed_event = unarchive_to_xaero_event(&archived_bytes)
            .expect("Failed to reconstruct event");

        assert_eq!(reconstructed_event.data(), test_data);
        assert_eq!(reconstructed_event.event_type(), EventType::ApplicationEvent(100).to_u8());
    }
}