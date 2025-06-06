pub mod aof;
pub mod indexing;
pub mod materializer;
pub mod networking;
pub mod pipe;

pub mod subject;
mod system_payload;

use std::{
    fmt::Display,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use indexing::storage::{actors::segment_writer_actor::SegmentConfig, format::archive};
use xaeroflux_core as core;
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
/// Envelope wrapping an application or system `Event` payload
/// along with an optional Merkle inclusion proof.
#[derive(Debug, Clone)]
pub struct XaeroEvent {
    /// Core event data (e.g., domain event encoded as bytes).
    pub evt: Event<Vec<u8>>,
    /// Optional Merkle proof bytes (e.g., from MMR).
    pub merkle_proof: Option<Vec<u8>>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ScanWindow {
    pub start: u64,
    pub end: u64,
}
unsafe impl Pod for ScanWindow {}
unsafe impl Zeroable for ScanWindow {}

/// Defines per-event pipeline operations.
#[derive(Clone)]
pub enum Operator {
    Scan(Arc<ScanWindow>),
    /// Transform the event into another event.
    Map(Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>),
    /// Keep only events matching the predicate.
    Filter(Arc<dyn Fn(&XaeroEvent) -> bool + Send + Sync>),
    /// Drop events without a Merkle proof.
    FilterMerkleProofs,
    /// Terminal op: drop all events.
    Blackhole,
}

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
    };
    use xaeroflux_macros::subject;

    use super::*;
    use crate::{
        aof::storage::{
            format::SegmentMeta,
            lmdb::{LmdbEnv, push_event},
        },
        core::initialize,
        indexing::storage::{
            actors::{
                segment_reader_actor::SegmentReaderActor, segment_writer_actor::SegmentConfig,
            },
            format::PAGE_SIZE,
        },
        subject::SubjectHash,
    };

    #[test]
    fn test_subject_macro() {
        initialize();
        use xaeroflux_core::event::{EventType, SystemEventKind};
        let expected_sha256: [u8; 32] =
            xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let subject =
            subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");
        let workspace_created_event = subject
            .control
            .source
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
        let tmp = tempdir().expect("failed to create tempdir");
        // write a segment file
        let ts = emit_secs();
        let idx = 0;
        let seg_path = tmp
            .path()
            .join(format!("xaeroflux-actors-{}-{}.seg", ts, idx));
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
        // push meta into LMDB
        let meta_env = Arc::new(Mutex::new(
            LmdbEnv::new(
                tmp.path().to_str().expect("failed_to_unwrap"),
                BusKind::Data,
            )
            .expect("create lmdb"),
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
            bytes_of(&seg_meta).to_vec(),
            EventType::MetaEvent(1).to_u8(),
        );
        push_event(&meta_env, &ev).expect("push_event");
        // instantiate actor
        let (tx, rx) = unbounded::<XaeroEvent>();
        let config = SegmentConfig {
            page_size: PAGE_SIZE,
            pages_per_segment: 1,
            prefix: "xaeroflux-actors".into(),
            segment_dir: tmp.path().to_string_lossy().into(),
            lmdb_env_path: tmp.path().to_string_lossy().into(),
        };
        let subject_hash = SubjectHash([0; 32]);
        let pipe = Pipe::new(BusKind::Data, None);
        let actor = SegmentReaderActor::new(subject_hash, pipe, config);
        // send replay event
        let replay_evt = Event::new(
            Vec::new(),
            EventType::SystemEvent(xaeroflux_core::event::SystemEventKind::Replay).to_u8(),
        );
        actor
            .pipe
            .sink
            .tx
            .send(XaeroEvent {
                evt: replay_evt,
                merkle_proof: None,
            })
            .expect("send replay");
        // receive and assert first (should be "hello")
        let first = rx.recv_timeout(Duration::from_secs(1)).expect("recv first");
        assert_eq!(first.evt.data, b"hello".to_vec());
    }

    #[test]
    fn test_subject_macro_object_event_and_hash() {
        initialize();
        // Compute expected SHA-256 for workspace and object
        let expected_ws_sha: [u8; 32] =
            xaeroflux_core::hash::sha_256_hash("cyan_workspace_123".as_bytes().to_vec());
        let expected_obj_sha: [u8; 32] = xaeroflux_core::hash::sha_256_hash(
            "cyan_object_white_board_id_134".as_bytes().to_vec(),
        );

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
        let subject =
            subject!("workspace/cyan_workspace_123/object/cyan_object_white_board_id_134");

        // 1) First event was WorkspaceCreated; drain it
        let ws_evt = subject
            .control
            .source
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed")
            .evt;
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
        let obj_evt = subject
            .control
            .source
            .rx
            .recv()
            .expect("attempt_to_unwrap_failed")
            .evt;
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
        // topic_key.flags was set to 1 in new_with_workspace
        assert_eq!(subject.topic_key.flags, 1);
        // workspace_id and object_id bytes checked above in previous test
        assert_eq!(subject.workspace_id.as_slice(), ws_sha.as_ref());
        assert_eq!(subject.object_id.as_slice(), obj_sha.as_ref());
        // hash field must match combined Blake3
        assert_eq!(subject.hash.0, combined_blake);
    }
}
