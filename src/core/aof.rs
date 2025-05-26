//! Append-only event log (AOF) actor using LMDB for persistent storage.
//!
//! This module provides:
//! - `AOFActor`: listens for events and writes them durably into LMDB.
//! - `LmdbEnv`: wrapper around the LMDB environment and databases.
//! - Functions to push events, scan event ranges, and manage metadata.

use std::{
    ffi::CString,
    fmt::Debug,
    ptr,
    sync::{Arc, Mutex},
};

// use crate::core::event::ArchivedEvent;
// use rkyv::rancor::Failure;
// use rkyv::api::high::access;
use liblmdb::{
    MDB_CREATE, MDB_RDONLY, MDB_RESERVE, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn,
    MDB_val, mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_dbi_close, mdb_dbi_open,
    mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_get, mdb_put,
    mdb_txn_abort, mdb_txn_begin, mdb_txn_commit,
};
use rkyv::{rancor::Failure, util::AlignedVec};

use crate::{
    core::event::{Event, EventType},
    indexing::hash::sha_256,
};
pub const AOF_DB: i32 = 0;
pub const META_DB: i32 = 1;
pub enum DBI {
    AOF = 0,
    META = 1,
}

/// An append-only file (AOF) actor that persists events into LMDB.
///
/// The `AOFActor` encapsulates an LMDB environment and an event listener.
/// It writes incoming events to disk for durable storage and later retrieval.
pub struct AOFActor {
    pub env: Arc<Mutex<LmdbEnv>>,
    pub listener: EventListener<Vec<u8>>,
}

impl Default for AOFActor {
    fn default() -> Self {
        Self::new()
    }
}

impl AOFActor {
    pub fn new() -> Self {
        initialize();
        let c = CONF.get().expect("failed to unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new("xaeroflux-aof").expect("failed to unravel"),
        ));
        let env_c = Arc::clone(&env);
        let h = Arc::new(move |e| {
            tracing::info!("Pushing event to LMDB: {:?}", e);
            push_event(&env_c, &e).expect("failed to unravel");
        });
        Self {
            env,
            listener: EventListener::new(
                "xaeroflux-aof",
                h,
                Some(CONF.get().expect("failed to load config").aof.buffer_size),
                Some(c.aof.threads.num_worker_threads),
            ),
        }
    }
}
/// A wrapper around an LMDB environment with two databases: AOF and META.
///
/// - AOF_DB stores application events keyed by timestamp, event type, and hash.
/// - META_DB stores segment and MMR metadata for efficient lookup.
pub struct LmdbEnv {
    pub env: *mut MDB_env,
    pub dbis: [MDB_dbi; 2],
}
unsafe impl Sync for LmdbEnv {} // raw pointers are Sync
// Assuming LmdbEnv is already Send:
unsafe impl Send for LmdbEnv {} // raw pointers are Send
// No need for Sync, since we guard with a Mutex
impl LmdbEnv {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(path)?;
        // 1) create & configure env
        let mut env = ptr::null_mut();
        unsafe {
            tracing::info!("Creating LMDB environment at {}", path);
            let sc_create_env = mdb_env_create(&mut env);
            if sc_create_env != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_create_env)));
            }
            tracing::info!("Configuring LMDB environment");
            tracing::info!("Setting max DBs to 2");
            let sc_set_max_dbs = mdb_env_set_maxdbs(env, 2);
            if sc_set_max_dbs != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_set_max_dbs)));
            }
            tracing::info!("Setting mapsize to 1GB");
            let sc_set_mapsize = mdb_env_set_mapsize(env, 1 << 30);
            if sc_set_mapsize != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_set_mapsize)));
            }
            let cs = CString::new(path)?;
            let sc_env_open = mdb_env_open(env, cs.as_ptr(), MDB_CREATE, 0o600);
            if sc_env_open != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_env_open)));
            }
        }
        // 2) open both DBIs using helper
        let aof_dbi = unsafe { open_named_db(env, c"xaeroflux-aof".as_ptr())? };
        let meta_dbi = unsafe { open_named_db(env, c"xaeroflux-meta".as_ptr())? };

        Ok(Self {
            env,
            dbis: [aof_dbi, meta_dbi],
        })
    }
}

/// Open a named database in the LMDB environment.
/// This function is unsafe because it directly interacts with the LMDB C API.
/// It assumes that the environment is already created and opened.
/// It also assumes that the database name is valid and does not contain null characters.
/// The function will create the database if it does not exist.
/// It returns the database handle (MDB_dbi) on success.
/// If an error occurs, it returns a boxed error.
/// The caller is responsible for managing the lifetime of the database handle.
/// Opens or creates a named database in the LMDB environment.
///
/// Begins a write transaction, opens (or creates) the given database name,
/// commits the transaction, and returns the `MDB_dbi` handle.
///
/// # Safety
/// This function calls directly into the LMDB C API and assumes the
/// environment is valid. The caller must ensure the `env` pointer is correct
/// and `name_ptr` is a valid C string.
unsafe fn open_named_db(
    env: *mut MDB_env,
    name_ptr: *const i8,
) -> Result<MDB_dbi, Box<dyn std::error::Error>> {
    unsafe {
        let mut txn = ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env, ptr::null_mut(), 0, &mut txn);
        if sc_tx_begin != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_begin)));
        }
        let mut dbi = 0;
        let sc_dbi_open = mdb_dbi_open(txn, name_ptr, MDB_CREATE, &mut dbi);
        if sc_dbi_open != 0 {
            let sc_tx_commit = mdb_txn_commit(txn);
            if sc_tx_commit != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_commit)));
            }
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_dbi_open)));
        }
        let sc_tx_commit = mdb_txn_commit(txn);
        if sc_tx_commit != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_commit)));
        }
        // return the dbi
        Ok(dbi)
    }
}

impl Drop for LmdbEnv {
    fn drop(&mut self) {
        unsafe {
            // close the dbi
            for dbi in self.dbis.iter() {
                mdb_dbi_close(self.env, *dbi);
            }
            // close the env
            liblmdb::mdb_env_close(self.env);
        }
    }
}

use bytemuck::{Pod, Zeroable};

use super::{CONF, initialize, listeners::EventListener, meta::SegmentMeta};

#[repr(C)]
#[derive(Copy, Clone)]
pub struct EventKey {
    ts: u64,        // 8 bytes, big-endian
    kind: u8,       // 1 byte
    hash: [u8; 32], // 32 bytes
}
unsafe impl Pod for EventKey {}
unsafe impl Zeroable for EventKey {}

impl Debug for EventKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventKey {{ ts: {}, kind: {}, hash: {} }}",
            self.ts,
            self.kind,
            hex::encode(self.hash)
        )
    }
}

/// Scans archived events in the AOF database for a given timestamp range.
///
/// Returns a `Vec<AlignedVec>` where each element is the serialized event data
/// for events whose keys have timestamps in `[start_ms, end_ms)`.
///
/// # Safety
/// This function interacts directly with the LMDB C API and assumes the
/// environment and database are open and valid.
pub unsafe fn scan_range(
    env: &Arc<Mutex<LmdbEnv>>,
    start_ms: u64,
    end_ms: u64,
) -> anyhow::Result<Vec<AlignedVec>> {
    let mut results = Vec::<AlignedVec>::new();
    let g = env.lock().expect("failed to lock env");
    let env = g.env;
    unsafe {
        // 1) Begin a read txn
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            return Err(anyhow::anyhow!("Failed to begin read txn: {}", sc_tx_begin));
        }
        // 2) Open a cursor on that DB
        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, g.dbis[0], &mut cursor);
        if sc_cursor_open != 0 {
            mdb_txn_abort(rtxn);
            return Err(anyhow::anyhow!("Failed to open cursor: {}", sc_cursor_open));
        }
        // 3) Build the start key MDB_val
        let mut start_key = [0u8; 16];
        start_key[..8].copy_from_slice(&start_ms.to_be_bytes());
        // the rest of the key (kind + seq) can be left as zero
        let mut key_val = MDB_val {
            mv_size: start_key.len(),
            mv_data: start_key.as_ptr() as *mut _,
        };
        // empty MDB_val for the value
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        // 4) Position to first key ≥ start_key
        let rc = mdb_cursor_get(
            cursor,
            &mut key_val,
            &mut data_val,
            liblmdb::MDB_cursor_op_MDB_SET_RANGE,
        );
        if rc != 0 {
            // MDB_NOTFOUND or error: no items ≥ start_key
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        loop {
            // 5) Extract the timestamp prefix from key_val.mv_data
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);
            let ts = u64::from_be_bytes(raw_key[0..8].try_into().expect("failed to unravel"));
            if ts >= end_ms {
                break;
            }

            // 6) `data_val` now contains your archived bytes, you can do rkyv::archived_root on
            //    them here ...
            let data_slice =
                std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
            let mut av: AlignedVec = AlignedVec::new();
            av.extend_from_slice(data_slice);
            results.push(av);
            // 7) Advance the cursor
            let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
            if rc != 0 {
                // MDB_NOTFOUND = end of DB, or real error
                break;
            }
        }
        // 8) Cleanup
        mdb_cursor_close(cursor);
        mdb_txn_abort(rtxn); // read-only txn: abort is OK
        Ok(results)
    }
}

/// Persists an event into the LMDB AOF or META database.
///
/// Behavior varies by event type:
/// - `MetaEvent(1)`: stores segment metadata under composite and static keys.
/// - `MetaEvent(2)`: stores MMR metadata under a static key.
/// - Application events: stored in the AOF database with a composite key.
///
/// # Errors
/// Returns an error if any LMDB operation fails.
pub fn push_event(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    event: &Event<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        let env = arc_env.lock().expect("failed to lock env");
        let mut txn = ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn);
        if sc_tx_begin != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_begin)));
        }
        // handle segment_meta events specially
        if let EventType::MetaEvent(1) = event.event_type {
            // 1) Composite entry: raw SegmentMeta bytes under composite key
            let sm: &SegmentMeta = bytemuck::from_bytes(&event.data);
            let mut key_buf = [0u8; 16];
            key_buf[..8].copy_from_slice(&sm.ts_start.to_be_bytes());
            key_buf[8..16].copy_from_slice(&(sm.segment_index as u64).to_be_bytes());
            let mut key_val = MDB_val {
                mv_size: key_buf.len(),
                mv_data: key_buf.as_ptr() as *mut libc::c_void,
            };
            let raw_bytes = &event.data;
            let mut data_val = MDB_val {
                mv_size: raw_bytes.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc = mdb_put(
                txn,
                env.dbis[META_DB as usize],
                &mut key_val,
                &mut data_val,
                MDB_RESERVE,
            );
            if sc != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
            }
            std::ptr::copy_nonoverlapping(
                raw_bytes.as_ptr(),
                data_val.mv_data.cast(),
                raw_bytes.len(),
            );

            // 2) Static entry: raw SegmentMeta bytes under "segment_meta"
            let static_key = b"segment_meta";
            let mut static_key_val = MDB_val {
                mv_size: static_key.len(),
                mv_data: static_key.as_ptr() as *mut libc::c_void,
            };
            let mut static_val = MDB_val {
                mv_size: raw_bytes.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc2 = mdb_put(
                txn,
                env.dbis[META_DB as usize],
                &mut static_key_val,
                &mut static_val,
                MDB_RESERVE,
            );
            if sc2 != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc2)));
            }
            std::ptr::copy_nonoverlapping(
                raw_bytes.as_ptr(),
                static_val.mv_data.cast(),
                raw_bytes.len(),
            );
        } else if let EventType::MetaEvent(2) = event.event_type {
            // Static entry: raw MMRMeta bytes under "mmr_meta"
            let static_key = b"mmr_meta";
            let mut key_val = MDB_val {
                mv_size: static_key.len(),
                mv_data: static_key.as_ptr() as *mut libc::c_void,
            };
            let raw_bytes = &event.data;
            let mut data_val = MDB_val {
                mv_size: raw_bytes.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc = mdb_put(
                txn,
                env.dbis[META_DB as usize],
                &mut key_val,
                &mut data_val,
                MDB_RESERVE,
            );
            if sc != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
            }
            std::ptr::copy_nonoverlapping(
                raw_bytes.as_ptr(),
                data_val.mv_data.cast(),
                raw_bytes.len(),
            );
        } else {
            // application event: existing path
            let key = generate_key(event)?;
            let key_bytes: &[u8] = bytemuck::bytes_of(&key);
            let mut key_val = MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut libc::c_void,
            };
            let value = generate_value(event)?;
            let mut data_val = MDB_val {
                mv_size: value.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc = mdb_put(
                txn,
                env.dbis[AOF_DB as usize],
                &mut key_val,
                &mut data_val,
                MDB_RESERVE,
            );
            if sc != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
            }
            std::ptr::copy_nonoverlapping(value.as_ptr(), data_val.mv_data.cast(), value.len());
        }
        tracing::info!("Pushed event to LMDB: {:?}", event.event_type);
        let sc_tx_commit = mdb_txn_commit(txn);
        if sc_tx_commit != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_commit)));
        }
    }
    Ok(())
}

/// Reads all `SegmentMeta` entries from the META_DB whose composite key
/// `[ts_start (8 bytes BE) ‖ segment_index (8 bytes BE)]` is in `[start, end)`.
///
/// Returns a `Vec<SegmentMeta>` in ascending `ts_start` order.
pub fn iterate_segment_meta_by_range(
    env: &Arc<Mutex<LmdbEnv>>,
    start: u64,
    end: Option<u64>,
) -> Result<Vec<SegmentMeta>, Box<dyn std::error::Error>> {
    use liblmdb::MDB_cursor_op_MDB_NEXT;
    let mut results = Vec::<SegmentMeta>::new();
    let g = env.lock().expect("failed to lock env");
    let env = g.env;
    unsafe {
        // 1) Begin a read txn
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            return Err(format!("Failed to begin read txn: {}", sc_tx_begin).into());
        }
        // 2) Open a cursor on that DB (should be meta DB, index 1)
        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, g.dbis[1], &mut cursor);
        if sc_cursor_open != 0 {
            mdb_txn_abort(rtxn);
            return Err(format!("Failed to open cursor: {}", sc_cursor_open).into());
        }
        // 3) Build the start key MDB_val
        // key: [ ts_start (8) ‖ segment_index (8) ]
        let mut start_key = [0u8; 16]; // 8 for start ts, 8 for end and then zero for seg_id
        start_key[..8].copy_from_slice(&start.to_be_bytes());
        // The rest is already zeroed by the array initialization

        // the rest of the key (kind + seq) can be left as zero
        let mut key_val = MDB_val {
            mv_size: start_key.len(),
            mv_data: start_key.as_ptr() as *mut _,
        };
        // empty MDB_val for the value
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        // 4) Position to first key ≥ start_key
        let rc = mdb_cursor_get(
            cursor,
            &mut key_val,
            &mut data_val,
            liblmdb::MDB_cursor_op_MDB_SET_RANGE,
        );
        if rc != 0 {
            // MDB_NOTFOUND or error: no items ≥ start_key
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        loop {
            // skip static or non-composite keys (we only want 16-byte ts_start‖segment_index keys)
            if key_val.mv_size != 16 {
                // advance cursor and continue
                let rc_skip =
                    mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
                if rc_skip != 0 {
                    break; // no more entries
                }
                continue;
            }
            // 5) Extract the timestamp prefix from key_val.mv_data
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);
            let ts = u64::from_be_bytes(raw_key[0..8].try_into().expect("failed to unravel"));
            if let Some(e) = end {
                // for start=0, include ts == end; otherwise treat end as exclusive
                let should_break = if start == 0 { ts > e } else { ts >= e };
                if should_break {
                    break;
                }
            }

            // 6) Directly parse raw SegmentMeta bytes (packed struct allows unaligned)
            let data_slice =
                std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
            let sm: &SegmentMeta = bytemuck::from_bytes(data_slice);
            results.push(*sm);

            // 7) Advance the cursor
            let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
            if rc != 0 {
                // MDB_NOTFOUND = end of DB, or real error
                break;
            }
        }
        // 8) Cleanup
        mdb_cursor_close(cursor);
        mdb_txn_abort(rtxn); // read-only txn: abort is OK
    }
    Ok(results)
}

/// Retrieves a metadata value from META_DB for the given key.
///
/// Returns the raw bytes of the stored metadata (e.g., segment or MMR meta).
///
/// # Safety
/// Directly calls the LMDB C API and assumes the environment and database are open.
pub unsafe fn get_meta_val(env: &Arc<Mutex<LmdbEnv>>, key: &[u8]) -> AlignedVec {
    let guard = env.lock().expect("failed_to_unwrap");
    let lmdb = &*guard;
    // begin read txn
    let mut txn: *mut MDB_txn = ptr::null_mut();
    let rc_begin = unsafe { mdb_txn_begin(lmdb.env, ptr::null_mut(), MDB_RDONLY, &mut txn) };
    if rc_begin != 0 {
        panic!("failed to begin read txn: {}", rc_begin);
    }
    // prepare key
    let mut key_val = MDB_val {
        mv_size: key.len(),
        mv_data: key.as_ptr() as *mut _,
    };
    let mut data_val = MDB_val {
        mv_size: 0,
        mv_data: ptr::null_mut(),
    };
    // get
    let rc = unsafe { mdb_get(txn, lmdb.dbis[1], &mut key_val, &mut data_val) };
    if rc != 0 {
        // txn abort
        unsafe { mdb_txn_abort(txn) };
        panic!("failed to get meta value: {}", rc);
    }
    // copy into aligned vec
    let slice =
        unsafe { std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size) };
    let mut av = AlignedVec::new();
    av.extend_from_slice(slice);
    // abort txn
    unsafe { mdb_txn_abort(txn) };
    av
}

/// Generates an `EventKey` consisting of the event timestamp, type, and data hash.
pub fn generate_key(event: &Event<Vec<u8>>) -> Result<EventKey, Failure> {
    Ok(EventKey {
        ts: event.ts.to_be(),
        kind: event.event_type.to_u8(),
        hash: sha_256(&event.data),
    })
}
/// Serializes an event into a zero-copy `AlignedVec` using `rkyv`.
pub fn generate_value(event: &Event<Vec<u8>>) -> Result<AlignedVec, Failure> {
    let bytes = rkyv::to_bytes::<Failure>(event)?;
    Ok(bytes)
}

// In your module file, add these tests at the bottom:

#[cfg(test)]
mod tests {

    use bytemuck::bytes_of;
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

    use super::*;
    use crate::core::{
        date_time::{MS_PER_DAY, day_bounds_from_epoch_ms, emit_secs},
        event::{ArchivedEvent, EventType},
        initialize,
        meta::{MMRMeta, SegmentMeta},
    };

    #[repr(C)]
    #[derive(Debug, Clone, Archive, Serialize, Deserialize, Default)]
    #[rkyv(derive(Debug))]
    pub struct Post {
        pub zero_id: u64,
        pub title: String,
        pub body: String,
    }

    /// Helper to read an archived Event<Vec<u8>> back into an owned Event
    #[test]
    fn test_env_creation_and_dbi_handles() {
        initialize();
        let dir = tempdir().expect("failed to unravel");
        let env = LmdbEnv::new(dir.path().to_str().expect("failed to unravel"))
            .expect("failed to unravel");
        // env.env should be non-null and dbis > 0
        assert!(!env.env.is_null());
        assert!(env.dbis[0] > 0);
        assert!(env.dbis[1] > 0);
    }

    #[test]
    fn test_generate_key_consistency() {
        initialize();
        let data = b"hello".to_vec();
        let event = Event {
            event_type: EventType::ApplicationEvent(1),
            version: 1,
            data: data.clone(),
            ts: 123_456_789,
        };
        let key = generate_key(&event).expect("failed to unravel");
        let bytes = bytes_of(&key);
        tracing::info!("key bytes: {:?}", bytes);
        assert_eq!(
            u64::from_be_bytes(bytes[0..8].try_into().expect("failed to unravel")),
            123_456_789
        );
        // kind
        assert_eq!(bytes[8], event.event_type.to_u8());
        // hash length
        assert_eq!(bytes.len(), std::mem::size_of::<EventKey>());
    }

    #[test]
    fn test_generate_value_roundtrip() {
        initialize();
        let payload = b"abc123".to_vec();
        let event = Event::new(payload.clone(), 1);
        let buf = generate_value(&event).expect("failed to unravel");
        let event2 = rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, Failure>(&buf)
            .expect("failed to access");
        assert_eq!(event2.data, payload);
        assert_eq!(event2.ts, event.ts);
    }

    #[test]
    fn test_push_and_scan_day() {
        initialize();
        let dir = tempdir().expect("failed to unravel");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to unravel"))
                .expect("failed to unravel"),
        ));
        // push two events
        let e1 = Event::new(b"one".to_vec(), 1);
        push_event(&arc_env, &e1).expect("failed to unravel");
        let e2 = Event::new(b"two".to_vec(), 2);
        push_event(&arc_env, &e2).expect("failed to unravel");

        // compute current day bounds
        let (start, end) = day_bounds_from_epoch_ms(e1.ts);
        // scan today: should see at least 2
        let bufs = unsafe { scan_range(&arc_env, start, end).expect("failed to unravel") };
        assert!(bufs.len() >= 2, "expected >=2 events, got {}", bufs.len());
        // roundtrip them
        let events: Vec<&ArchivedEvent<Vec<u8>>> = bufs
            .iter()
            .map(|b| {
                rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, Failure>(b)
                    .expect("failed to access")
            })
            .collect();
        assert!(events.iter().any(|e| e.data == b"one".to_vec()));
        assert!(events.iter().any(|e| e.data == b"two".to_vec()));

        // scan next day: should be empty
        let tomorrow_start = end;
        let tomorrow_end = tomorrow_start + MS_PER_DAY;
        let bufs2 = unsafe {
            scan_range(&arc_env, tomorrow_start, tomorrow_end).expect("failed to unravel")
        };
        assert_eq!(bufs2.len(), 0, "expected 0 events, got {}", bufs2.len());
    }

    #[test]
    fn test_open_named_db_idempotent() {
        initialize();
        let dir = tempdir().expect("failed to unravel");
        let env = LmdbEnv::new(dir.path().to_str().expect("failed to unravel"))
            .expect("failed to unravel");
        // opening again should give same handle values
        let a1 = unsafe {
            open_named_db(env.env, c"xaeroflux-aof".as_ptr()).expect("failed to unravel")
        };
        let m1 = unsafe {
            open_named_db(env.env, c"xaeroflux-meta".as_ptr()).expect("failed to unravel")
        };
        assert_eq!(a1, env.dbis[0]);
        assert_eq!(m1, env.dbis[1]);
    }

    #[test]
    fn test_segment_meta_overwrite() {
        initialize();
        let dir = tempdir().expect("failed_to_unwrap");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap"),
        ));
        // two different metas
        let meta1 = SegmentMeta {
            page_index: 1,
            segment_index: 2,
            write_pos: 128,
            byte_offset: 256,
            latest_segment_id: 3,
            ts_start: emit_secs(),
            ts_end: emit_secs(),
        };
        let meta2 = SegmentMeta {
            page_index: 5,
            segment_index: 6,
            write_pos: 512,
            byte_offset: 1024,
            latest_segment_id: 7,
            ts_start: emit_secs(),
            ts_end: emit_secs(),
        };
        // wrap into Event<Vec<u8>>
        let bytes1 = bytemuck::bytes_of(&meta1).to_vec();
        let ev1 = Event::new(bytes1.clone(), EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &ev1).expect("failed_to_unwrap");
        // read back
        let raw1 = unsafe { get_meta_val(&arc_env, b"segment_meta") };
        let got1: &SegmentMeta = bytemuck::from_bytes(&raw1);
        let g1_pid = got1.page_index;
        let m1_pid = meta1.page_index;
        assert_eq!(g1_pid, m1_pid);
        let g1_boff = got1.byte_offset;
        let m1_boff = meta1.byte_offset;
        assert_eq!(g1_boff, m1_boff);
        let g1_wpos = got1.write_pos;
        let m1_wpos = meta1.write_pos;
        assert_eq!(g1_wpos, m1_wpos);
        let g1_sid = got1.segment_index;
        let m1_sid = meta1.segment_index;
        assert_eq!(g1_sid, m1_sid);
        let g1_lsid = got1.latest_segment_id;
        let m1_lsid = meta1.latest_segment_id;
        assert_eq!(g1_lsid, m1_lsid);

        // now overwrite
        let bytes2 = bytemuck::bytes_of(&meta2).to_vec();
        let ev2 = Event::new(bytes2.clone(), EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &ev2).expect("failed_to_unwrap");
        let raw2 = unsafe { get_meta_val(&arc_env, b"segment_meta") };
        let got2: &SegmentMeta = bytemuck::from_bytes(&raw2);
        let g2_pid = got2.page_index;
        let m2_pid = meta2.page_index;
        assert_eq!(g2_pid, m2_pid);
        let g2_boff = got2.byte_offset;
        let m2_boff = meta2.byte_offset;
        assert_eq!(g2_boff, m2_boff);
        let g2_wpos = got2.write_pos;
        let m2_wpos = meta2.write_pos;
        assert_eq!(g2_wpos, m2_wpos);
        let g2_sid = got2.segment_index;
        let m2_sid = meta2.segment_index;
        assert_eq!(g2_sid, m2_sid);
        let g2_lsid = got2.latest_segment_id;
        let m2_lsid = meta2.latest_segment_id;
        assert_eq!(g2_lsid, m2_lsid);
    }

    #[test]
    fn test_mmr_meta_store_and_retrieve() {
        initialize();
        let dir = tempdir().expect("failed_to_unwrap");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap"),
        ));
        let seg_meta = SegmentMeta {
            page_index: 8,
            segment_index: 9,
            write_pos: 2048,
            byte_offset: 4096,
            latest_segment_id: 10,
            ts_start: emit_secs(),
            ts_end: emit_secs(),
        };
        let mmr_meta = MMRMeta {
            root_hash: sha_256::<String>(&"deadbeef".to_string()),
            peaks_count: 4,
            leaf_count: 16,
            segment_meta: seg_meta,
        };
        let meta_bytes = bytemuck::bytes_of(&mmr_meta);
        let ev = Event::new(meta_bytes.to_vec(), EventType::MetaEvent(2).to_u8());
        push_event(&arc_env, &ev).expect("failed_to_unwrap");
        let raw = unsafe { get_meta_val(&arc_env, b"mmr_meta") };
        let got_meta: &MMRMeta = bytemuck::from_bytes::<MMRMeta>(raw.as_slice());
        assert_eq!(got_meta.root_hash, mmr_meta.root_hash);
        let got_peaks_count = got_meta.peaks_count;
        let mmr_peaks_count = mmr_meta.peaks_count;
        assert_eq!(got_peaks_count, mmr_peaks_count);
        let got_leaf_count = got_meta.leaf_count;
        let mmr_leaf_count = mmr_meta.leaf_count;
        assert_eq!(got_leaf_count, mmr_leaf_count);
        let got_page_index = got_meta.segment_meta.page_index;
        let sm_pid = seg_meta.page_index;
        assert_eq!(got_page_index, sm_pid);
    }

    use std::sync::{Arc, Mutex};

    use crate::core::event::Event;

    fn make_meta(ts_start: u64, ts_end: u64, idx: usize) -> SegmentMeta {
        SegmentMeta {
            page_index: idx * 10,
            segment_index: idx,
            write_pos: idx * 100,
            byte_offset: idx * 1000,
            latest_segment_id: idx,
            ts_start,
            ts_end,
        }
    }

    #[test]
    fn empty_db_returns_empty() {
        initialize();
        let dir = tempdir().expect("failed_to_unwrap");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap"),
        ));

        // no metas inserted yet
        let all = iterate_segment_meta_by_range(&env, 0, None).expect("failed_to_unwrap");
        assert!(all.is_empty());
    }

    #[test]
    fn single_meta_roundtrip() {
        initialize();
        let dir = tempdir().expect("failed_to_unwrap");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap"),
        ));

        // push one meta at ts = now
        let ts = emit_secs();
        let meta = make_meta(ts, ts + 5, 42);
        let bytes = bytemuck::bytes_of(&meta).to_vec();
        let ev = Event::new(bytes, EventType::MetaEvent(1).to_u8());
        push_event(&env, &ev).expect("failed_to_unwrap");

        // open‐ended scan from 0 should return it
        let all = iterate_segment_meta_by_range(&env, 0, None).expect("failed_to_unwrap");
        assert_eq!(all.len(), 1);
        let sid = all[0].segment_index;
        assert_eq!(sid, 42);

        // scan starting _after_ ts should drop it
        let none = iterate_segment_meta_by_range(&env, ts + 1, None).expect("failed_to_unwrap");
        assert!(none.is_empty());
    }

    #[test]
    fn multiple_meta_filter_and_ordering() {
        initialize();
        let dir = tempdir().expect("failed_to_unwrap");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap")).expect("failed_to_unwrap"),
        ));

        // create three metas at t=10,20,30
        let m0 = make_meta(10, 15, 0);
        let m1 = make_meta(20, 25, 1);
        let m2 = make_meta(30, 35, 2);

        for meta in &[m0, m1, m2] {
            let bytes = bytemuck::bytes_of(meta).to_vec();
            let ev = Event::new(bytes, EventType::MetaEvent(1).to_u8());
            push_event(&env, &ev).expect("failed_to_unwrap");
        }

        // scan [0..] returns all three, in timestamp order
        let all = iterate_segment_meta_by_range(&env, 0, None).expect("failed_to_unwrap");
        assert_eq!(all.len(), 3);
        assert_eq!(all.iter().map(|m| m.ts_start).collect::<Vec<_>>(), vec![
            10, 20, 30
        ]);

        // scan [15..30] should return m1 only (m0 ends at 15 but we filter on start >15)
        let mid = iterate_segment_meta_by_range(&env, 15, Some(30)).expect("failed_to_unwrap");
        assert_eq!(mid.len(), 1);
        let mid_idx = mid[0].segment_index;
        assert_eq!(mid_idx, 1);

        // scan [0..20] should include m0 and m1 (m1 starts exactly at 20)
        let upto = iterate_segment_meta_by_range(&env, 0, Some(20)).expect("failed_to_unwrap");
        assert_eq!(upto.len(), 2);
        assert_eq!(
            upto.iter().map(|m| m.segment_index).collect::<Vec<_>>(),
            vec![0, 1]
        );
    }
}
