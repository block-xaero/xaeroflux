use std::{
    ffi::CString,
    fmt::Debug,
    ptr,
    sync::{Arc, Mutex},
};

use liblmdb::{
    MDB_CREATE, MDB_RDONLY, MDB_RESERVE, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn,
    MDB_val, mdb_cursor_get, mdb_cursor_open, mdb_dbi_close, mdb_dbi_open, mdb_env_create,
    mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_get, mdb_put, mdb_txn_abort,
    mdb_txn_begin, mdb_txn_commit,
};
use rkyv::{rancor::Failure, util::AlignedVec};

use crate::{
    core::event::{Event, EventType},
    indexing::hash::sha_256,
};

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

use super::{CONF, initialize, listeners::EventListener};

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

/// Scan all events whose 8-byte big-endian timestamp prefix is in [start_ms, end_ms).
/// ## Safety
/// This function is unsafe because it directly interacts with the LMDB C API.
/// It assumes that the environment is already created and opened.
/// It also assumes that the database is already opened.
pub unsafe fn scan_day(
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
        mdb_txn_abort(rtxn); // read-only txn: abort is OK
        Ok(results)
    }
}

/// pushes an event to the LMDB database.
/// The event is serialized using the `rkyv` crate.
/// The event is stored in the first database (aof).
/// The event key is a combination of the event timestamp, event type, and a hash of the event data.
/// The event value is the serialized event data.
/// This is ZERO COPY, aligned vec, so no copying of the data is done.
/// Event key is a Pod struct, so it is also zero copy.
/// NOTE: Actual event data is not stored in the database. it is stored in the target file system
/// chunk by chunk.
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
        let dbi = match event.event_type {
            EventType::MetaEvent(_) => env.dbis[1],
            _ => env.dbis[0],
        };
        let key = generate_key(event)?;
        let key_bytes: &[u8] = bytemuck::bytes_of(&key);
        let meta_key = match event.event_type {
            EventType::MetaEvent(1) => Some("segment_meta".as_bytes()),
            EventType::MetaEvent(2) => Some("mmr_meta".as_bytes()),
            _ => None,
        };
        let mdb_key = match meta_key {
            Some(mk) => &mut MDB_val {
                mv_size: mk.len(),
                mv_data: mk.as_ptr() as *mut libc::c_void,
            },
            None => &mut MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut libc::c_void,
            },
        };
        let value = generate_value(event)?;
        let mut data_val = MDB_val {
            mv_size: value.len(),
            mv_data: std::ptr::null_mut(),
        };
        let sc_mdb_put = mdb_put(txn, dbi, mdb_key, &mut data_val, MDB_RESERVE);
        if sc_mdb_put != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_mdb_put)));
        }
        std::ptr::copy_nonoverlapping(value.as_ptr(), data_val.mv_data.cast(), value.len());
        tracing::info!("Pushed event to LMDB: {:?}", key);
        let sc_tx_commit = mdb_txn_commit(txn);
        if sc_tx_commit != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_commit)));
        }
    }
    Ok(())
}

/// Shortcut to get the meta value from the LMDB database.
/// This is a zero-copy operation.
/// The meta value is stored in the second database (meta).
/// Meta key is "segment_meta" or "mmr_meta".
/// The meta value is a `rkyv` `rkyv::to_bytes` archived/ serialized `MMRMeta` struct or
/// `SegmentMeta` struct.
/// # Safety
/// SAFE: This function is unsafe because it directly interacts with the LMDB C API.
/// It assumes that the environment is already created and opened.
/// It also assumes that the database is already opened.
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

/// Generate a key for the event.
/// The key is a combination of the event timestamp, event type, and a hash of the event data.
pub fn generate_key(event: &Event<Vec<u8>>) -> Result<EventKey, Failure> {
    Ok(EventKey {
        ts: event.ts.to_be(),
        kind: event.event_type.to_u8(),
        hash: sha_256(&event.data),
    })
}
/// Store event carrying raw bytes.
/// The event is serialized using the `rkyv` crate.
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
        date_time::{MS_PER_DAY, day_bounds_from_epoch_ms},
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
        let bufs = unsafe { scan_day(&arc_env, start, end).expect("failed to unravel") };
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
        let bufs2 =
            unsafe { scan_day(&arc_env, tomorrow_start, tomorrow_end).expect("failed to unravel") };
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
        };
        let meta2 = SegmentMeta {
            page_index: 5,
            segment_index: 6,
            write_pos: 512,
            byte_offset: 1024,
            latest_segment_id: 7,
        };
        // wrap into Event<Vec<u8>>
        let bytes1 = bytemuck::bytes_of(&meta1).to_vec();
        let ev1 = Event::new(bytes1.clone(), EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &ev1).expect("failed_to_unwrap");
        // read back
        let raw1 = unsafe { get_meta_val(&arc_env, b"segment_meta") };
        let archived_ev1 = rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, Failure>(&raw1)
            .expect("failed_to_unwrap");
        let data1 = &archived_ev1.data;
        let got1: &SegmentMeta = bytemuck::from_bytes(data1);
        assert_eq!(got1.page_index, meta1.page_index);
        assert_eq!(got1.byte_offset, meta1.byte_offset);
        assert_eq!(got1.write_pos, meta1.write_pos);
        assert_eq!(got1.segment_index, meta1.segment_index);
        assert_eq!(got1.latest_segment_id, meta1.latest_segment_id);

        // now overwrite
        let bytes2 = bytemuck::bytes_of(&meta2).to_vec();
        let ev2 = Event::new(bytes2.clone(), EventType::MetaEvent(1).to_u8());
        push_event(&arc_env, &ev2).expect("failed_to_unwrap");
        let raw2 = unsafe { get_meta_val(&arc_env, b"segment_meta") };
        let archived_ev2 = rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, Failure>(&raw2)
            .expect("failed_to_unwrap");
        let data2 = &archived_ev2.data;
        let got2: &SegmentMeta = bytemuck::from_bytes(data2);
        assert_eq!(got2.page_index, meta2.page_index);
        assert_eq!(got2.byte_offset, meta2.byte_offset);
        assert_eq!(got2.write_pos, meta2.write_pos);
        assert_eq!(got2.segment_index, meta2.segment_index);
        assert_eq!(got2.latest_segment_id, meta2.latest_segment_id);
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
        };
        let mmr_meta = MMRMeta {
            root_hash: sha_256::<String>(&"deadbeef".to_string()),
            peaks_count: 4,
            leaf_count: 16,
            segment_meta: seg_meta,
        };
        let meta_bytes = rkyv::to_bytes::<Failure>(&mmr_meta)
            .expect("failed_to_unwrap")
            .to_vec();
        let ev = Event::new(meta_bytes.clone(), EventType::MetaEvent(2).to_u8());
        push_event(&arc_env, &ev).expect("failed_to_unwrap");
        let raw = unsafe { get_meta_val(&arc_env, b"mmr_meta") };
        let archived_ev = rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, Failure>(&raw)
            .expect("failed_to_unwrap");
        let data = &archived_ev.data;
        let got_meta: MMRMeta =
            rkyv::from_bytes::<MMRMeta, Failure>(data).expect("failed_to_unwrap");
        assert_eq!(got_meta.root_hash, mmr_meta.root_hash);
        assert_eq!(got_meta.peaks_count, mmr_meta.peaks_count);
        assert_eq!(got_meta.leaf_count, mmr_meta.leaf_count);
        assert_eq!(got_meta.segment_meta.page_index, seg_meta.page_index);
    }
}
