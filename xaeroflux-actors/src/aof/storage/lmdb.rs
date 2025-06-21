#[allow(deprecated)]
use std::{
    ffi::CString,
    io::{Error, ErrorKind},
    ptr,
    sync::{Arc, Mutex},
};

use liblmdb::{
    MDB_CREATE, MDB_NOTFOUND, MDB_RDONLY, MDB_RESERVE, MDB_SUCCESS, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn,
    MDB_val, mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_dbi_close, mdb_dbi_open, mdb_env_create,
    mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_put, mdb_strerror, mdb_txn_abort, mdb_txn_begin,
    mdb_txn_commit,
};
use rkyv::{rancor::Failure, util::AlignedVec};
use xaeroflux_core::{
    XaeroPoolManager,
    event::{EventType, XaeroEvent},
    hash::{sha_256, sha_256_slice},
};

use super::format::{EventKey, SegmentMeta};
use crate::{BusKind, indexing::storage::format::archive_xaero_event};

#[repr(usize)]
pub enum DBI {
    Aof = 0,
    Meta = 1,
    SecondaryIndex = 2,
}

/// A wrapper around an LMDB environment with three databases: AOF, META, and SecondaryIndex.
///
/// - AOF_DB stores Arc<XaeroEvent> data keyed by timestamp, event type, and hash.
/// - META_DB stores segment and MMR metadata for efficient lookup.
/// - SecondaryIndex_DB stores leaf hash -> segment meta mappings.
pub struct LmdbEnv {
    pub env: *mut MDB_env,
    pub dbis: [MDB_dbi; 3],
}

unsafe impl Sync for LmdbEnv {}
unsafe impl Send for LmdbEnv {}

impl LmdbEnv {
    pub fn new(path: &str, pipe_kind: BusKind) -> Result<Self, Box<dyn std::error::Error>> {
        let res = std::fs::create_dir_all(path);
        match res {
            Ok(_) => {}
            Err(e) => return Err(e.into()),
        }

        // 1) create & configure env
        let mut env = ptr::null_mut();
        unsafe {
            tracing::info!("Creating LMDB environment at {}", path);
            let sc_create_env = mdb_env_create(&mut env);
            if sc_create_env != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_create_env)));
            }

            tracing::info!("Configuring LMDB environment");
            tracing::info!("Setting max DBs to 3");
            let sc_set_max_dbs = mdb_env_set_maxdbs(env, 3);
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

        match pipe_kind {
            BusKind::Control => {
                let aof_dbi = unsafe { open_named_db(env, c"/aof".as_ptr())? };
                let meta_dbi = unsafe { open_named_db(env, c"/meta".as_ptr())? };
                Ok(Self {
                    env,
                    dbis: [aof_dbi, meta_dbi, 0],
                })
            }
            BusKind::Data => {
                let aof_dbi = unsafe { open_named_db(env, c"/aof".as_ptr())? };
                let meta_dbi = unsafe { open_named_db(env, c"/meta".as_ptr())? };
                let secondary = unsafe { open_named_db(env, c"/secondary".as_ptr())? };
                Ok(Self {
                    env,
                    dbis: [aof_dbi, meta_dbi, secondary],
                })
            }
        }
    }
}

/// Opens or creates a named database in the LMDB environment.
unsafe fn open_named_db(env: *mut MDB_env, name_ptr: *const i8) -> Result<MDB_dbi, Box<dyn std::error::Error>> {
    let mut txn = std::ptr::null_mut();
    // Phase 1: Try open without MDB_CREATE
    let rc = unsafe { mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn) };
    if rc != MDB_SUCCESS as i32 {
        return Err(from_lmdb_err(rc));
    }

    let mut dbi: MDB_dbi = 0;
    let rc_open = unsafe { mdb_dbi_open(txn, name_ptr, 0, &mut dbi) };
    if rc_open == MDB_SUCCESS as i32 {
        let rc_commit = unsafe { mdb_txn_commit(txn) };
        if rc_commit != MDB_SUCCESS as i32 {
            return Err(from_lmdb_err(rc_commit));
        }
        Ok(dbi)
    } else if rc_open == MDB_NOTFOUND {
        unsafe { mdb_txn_abort(txn) };
        // Phase 2: Try open/create with MDB_CREATE
        let mut txn2 = std::ptr::null_mut();
        let rc2 = unsafe { mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn2) };
        if rc2 != MDB_SUCCESS as i32 {
            return Err(from_lmdb_err(rc2));
        }

        let mut dbi2: MDB_dbi = 0;
        let rc_create = unsafe { mdb_dbi_open(txn2, name_ptr, MDB_CREATE, &mut dbi2) };
        if rc_create != MDB_SUCCESS as i32 {
            unsafe { mdb_txn_abort(txn2) };
            return Err(from_lmdb_err(rc_create));
        }

        let rc_commit = unsafe { mdb_txn_commit(txn2) };
        if rc_commit != MDB_SUCCESS as i32 {
            return Err(from_lmdb_err(rc_commit));
        }
        return Ok(dbi2);
    } else {
        unsafe { mdb_txn_abort(txn) };
        return Err(from_lmdb_err(rc_open));
    }
}

pub fn from_lmdb_err(code: i32) -> Box<dyn std::error::Error> {
    let cstr = unsafe { mdb_strerror(code) };
    let msg = unsafe { std::ffi::CStr::from_ptr(cstr) }.to_string_lossy().into_owned();
    Box::<dyn std::error::Error>::from(msg)
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

#[allow(clippy::missing_safety_doc)]
/// Scans archived XaeroEvents in the AOF database for a given timestamp range.
///
/// Returns a `Vec<Arc<XaeroEvent>>` where each element is a reconstructed XaeroEvent
/// for events whose keys have timestamps in `[start_ms, end_ms)`.
pub unsafe fn scan_xaero_range(
    env: &Arc<Mutex<LmdbEnv>>,
    start_ms: u64,
    end_ms: u64,
) -> anyhow::Result<Vec<Arc<XaeroEvent>>> {
    let mut results = Vec::<Arc<XaeroEvent>>::new();
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
        let mut key_val = MDB_val {
            mv_size: start_key.len(),
            mv_data: start_key.as_ptr() as *mut _,
        };
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

            // 6) Reconstruct XaeroEvent from archived data
            let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

            match crate::indexing::storage::format::unarchive_to_xaero_event(data_slice) {
                Ok(xaero_event) => {
                    results.push(xaero_event);
                    tracing::debug!("Reconstructed XaeroEvent from AOF storage");
                }
                Err(pool_error) => {
                    tracing::error!("Pool allocation failed during AOF scan: {:?}", pool_error);
                    panic!("Failed to reconstruct XaeroEvent from AOF storage: {:?}", pool_error);
                }
            }

            // 7) Advance the cursor
            let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
            if rc != 0 {
                break;
            }
        }

        // 8) Cleanup
        mdb_cursor_close(cursor);
        mdb_txn_abort(rtxn);
        Ok(results)
    }
}

/// Persists a XaeroEvent into the LMDB AOF or META database.
///
/// Behavior varies by event type:
/// - `MetaEvent(1)`: stores segment metadata under composite and static keys.
/// - `MetaEvent(2)`: stores MMR metadata under a static key.
/// - Application events: stored in the AOF database with a composite key.
pub fn push_xaero_event(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    xaero_event: &Arc<XaeroEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        let env = arc_env.lock().expect("failed to lock env");
        let mut txn = ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn);
        if sc_tx_begin != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_begin)));
        }

        let event_type = EventType::from_u8(xaero_event.event_type());

        // Handle segment_meta events specially
        if let EventType::MetaEvent(1) = event_type {
            let event_data = xaero_event.data();
            let sm: &SegmentMeta = bytemuck::from_bytes(event_data);

            // 1) Composite entry: raw SegmentMeta bytes under composite key
            let mut key_buf = [0u8; 16];
            key_buf[..8].copy_from_slice(&sm.ts_start.to_be_bytes());
            key_buf[8..16].copy_from_slice(&(sm.segment_index as u64).to_be_bytes());
            let mut key_val = MDB_val {
                mv_size: key_buf.len(),
                mv_data: key_buf.as_ptr() as *mut libc::c_void,
            };

            let mut data_val = MDB_val {
                mv_size: event_data.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc = mdb_put(
                txn,
                env.dbis[DBI::Meta as usize],
                &mut key_val,
                &mut data_val,
                MDB_RESERVE,
            );
            if sc != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
            }
            std::ptr::copy_nonoverlapping(event_data.as_ptr(), data_val.mv_data.cast(), event_data.len());

            // 2) Static entry: raw SegmentMeta bytes under "segment_meta"
            let static_key = b"segment_meta";
            let mut static_key_val = MDB_val {
                mv_size: static_key.len(),
                mv_data: static_key.as_ptr() as *mut libc::c_void,
            };
            let mut static_val = MDB_val {
                mv_size: event_data.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc2 = mdb_put(
                txn,
                env.dbis[DBI::Meta as usize],
                &mut static_key_val,
                &mut static_val,
                MDB_RESERVE,
            );
            if sc2 != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc2)));
            }
            std::ptr::copy_nonoverlapping(event_data.as_ptr(), static_val.mv_data.cast(), event_data.len());
        } else if let EventType::MetaEvent(2) = event_type {
            // Static entry: raw MMRMeta bytes under "mmr_meta"
            let static_key = b"mmr_meta";
            let mut key_val = MDB_val {
                mv_size: static_key.len(),
                mv_data: static_key.as_ptr() as *mut libc::c_void,
            };

            let event_data = xaero_event.data();
            let mut data_val = MDB_val {
                mv_size: event_data.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc = mdb_put(
                txn,
                env.dbis[DBI::Meta as usize],
                &mut key_val,
                &mut data_val,
                MDB_RESERVE,
            );
            if sc != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
            }
            std::ptr::copy_nonoverlapping(event_data.as_ptr(), data_val.mv_data.cast(), event_data.len());
        } else {
            // Application event: store using new archive format
            let key = generate_xaero_key(xaero_event)?;
            let key_bytes: &[u8] = bytemuck::bytes_of(&key);
            let mut key_val = MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut libc::c_void,
            };

            let archived_data = archive_xaero_event(xaero_event);
            let mut data_val = MDB_val {
                mv_size: archived_data.len(),
                mv_data: std::ptr::null_mut(),
            };
            let sc = mdb_put(
                txn,
                env.dbis[DBI::Aof as usize],
                &mut key_val,
                &mut data_val,
                MDB_RESERVE,
            );
            if sc != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
            }
            std::ptr::copy_nonoverlapping(archived_data.as_ptr(), data_val.mv_data.cast(), archived_data.len());
        }

        tracing::info!("Pushed XaeroEvent to LMDB: type={}", xaero_event.event_type());
        let sc_tx_commit = mdb_txn_commit(txn);
        if sc_tx_commit != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_commit)));
        }
    }
    Ok(())
}

/// Generates an `EventKey` from a XaeroEvent consisting of timestamp, type, and data hash.
pub fn generate_xaero_key(xaero_event: &Arc<XaeroEvent>) -> Result<EventKey, Failure> {
    let event_data = xaero_event.data();
    let timestamp = xaero_event.latest_ts;

    Ok(EventKey {
        ts: timestamp.to_be(),
        kind: xaero_event.event_type(),
        hash: sha_256_slice(event_data),
    })
}

/// Store a mapping from `leaf_hash` to `SegmentMeta` in the SecondaryIndex DB.
pub fn put_secondary_index(
    arc_env: &std::sync::Arc<std::sync::Mutex<LmdbEnv>>,
    leaf_hash: &[u8; 32],
    meta: &SegmentMeta,
) -> Result<(), Box<dyn std::error::Error>> {
    use bytemuck::bytes_of;
    let data = bytes_of(meta);
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(rc)));
        }

        let mut key_val = MDB_val {
            mv_size: 32,
            mv_data: leaf_hash.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: data.len(),
            mv_data: std::ptr::null_mut(),
        };
        let dbi = guard.dbis[DBI::SecondaryIndex as usize];
        let sc = mdb_put(txn, dbi, &mut key_val, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc)));
        }
        std::ptr::copy_nonoverlapping(data.as_ptr(), data_val.mv_data.cast(), data.len());

        let cc = mdb_txn_commit(txn);
        if cc != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(cc)));
        }
    }
    Ok(())
}

/// Retrieve a stored SegmentMeta for the given `leaf_hash`, if it exists.
pub fn get_secondary_index(
    arc_env: &std::sync::Arc<std::sync::Mutex<LmdbEnv>>,
    leaf_hash: &[u8; 32],
) -> Result<Option<SegmentMeta>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(rc)));
        }

        let mut key_val = MDB_val {
            mv_size: 32,
            mv_data: leaf_hash.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };
        let dbi = guard.dbis[DBI::SecondaryIndex as usize];
        let getrc = liblmdb::mdb_get(txn, dbi, &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(Box::new(std::io::Error::from_raw_os_error(getrc)));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let meta: &SegmentMeta = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);
        Ok(Some(*meta))
    }
}

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytemuck::bytes_of;
    use tempfile::tempdir;
    use xaeroflux_core::{
        XaeroPoolManager,
        date_time::{MS_PER_DAY, day_bounds_from_epoch_ms, emit_secs},
        event::{EventType, XaeroEvent},
        initialize,
    };

    use super::*;
    use crate::aof::storage::{
        format::MMRMeta,
        lmdb::{LmdbEnv, get_secondary_index, put_secondary_index},
        meta::{get_meta_val, iterate_segment_meta_by_range},
    };

    #[test]
    fn test_put_and_get_secondary_index() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data).expect("failed_to_unravel"),
        ));

        let data = b"payload".to_vec();
        let leaf_hash = sha_256(&data);
        let meta = SegmentMeta {
            page_index: 1,
            segment_index: 2,
            write_pos: 100,
            byte_offset: 200,
            latest_segment_id: 3,
            ts_start: 1000,
            ts_end: 2000,
        };

        put_secondary_index(&arc_env, &leaf_hash, &meta).expect("put_secondary_index");
        let unaligned_m_pid = meta.page_index;
        let got = get_secondary_index(&arc_env, &leaf_hash)
            .expect("get_secondary_index")
            .expect("meta missing");
        let unaligned_g_pidx = got.page_index;
        assert_eq!(unaligned_m_pid, unaligned_g_pidx, "page_index mismatch");
    }

    #[test]
    fn test_get_secondary_index_not_found() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data).expect("failed_to_unravel"),
        ));

        let leaf_hash = [0u8; 32];
        let got = get_secondary_index(&arc_env, &leaf_hash).expect("get_secondary_index");
        assert!(got.is_none());
    }

    #[test]
    fn test_env_creation_and_dbi_handles() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed to unravel");
        let env =
            LmdbEnv::new(dir.path().to_str().expect("failed to unravel"), BusKind::Control).expect("failed to unravel");

        assert!(!env.env.is_null());
        assert!(env.dbis[0] > 0);
        assert!(env.dbis[1] > 0);
    }

    #[test]
    fn test_generate_xaero_key_consistency() {
        initialize();
        XaeroPoolManager::init();

        let data = b"hello".to_vec();
        let timestamp = 123_456_789;
        let event_type = EventType::ApplicationEvent(1).to_u8();

        let xaero_event = XaeroPoolManager::create_xaero_event(&data, event_type, None, None, None, timestamp)
            .expect("Failed to create XaeroEvent");

        let key = generate_xaero_key(&xaero_event).expect("failed to unravel");
        let bytes = bytes_of(&key);

        tracing::info!("key bytes: {:?}", bytes);
        assert_eq!(
            u64::from_be_bytes(bytes[0..8].try_into().expect("failed to unravel")),
            timestamp
        );
        assert_eq!(bytes[8], event_type);
        assert_eq!(bytes.len(), std::mem::size_of::<EventKey>());
    }

    #[test]
    fn test_push_and_scan_xaero_events() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed to unravel");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to unravel"), BusKind::Data).expect("failed to unravel"),
        ));

        // Create two XaeroEvents
        let e1 = XaeroPoolManager::create_xaero_event(
            b"one",
            EventType::ApplicationEvent(1).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .expect("Failed to create event 1");

        let e2 = XaeroPoolManager::create_xaero_event(
            b"two",
            EventType::ApplicationEvent(2).to_u8(),
            None,
            None,
            None,
            emit_secs(),
        )
        .expect("Failed to create event 2");

        push_xaero_event(&arc_env, &e1).expect("failed to push e1");
        push_xaero_event(&arc_env, &e2).expect("failed to push e2");

        // Compute current day bounds
        let (start, end) = day_bounds_from_epoch_ms(e1.latest_ts);

        // Scan today: should see at least 2
        let events = unsafe { scan_xaero_range(&arc_env, start, end).expect("failed to scan") };
        assert!(events.len() >= 2, "expected >=2 events, got {}", events.len());

        // Verify event data
        assert!(events.iter().any(|e| e.data() == b"one"));
        assert!(events.iter().any(|e| e.data() == b"two"));

        // Scan next day: should be empty
        let tomorrow_start = end;
        let tomorrow_end = tomorrow_start + MS_PER_DAY;
        let events2 = unsafe { scan_xaero_range(&arc_env, tomorrow_start, tomorrow_end).expect("failed to scan") };
        assert_eq!(events2.len(), 0, "expected 0 events, got {}", events2.len());
    }

    #[test]
    fn test_segment_meta_with_xaero_events() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unwrap");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unwrap"), BusKind::Control).expect("failed_to_unwrap"),
        ));

        let meta = SegmentMeta {
            page_index: 1,
            segment_index: 2,
            write_pos: 128,
            byte_offset: 256,
            latest_segment_id: 3,
            ts_start: emit_secs(),
            ts_end: emit_secs(),
        };

        let data = bytemuck::bytes_of(&meta);
        let xaero_event =
            XaeroPoolManager::create_xaero_event(data, EventType::MetaEvent(1).to_u8(), None, None, None, emit_secs())
                .expect("Failed to create meta event");

        push_xaero_event(&arc_env, &xaero_event).expect("failed to push meta");

        // Read back using meta interface
        let raw = unsafe { get_meta_val(&arc_env, b"segment_meta") };
        let got: &SegmentMeta = bytemuck::from_bytes(&raw);
        // Declare unaligned field accesses
        let unaligned_page_index = got.page_index;
        let unaligned_byte_offset = got.byte_offset;
        let unaligned_write_pos = got.write_pos;
        let unaligned_segment_index = got.segment_index;
        let unaligned_latest_segment_id = got.latest_segment_id;

        // Perform assertions using unaligned values
        // Declare unaligned field accesses for got
        let unaligned_got_page_index = got.page_index;
        let unaligned_got_byte_offset = got.byte_offset;
        let unaligned_got_write_pos = got.write_pos;
        let unaligned_got_segment_index = got.segment_index;
        let unaligned_got_latest_segment_id = got.latest_segment_id;

        // Declare unaligned field accesses for meta
        let unaligned_meta_page_index = meta.page_index;
        let unaligned_meta_byte_offset = meta.byte_offset;
        let unaligned_meta_write_pos = meta.write_pos;
        let unaligned_meta_segment_index = meta.segment_index;
        let unaligned_meta_latest_segment_id = meta.latest_segment_id;

        // Perform assertions using unaligned values
        assert_eq!(unaligned_got_page_index, unaligned_meta_page_index);
        assert_eq!(unaligned_got_byte_offset, unaligned_meta_byte_offset);
        assert_eq!(unaligned_got_write_pos, unaligned_meta_write_pos);
        assert_eq!(unaligned_got_segment_index, unaligned_meta_segment_index);
        assert_eq!(unaligned_got_latest_segment_id, unaligned_meta_latest_segment_id);
    }
}
