#[allow(deprecated)]
use std::{
    ffi::CString,
    io::{Error, ErrorKind},
    ptr,
    sync::{Arc, Mutex},
};

use liblmdb::{
    mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_dbi_close, mdb_dbi_open, mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_put, mdb_strerror, mdb_txn_abort, mdb_txn_begin,
    mdb_txn_commit, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn, MDB_val, MDB_CREATE, MDB_NOTFOUND, MDB_RDONLY, MDB_RESERVE, MDB_SUCCESS,
};
use rkyv::{rancor::Failure, util::AlignedVec};
use rusted_ring_new::{EventPoolFactory, EventUtils};
use xaeroflux_core::{
    date_time::emit_secs,
    event::{EventType, XaeroEvent},
    hash::{blake_hash_slice, sha_256, sha_256_slice},
    pool::XaeroInternalEvent,
};

use super::format::{EventKey, MMRMeta};

#[repr(usize)]
pub enum DBI {
    Aof = 0,
    Meta = 1,
    HashIndex = 2,  // NEW: Hash index database
}

/// A wrapper around an LMDB environment with three databases: AOF, META, and HASH_INDEX.
///
/// - AOF_DB stores Arc<XaeroEvent> data keyed by timestamp, event type, and hash.
/// - META_DB stores MMR metadata for efficient lookup.
/// - HASH_INDEX_DB stores hash → EventKey mappings for O(1) leaf hash lookups.
pub struct LmdbEnv {
    pub env: *mut MDB_env,
    pub dbis: [MDB_dbi; 3],  // CHANGED: Now 3 databases
}

unsafe impl Sync for LmdbEnv {}
unsafe impl Send for LmdbEnv {}

impl LmdbEnv {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
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
            tracing::info!("Setting max DBs to 3");  // CHANGED: Now 3 DBs
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

        let aof_dbi = unsafe { open_named_db(env, c"/aof".as_ptr())? };
        let meta_dbi = unsafe { open_named_db(env, c"/meta".as_ptr())? };
        let hash_index_dbi = unsafe { open_named_db(env, c"/hash_index".as_ptr())? };  // NEW

        Ok(Self { env, dbis: [aof_dbi, meta_dbi, hash_index_dbi] })  // CHANGED
    }
}

/// Opens or creates a named database in the LMDB environment.
pub unsafe fn open_named_db(env: *mut MDB_env, name_ptr: *const i8) -> Result<MDB_dbi, Box<dyn std::error::Error>> {
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

/// Generates event key that consists of :
/// - `timestamp`
/// - `even_type` (see `EventType`)
/// - `sha_256_slice` hash of event_data
/// to uniquely identify an event.
pub fn generate_event_key(event_data: &[u8], event_type: u32, timestamp: u64, xaero_id_hash: [u8; 32], vector_clock_hash: [u8; 32]) -> EventKey {
    EventKey {
        xaero_id_hash,
        vector_clock_hash,
        ts: timestamp.to_be(),
        kind: event_type as u8,
        hash: blake_hash_slice(event_data),
    }
}

// ================================================================================================
// NEW: HASH INDEX FUNCTIONS
// ================================================================================================

/// Store hash → EventKey mapping in HASH_INDEX_DB for O(1) lookups
pub fn put_hash_index(arc_env: &Arc<Mutex<LmdbEnv>>, event_hash: [u8; 32], event_key: &EventKey) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        // Key: 32-byte event hash
        let mut key_val = MDB_val {
            mv_size: event_hash.len(),
            mv_data: event_hash.as_ptr() as *mut _,
        };

        // Value: EventKey struct
        let key_bytes = bytemuck::bytes_of(event_key);
        let mut data_val = MDB_val {
            mv_size: key_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };

        let sc = mdb_put(txn, guard.dbis[DBI::HashIndex as usize], &mut key_val, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        std::ptr::copy_nonoverlapping(key_bytes.as_ptr(), data_val.mv_data.cast(), key_bytes.len());

        let cc = mdb_txn_commit(txn);
        if cc != 0 {
            return Err(from_lmdb_err(cc));
        }

        tracing::debug!("Stored hash index: {} → EventKey", hex::encode(event_hash));
    }
    Ok(())
}

/// Get EventKey for a specific event hash (O(1) lookup)
pub fn get_event_key_by_hash(arc_env: &Arc<Mutex<LmdbEnv>>, event_hash: [u8; 32]) -> Result<Option<EventKey>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: event_hash.len(),
            mv_data: event_hash.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::HashIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let event_key: &EventKey = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);

        tracing::debug!("Found event key for hash: {}", hex::encode(event_hash));
        Ok(Some(*event_key))
    }
}

/// Get event data directly by hash (O(1) lookup) - uses hash index then fetches event
pub fn get_event_by_hash<const TSHIRT_SIZE: usize>(arc_env: &Arc<Mutex<LmdbEnv>>, event_hash: [u8; 32]) -> Result<Option<XaeroInternalEvent<TSHIRT_SIZE>>, Box<dyn std::error::Error>> {
    // 1. Look up EventKey by hash
    let event_key = match get_event_key_by_hash(arc_env, event_hash)? {
        Some(key) => key,
        None => return Ok(None),
    };

    // 2. Fetch event data using EventKey
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let key_bytes = bytemuck::bytes_of(&event_key);
        let mut key_val = MDB_val {
            mv_size: key_bytes.len(),
            mv_data: key_bytes.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::Aof as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

        // Check size matches
        if data_slice.len() == std::mem::size_of::<XaeroInternalEvent<TSHIRT_SIZE>>() {
            match bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT_SIZE>>(data_slice) {
                Ok(internal_event) => {
                    mdb_txn_abort(txn);
                    return Ok(Some(*internal_event));
                }
                Err(_) => {
                    // Handle alignment issues
                    let mut aligned_buffer: std::mem::MaybeUninit<XaeroInternalEvent<TSHIRT_SIZE>> = std::mem::MaybeUninit::uninit();
                    std::ptr::copy_nonoverlapping(data_slice.as_ptr(), aligned_buffer.as_mut_ptr() as *mut u8, data_slice.len());
                    let internal_event = aligned_buffer.assume_init();
                    mdb_txn_abort(txn);
                    return Ok(Some(internal_event));
                }
            }
        }

        mdb_txn_abort(txn);
        Ok(None)
    }
}

// ================================================================================================
// EXISTING FUNCTIONS (MODIFIED TO UPDATE HASH INDEX)
// ================================================================================================

/// Push XaeroInternalEvent directly using bytemuck - no headers needed!
/// MODIFIED: Now also updates hash index
pub fn push_xaero_internal_event<const TSHIRT_SIZE: usize>(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_event: &XaeroInternalEvent<TSHIRT_SIZE>) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        let env = arc_env.lock().expect("failed to lock env");
        let mut txn = ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn);
        if sc_tx_begin != 0 {
            return Err(from_lmdb_err(sc_tx_begin));
        }

        // Generate enhanced key with peer and vector clock hashes from the event
        let event_data = &xaero_event.evt.data[..xaero_event.evt.len as usize];
        let key = generate_event_key(
            event_data,
            xaero_event.evt.event_type,
            xaero_event.latest_ts,
            xaero_event.xaero_id_hash,
            xaero_event.vector_clock_hash,
        );

        let key_bytes: &[u8] = bytemuck::bytes_of(&key);
        let mut key_val = MDB_val {
            mv_size: key_bytes.len(),
            mv_data: key_bytes.as_ptr() as *mut libc::c_void,
        };

        // Store the entire XaeroInternalEvent using bytemuck
        let event_bytes: &[u8] = bytemuck::bytes_of(xaero_event);
        let mut data_val = MDB_val {
            mv_size: event_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };

        let sc = mdb_put(txn, env.dbis[DBI::Aof as usize], &mut key_val, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        // Copy the event bytes directly
        std::ptr::copy_nonoverlapping(event_bytes.as_ptr(), data_val.mv_data.cast(), event_bytes.len());

        // NEW: Also update hash index (hash → EventKey mapping)
        let event_hash = blake_hash_slice(event_data);
        let hash_key_bytes = event_hash;
        let mut hash_key_val = MDB_val {
            mv_size: hash_key_bytes.len(),
            mv_data: hash_key_bytes.as_ptr() as *mut libc::c_void,
        };

        let event_key_bytes = bytemuck::bytes_of(&key);
        let mut hash_data_val = MDB_val {
            mv_size: event_key_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };

        let hash_sc = mdb_put(txn, env.dbis[DBI::HashIndex as usize], &mut hash_key_val, &mut hash_data_val, MDB_RESERVE);
        if hash_sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(hash_sc));
        }

        std::ptr::copy_nonoverlapping(event_key_bytes.as_ptr(), hash_data_val.mv_data.cast(), event_key_bytes.len());

        tracing::debug!(
            "Pushed XaeroInternalEvent to LMDB: type={}, size={} bytes, ts={}, hash={}",
            xaero_event.evt.event_type,
            event_bytes.len(),
            xaero_event.latest_ts,
            hex::encode(event_hash)
        );

        let sc_tx_commit = mdb_txn_commit(txn);
        if sc_tx_commit != 0 {
            return Err(from_lmdb_err(sc_tx_commit));
        }
    }
    Ok(())
}

/// Universal push function that estimates size and creates appropriate XaeroInternalEvent
pub fn push_internal_event_universal(arc_env: &Arc<Mutex<LmdbEnv>>, event_data: &[u8], event_type: u32, timestamp: u64) -> Result<(), Box<dyn std::error::Error>> {
    // Use ring buffer library to estimate appropriate size
    let estimated_size = EventPoolFactory::estimate_size(event_data.len());

    match estimated_size {
        rusted_ring_new::EventSize::XS => {
            let pooled_event = EventUtils::create_pooled_event::<64>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<64> {
                xaero_id_hash: [0u8; 32],     // Empty for universal events
                vector_clock_hash: [0u8; 32], // Empty for universal events
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring_new::EventSize::S => {
            let pooled_event = EventUtils::create_pooled_event::<256>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<256> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring_new::EventSize::M => {
            let pooled_event = EventUtils::create_pooled_event::<1024>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<1024> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring_new::EventSize::L => {
            let pooled_event = EventUtils::create_pooled_event::<4096>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<4096> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring_new::EventSize::XL => {
            let pooled_event = EventUtils::create_pooled_event::<16384>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<16384> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring_new::EventSize::XXL => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Event data too large: {} bytes exceeds maximum size", event_data.len()),
            )));
        }
    }
}

// ================================================================================================
// MMR METADATA STORAGE FUNCTIONS
// ================================================================================================

/// Store MMR metadata in META DB using a fixed key
pub fn put_mmr_meta(arc_env: &Arc<Mutex<LmdbEnv>>, mmr_meta: &MMRMeta) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        // Use a fixed key for MMR metadata
        let mmr_key = b"current_mmr_state";
        let mut key_val = MDB_val {
            mv_size: mmr_key.len(),
            mv_data: mmr_key.as_ptr() as *mut _,
        };

        let meta_bytes = bytemuck::bytes_of(mmr_meta);
        let mut data_val = MDB_val {
            mv_size: meta_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };

        let sc = mdb_put(txn, guard.dbis[DBI::Meta as usize], &mut key_val, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        std::ptr::copy_nonoverlapping(meta_bytes.as_ptr(), data_val.mv_data.cast(), meta_bytes.len());

        let cc = mdb_txn_commit(txn);
        if cc != 0 {
            return Err(from_lmdb_err(cc));
        }
        let lc = mmr_meta.leaf_count;
        let pc = mmr_meta.peaks_count;
        tracing::debug!("Stored MMR metadata: {} leaves, {} peaks", lc, pc);
    }
    Ok(())
}

/// Get current MMR metadata from META DB
pub fn get_mmr_meta(arc_env: &Arc<Mutex<LmdbEnv>>) -> Result<Option<MMRMeta>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mmr_key = b"current_mmr_state";
        let mut key_val = MDB_val {
            mv_size: mmr_key.len(),
            mv_data: mmr_key.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::Meta as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let meta: &MMRMeta = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);

        let lc = meta.leaf_count;
        let pc = meta.peaks_count;
        tracing::debug!("Stored MMR metadata: {} leaves, {} peaks", lc, pc);

        Ok(Some(*meta))
    }
}

#[allow(clippy::missing_safety_doc)]
/// Enhanced scan function that works with the new EventKey format and XaeroInternalEvent storage
///
/// Scans events with enhanced keys that include peer ID and vector clock hashes.
/// The new key format is: [xaero_id_hash: 32][vector_clock_hash: 32][ts: 8][kind: 1][hash: 32]
///
/// Returns events where the timestamp field (at offset 64) is in the range [start_ts, end_ts)
pub unsafe fn scan_enhanced_range<const TSHIRT_SIZE: usize>(env: &Arc<Mutex<LmdbEnv>>, start_ts: u64, end_ts: u64) -> anyhow::Result<Vec<XaeroInternalEvent<TSHIRT_SIZE>>> {
    let mut results = Vec::<XaeroInternalEvent<TSHIRT_SIZE>>::new();
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

        // 3) Start from the first key and iterate through all
        let mut key_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        // 4) Position to first key
        let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, liblmdb::MDB_cursor_op_MDB_FIRST);
        if rc != 0 {
            mdb_cursor_close(cursor);
            mdb_txn_abort(rtxn);
            tracing::debug!("Enhanced scan: No first key found");
            return Ok(results); // No events found
        }

        loop {
            // 5) Extract the timestamp from enhanced key format
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);

            // Ensure we have enough bytes for the enhanced key
            if raw_key.len() < std::mem::size_of::<EventKey>() {
                let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
                if rc != 0 {
                    break;
                }
                continue;
            }

            // Extract timestamp from offset 64 (after the two 32-byte hash fields)
            let ts_bytes: [u8; 8] = raw_key[64..72].try_into().map_err(|_| anyhow::anyhow!("Failed to extract timestamp from key"))?;
            let ts = u64::from_be_bytes(ts_bytes);

            if ts < start_ts {
                let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
                if rc != 0 {
                    break;
                }
                continue;
            }

            if ts >= end_ts {
                break; // Past our range
            }

            // 6) Try to reconstruct XaeroInternalEvent from stored data
            let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

            // Check if the data size matches XaeroInternalEvent<TSHIRT_SIZE>
            if data_slice.len() == std::mem::size_of::<XaeroInternalEvent<TSHIRT_SIZE>>() {
                // Safe conversion handling alignment
                match bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT_SIZE>>(data_slice) {
                    Ok(internal_event) => {
                        results.push(*internal_event);
                    }
                    Err(_) => {
                        // Copy to properly aligned buffer
                        let mut aligned_buffer: std::mem::MaybeUninit<XaeroInternalEvent<TSHIRT_SIZE>> = std::mem::MaybeUninit::uninit();
                        unsafe {
                            std::ptr::copy_nonoverlapping(data_slice.as_ptr(), aligned_buffer.as_mut_ptr() as *mut u8, data_slice.len());
                            let internal_event = aligned_buffer.assume_init();
                            results.push(internal_event);
                        }
                    }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;
    use xaeroflux_core::{date_time::emit_secs, initialize, hash::blake_hash_slice};

    use super::*;

    #[test]
    fn test_hash_index_functionality() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().expect("failed to get path")).expect("failed to create env")));

        // Create test event
        let event_data = b"test hash index event";
        let event_hash = blake_hash_slice(event_data);
        let timestamp = emit_secs();

        // Push event (automatically creates hash index)
        push_internal_event_universal(&arc_env, event_data, 123, timestamp).expect("Failed to push event");

        // Test hash index lookup
        let found_key = get_event_key_by_hash(&arc_env, event_hash).expect("Hash lookup failed");
        assert!(found_key.is_some(), "Event key should be found by hash");

        let event_key = found_key.unwrap();
        assert_eq!(event_key.hash, event_hash);
        assert_eq!(event_key.kind, 123);

        // Test direct event lookup by hash
        let found_event = get_event_by_hash::<64>(&arc_env, event_hash).expect("Event lookup by hash failed");
        assert!(found_event.is_some(), "Event should be found by hash");

        let event = found_event.unwrap();
        let retrieved_data = &event.evt.data[..event.evt.len as usize];
        assert_eq!(retrieved_data, event_data);

        println!("✅ Hash index functionality working correctly");
    }

    #[test]
    fn test_mmr_metadata_storage() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().expect("failed to get path")).expect("failed to create env")));

        // Create test MMR metadata
        let mmr_meta = MMRMeta {
            root_hash: [42u8; 32],
            peaks_count: 5,
            leaf_count: 1000,
            segment_meta: Default::default(),
        };

        // Test storage
        put_mmr_meta(&arc_env, &mmr_meta).expect("Failed to store MMR metadata");

        // Test retrieval
        let retrieved = get_mmr_meta(&arc_env).expect("Failed to get MMR metadata").expect("MMR metadata should exist");

        assert_eq!(retrieved.root_hash, mmr_meta.root_hash);
        let rpc = retrieved.peaks_count;
        let mpc = mmr_meta.peaks_count;
        let rlc = retrieved.leaf_count;
        let mlc = mmr_meta.leaf_count;
        assert_eq!(rpc, mpc);
        assert_eq!(rlc, mlc);

        println!("✅ MMR metadata storage and retrieval working");
    }

    #[test]
    fn test_env_creation_and_dbi_handles() {
        initialize();

        let dir = tempdir().expect("failed to unravel");
        let env = LmdbEnv::new(dir.path().to_str().expect("failed to unravel")).expect("failed to unravel");

        assert!(!env.env.is_null());
        assert!(env.dbis[0] > 0);  // AOF
        assert!(env.dbis[1] > 0);  // META
        assert!(env.dbis[2] > 0);  // HASH_INDEX
    }

    #[test]
    fn test_universal_push_and_enhanced_scan() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().expect("failed to get path")).expect("failed to create env")));

        // Test universal push with enhanced scan
        let base_timestamp = emit_secs();

        // Push events using universal push function
        push_internal_event_universal(&arc_env, b"universal_one", 101, base_timestamp).expect("Failed to push universal event 1");

        push_internal_event_universal(&arc_env, b"universal_two", 102, base_timestamp + 1).expect("Failed to push universal event 2");

        // Test enhanced range scan with XS size (since our data is small)
        let scan_start = base_timestamp - 5;
        let scan_end = base_timestamp + 5;

        let events = unsafe { scan_enhanced_range::<64>(&arc_env, scan_start, scan_end).expect("Enhanced scan failed") };

        assert!(events.len() >= 2, "Expected at least 2 events from enhanced scan, got {}", events.len());

        // Verify event data by checking the pooled event data
        let mut found_one = false;
        let mut found_two = false;

        for event in &events {
            let event_data = &event.evt.data[..event.evt.len as usize];
            if event_data == b"universal_one" {
                found_one = true;
            }
            if event_data == b"universal_two" {
                found_two = true;
            }
        }

        assert!(found_one, "Missing event 'universal_one'");
        assert!(found_two, "Missing event 'universal_two'");

        println!("✅ Universal push and enhanced scan working together");
    }
}