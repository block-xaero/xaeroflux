use std::slice;
#[allow(deprecated)]
use std::{
    ffi::CString,
    io::{Error, ErrorKind},
    ptr,
    sync::{Arc, Mutex},
};

use iroh::NodeId;
use liblmdb::{
    MDB_CREATE, MDB_NODUPDATA, MDB_NOTFOUND, MDB_RDONLY, MDB_RESERVE, MDB_SUCCESS, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn, MDB_val, mdb_cursor_close, mdb_cursor_get,
    mdb_cursor_open, mdb_dbi_close, mdb_dbi_open, mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_get, mdb_put, mdb_strerror, mdb_txn_abort,
    mdb_txn_begin, mdb_txn_commit,
};
use rkyv::{rancor::Failure, util::AlignedVec};
use rusted_ring::{EventPoolFactory, EventUtils};
use xaeroflux_core::{
    date_time::emit_secs,
    event::{EventType, XaeroEvent, get_base_event_type, is_create_event, is_pinned_event, is_update_event},
    hash::{blake_hash_slice, sha_256, sha_256_slice},
    pool::XaeroInternalEvent,
};
use xaeroid::{XaeroID, cache::xaero_id_hash};
use xaeroflux_core::vector_clock::XaeroVectorClock;
use super::format::{EventKey, MmrMeta};
use crate::read_api::PointQuery;

#[repr(usize)]
pub enum DBI {
    AofIndex = 0,
    MetaIndex = 1,
    HashIndex = 2, // Leaf hash index db
    VectorClockIndex = 3,
    XaeroIdNodeIdIndex = 4,
    XaeroIdIndex = 5,
    CurrentStateIndex = 6,
    RelationsIndex = 7,
}

/// A wrapper around an LMDB environment with three databases: AOF, META, and HASH_INDEX.
///
/// - AOF_DB stores Arc<XaeroEvent> data keyed by timestamp, event type, and hash.
/// - META_DB stores MMR metadata for efficient lookup.
/// - HASH_INDEX_DB stores hash → EventKey mappings for O(1) leaf hash lookups.
pub struct LmdbEnv {
    pub env: *mut MDB_env,
    pub dbis: [MDB_dbi; 8],
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

        let mut env = ptr::null_mut();
        unsafe {
            tracing::info!("Creating LMDB environment at {}", path);
            let sc_create_env = mdb_env_create(&mut env);
            println!("DEBUG: mdb_env_create = {}", sc_create_env); // ADD THIS
            if sc_create_env != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_create_env)));
            }

            tracing::info!("Setting max DBs to 8");
            let sc_set_max_dbs = mdb_env_set_maxdbs(env, 8);
            println!("DEBUG: mdb_env_set_maxdbs = {}", sc_set_max_dbs); // ADD THIS
            if sc_set_max_dbs != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_set_max_dbs)));
            }

            tracing::info!("Setting mapsize to 1MB for testing");
            let sc_set_mapsize = mdb_env_set_mapsize(env, 1 << 20); // Use 1MB instead of 1GB
            println!("DEBUG: mdb_env_set_mapsize = {}", sc_set_mapsize); // ADD THIS
            if sc_set_mapsize != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_set_mapsize)));
            }

            let cs = CString::new(path)?;
            let sc_env_open = mdb_env_open(env, cs.as_ptr(), MDB_CREATE, 0o600);
            println!("DEBUG: mdb_env_open = {}", sc_env_open); // ADD THIS
            if sc_env_open != 0 {
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_env_open)));
            }
        }

        println!("DEBUG: About to open named databases"); // ADD THIS
        let aof_dbi = unsafe { open_named_db(env, c"/aof_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened aof_dbi = {}", aof_dbi); // ADD THIS
        let meta_dbi = unsafe { open_named_db(env, c"/meta_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened meta_dbi = {}", meta_dbi); // ADD THIS
        let hash_index_dbi = unsafe { open_named_db(env, c"/hash_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened hash_index_dbi = {}", hash_index_dbi); // ADD THIS
        let vector_clock_index_dbi = unsafe { open_named_db(env, c"/vector_clock_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened vector_clock_index_dbi = {}", vector_clock_index_dbi);
        let xaero_id_node_id_index_dbi = unsafe { open_named_db(env, c"/xaero_id_node_id_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened xaero_id_node_id_index_dbi = {xaero_id_node_id_index_dbi:?}");
        let xaero_id_index_dbi = unsafe { open_named_db(env, c"/xaero_id_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened xaero_id_index_dbi = {}", xaero_id_index_dbi);
        let current_state_idx_dbi = unsafe { open_named_db(env, c"/aof_current_state_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened current_state_idx_dbu = {}", current_state_idx_dbi);
        let relations_idx_dbi = unsafe { open_named_db(env, c"/relations_index".as_ptr())? };
        tracing::debug!("DEBUG: Opened relations_idx_dbis = {}", relations_idx_dbi);
        Ok(Self {
            env,
            dbis: [
                aof_dbi,
                meta_dbi,
                hash_index_dbi,
                vector_clock_index_dbi,
                xaero_id_index_dbi,
                xaero_id_index_dbi,
                current_state_idx_dbi,
                relations_idx_dbi,
            ],
        })
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
        Ok(dbi2)
    } else {
        unsafe { mdb_txn_abort(txn) };
        Err(from_lmdb_err(rc_open))
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

pub fn put_xaero_id(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_id: XaeroID) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;
    let xaero_id_bytes = bytemuck::bytes_of::<XaeroID>(&xaero_id);
    let xaero_id_hash_calculated = blake_hash_slice(xaero_id_bytes);
    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        // Key: 32-byte event hash
        let mut xaero_id_hash_mdb_val = MDB_val {
            mv_size: xaero_id_hash_calculated.len(),
            mv_data: xaero_id_hash_calculated.as_ptr() as *mut _,
        };

        // value: node_id
        let mut xaero_id_bytes_mdb_val = MDB_val {
            mv_size: xaero_id_bytes.len(),
            mv_data: xaero_id_bytes.as_ptr() as *mut _,
        };

        let sc = mdb_put(
            txn,
            guard.dbis[DBI::XaeroIdIndex as usize],
            &mut xaero_id_hash_mdb_val,
            &mut xaero_id_bytes_mdb_val,
            MDB_NODUPDATA,
        );
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }
    }
    Ok(xaero_id_hash_calculated)
}

pub fn get_xaero_id_by_xaero_id_hash(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_id_hash: [u8; 32]) -> Result<Option<&XaeroID>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: xaero_id_hash.len(),
            mv_data: xaero_id_hash.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::XaeroIdIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let xaero_id_found = bytemuck::from_bytes::<XaeroID>(slice);
        mdb_txn_abort(txn);

        tracing::debug!("Found xaero_id : {xaero_id_found:?} from provided  xaero_id_hash: {:?}", hex::encode(xaero_id_hash));
        Ok(Some(xaero_id_found))
    }
}

pub fn put_xaero_id_to_node_id(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_id: [u8; 32], node_id: &[u8; 32]) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        // Key: 32-byte event hash
        let mut xaero_id_hash = MDB_val {
            mv_size: xaero_id.len(),
            mv_data: xaero_id.as_ptr() as *mut _,
        };

        // value: node_id
        let node_id_bytes = bytemuck::bytes_of(node_id);
        let mut node_id_val = MDB_val {
            mv_size: node_id_bytes.len(),
            mv_data: node_id_bytes.as_ptr() as *mut _,
        };

        let sc = mdb_put(txn, guard.dbis[DBI::XaeroIdIndex as usize], &mut xaero_id_hash, &mut node_id_val, MDB_NODUPDATA);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }
    }
    Ok(())
}

pub fn get_node_id_by_xaero_id(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_id_hash: [u8; 32]) -> Result<Option<[u8; 32]>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: xaero_id_hash.len(),
            mv_data: xaero_id_hash.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::XaeroIdIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let node_id_hash: &[u8; 32] = slice[0..32].try_into()?;
        mdb_txn_abort(txn);

        tracing::debug!("Found node_id_hash : {:?} from provided : {:?}", hex::encode(node_id_hash), hex::encode(xaero_id_hash));
        Ok(Some(*node_id_hash))
    }
}

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
pub fn get_event_by_hash<const TSHIRT_SIZE: usize>(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    event_hash: [u8; 32],
) -> Result<Option<XaeroInternalEvent<TSHIRT_SIZE>>, Box<dyn std::error::Error>> {
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

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::AofIndex as usize], &mut key_val, &mut data_val);
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
        if !is_pinned_event(xaero_event.evt.event_type) {
            panic!(
                "This was an unpinned event you sent to lmdb - make sure your events are
            pinned, see `make_pinned` in xaeroflux_core crate"
            ); // this should
            // not & cannot happen!
        }
        let event_type = get_base_event_type(xaero_event.evt.event_type);
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

        let sc = mdb_put(txn, env.dbis[DBI::AofIndex as usize], &mut key_val, &mut data_val, MDB_RESERVE);
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

        if is_create_event(event_type) || is_update_event(event_type) {
            // Extract IDs
            let mut entity_id = [0u8; 32];
            entity_id.copy_from_slice(&event_data[..32]);
            let mut parent_id = [0u8; 32];
            parent_id.copy_from_slice(&event_data[32..64]);

            // Update CurrentState
            let mut entity_id_key = MDB_val {
                mv_size: entity_id.len(),
                mv_data: entity_id.as_ptr() as *mut libc::c_void,
            };
            let mut entity_data_val = MDB_val {
                mv_size: event_data.len(), // Store just entity data, not wrapper
                mv_data: event_data.as_ptr() as *mut libc::c_void,
            };

            let sc = mdb_put(txn, env.dbis[DBI::CurrentStateIndex as usize], &mut entity_id_key, &mut entity_data_val, 0);
            if sc != 0 {
                mdb_txn_abort(txn);
                return Err(from_lmdb_err(sc));
            }

            // Update Relations if has parent
            if parent_id != [0u8; 32] {
                let mut parent_key = MDB_val {
                    mv_size: parent_id.len(),
                    mv_data: parent_id.as_ptr() as *mut libc::c_void,
                };

                // Get existing relations
                let mut existing_val = MDB_val {
                    mv_size: 0,
                    mv_data: std::ptr::null_mut(),
                };

                let mut entities_buffer = Vec::new();
                let get_sc = mdb_get(txn, env.dbis[DBI::RelationsIndex as usize], &mut parent_key, &mut existing_val);

                if get_sc == 0 {
                    // Found existing relations
                    let existing = slice::from_raw_parts(existing_val.mv_data as *const u8, existing_val.mv_size);

                    // Check if already present
                    let already_present = existing.chunks_exact(32).any(|chunk| chunk == entity_id);

                    if !already_present {
                        entities_buffer.extend_from_slice(existing);
                        entities_buffer.extend_from_slice(&entity_id);
                    }
                } else if get_sc == MDB_NOTFOUND {
                    // First child
                    entities_buffer.extend_from_slice(&entity_id);
                } else {
                    mdb_txn_abort(txn);
                    return Err(from_lmdb_err(get_sc));
                }

                // Write back
                if !entities_buffer.is_empty() {
                    let mut new_val = MDB_val {
                        mv_size: entities_buffer.len(),
                        mv_data: entities_buffer.as_ptr() as *mut libc::c_void,
                    };

                    let put_sc = mdb_put(txn, env.dbis[DBI::RelationsIndex as usize], &mut parent_key, &mut new_val, 0);
                    if put_sc != 0 {
                        mdb_txn_abort(txn);
                        return Err(from_lmdb_err(put_sc));
                    }
                }
            }
        }

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
        rusted_ring::EventSize::XS => {
            let pooled_event = EventUtils::create_pooled_event::<64>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<64> {
                xaero_id_hash: [0u8; 32],     // Empty for universal events
                vector_clock_hash: [0u8; 32], // Empty for universal events
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::S => {
            let pooled_event = EventUtils::create_pooled_event::<256>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<256> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::M => {
            let pooled_event = EventUtils::create_pooled_event::<1024>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<1024> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::L => {
            let pooled_event = EventUtils::create_pooled_event::<4096>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<4096> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::XL => {
            let pooled_event = EventUtils::create_pooled_event::<16384>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<16384> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::XXL => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Event data too large: {} bytes exceeds maximum size", event_data.len()),
        ))),
    }
}

pub fn encode_peer_timestamp_key(peer_id: [u8; 32], logical_ts: u64) -> [u8; 40] {
    let mut key = [0u8; 40];
    key[0..32].copy_from_slice(&peer_id);
    key[32..40].copy_from_slice(&logical_ts.to_be_bytes());
    key
}

///  clock_blake_hash -> clock
pub fn put_vector_clock_meta(arc_env: &Arc<Mutex<LmdbEnv>>, vector_clock: &XaeroVectorClock) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let clock_bytes = bytemuck::bytes_of(vector_clock);
    let vector_clock_key = blake_hash_slice(clock_bytes);
    if get_vector_clock_meta(arc_env, vector_clock_key)?.is_some() {
        return Ok(vector_clock_key); // Already stored
    }
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;
    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }
        let mut mdb_key = MDB_val {
            mv_size: vector_clock_key.len(),
            mv_data: vector_clock_key.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: clock_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };
        let sc = mdb_put(txn, guard.dbis[DBI::VectorClockIndex as usize], &mut mdb_key, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }
        std::ptr::copy_nonoverlapping(clock_bytes.as_ptr(), data_val.mv_data.cast(), clock_bytes.len());
        let cc = mdb_txn_commit(txn);
        if cc != 0 {
            return Err(from_lmdb_err(cc));
        }
        Ok(vector_clock_key)
    }
}
pub fn get_vector_clock_meta(arc_env: &Arc<Mutex<LmdbEnv>>, key: [u8; 32]) -> Result<Option<XaeroVectorClock>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::VectorClockIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let clock: &XaeroVectorClock = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);

        tracing::debug!("clock found : {clock:?}");
        Ok(Some(*clock))
    }
}
pub fn get_current_state_by_entity_id<const SIZE: usize>(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    entity_id: [u8; 32],
) -> Result<Option<XaeroInternalEvent<SIZE>>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: entity_id.len(),
            mv_data: entity_id.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::CurrentStateIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let event_found = bytemuck::from_bytes::<XaeroInternalEvent<SIZE>>(slice);
        mdb_txn_abort(txn);
        Ok(Some(*event_found))
    }
}
/// peer_key = peer_xaero_id_blake_hash_logical_ts
/// event is event_key to lookup actual event.
pub fn put_clock_peer_key_to_event(arc_env: &Arc<Mutex<LmdbEnv>>, peer_id: [u8; 32], logical_ts: u64, event_key: EventKey) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }
        let mut clock_peer_key_bytes = encode_peer_timestamp_key(peer_id, logical_ts);
        // Use a fixed key for MMR metadata
        let mut key_val = MDB_val {
            mv_size: clock_peer_key_bytes.len(),
            mv_data: clock_peer_key_bytes.as_ptr() as *mut _,
        };
        let event_key_bytes = bytemuck::bytes_of(&event_key);
        let mut data_val = MDB_val {
            mv_size: event_key_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };

        let sc = mdb_put(txn, guard.dbis[DBI::VectorClockIndex as usize], &mut key_val, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        std::ptr::copy_nonoverlapping(event_key_bytes.as_ptr(), data_val.mv_data.cast(), event_key_bytes.len());

        let cc = mdb_txn_commit(txn);
        if cc != 0 {
            return Err(from_lmdb_err(cc));
        }
        tracing::debug!("Stored peer key clock to event {event_key_bytes:?}");
    }
    Ok(())
}
pub fn get_clock_peer_key_to_event(arc_env: Arc<Mutex<LmdbEnv>>, peer_key: [u8; 40]) -> Result<Option<EventKey>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: peer_key.len(),
            mv_data: peer_key.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::VectorClockIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
        let event_key_found = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);
        Ok(Some(*event_key_found))
    }
}

pub fn get_events_by_peer_range(arc_env: &Arc<Mutex<LmdbEnv>>, peer_id: [u8; 32], start_ts: u64, end_ts: u64) -> Result<Vec<EventKey>, Box<dyn std::error::Error>> {
    let mut events = Vec::new();
    for ts in start_ts..=end_ts {
        let peer_key = encode_peer_timestamp_key(peer_id, ts);
        if let Some(event_hash) = get_clock_peer_key_to_event(arc_env.clone(), peer_key)? {
            events.push(event_hash);
        }
    }
    Ok(events)
}
pub fn get_children_entitities_by_entity_id(arc_env: &Arc<Mutex<LmdbEnv>>, entity_id: [u8; 32])
    -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: entity_id.len(),
            mv_data: entity_id.as_ptr() as *mut _,
        };

        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = mdb_get(
            txn,
            guard.dbis[DBI::RelationsIndex as usize],
            &mut key_val,
            &mut data_val
        );

        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == MDB_NOTFOUND {
                // Return zero hash if not found
                return Ok(vec![]);
            }
            return Err(from_lmdb_err(getrc));
        }

        let mut entities_found = Vec::new();
        entities_found.copy_from_slice(
            std::slice::from_raw_parts(data_val.mv_data as *const u8, 32)
        );

        mdb_txn_abort(txn);
        Ok(entities_found)
    }
}
// Add a special key for current VC
const CURRENT_VC_KEY: &[u8] = b"__current_vc__";

pub fn put_current_vc(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    vc_hash: [u8; 32]
) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        // Fixed key for current VC
        let mut key_val = MDB_val {
            mv_size: CURRENT_VC_KEY.len(),
            mv_data: CURRENT_VC_KEY.as_ptr() as *mut _,
        };

        let mut data_val = MDB_val {
            mv_size: 32,
            mv_data: vc_hash.as_ptr() as *mut _,
        };

        let sc = mdb_put(
            txn,
            guard.dbis[DBI::VectorClockIndex as usize],
            &mut key_val,
            &mut data_val,
            0
        );

        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        mdb_txn_commit(txn);
        Ok(())
    }
}

pub fn get_current_vc_hash(
    arc_env: &Arc<Mutex<LmdbEnv>>
) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: CURRENT_VC_KEY.len(),
            mv_data: CURRENT_VC_KEY.as_ptr() as *mut _,
        };

        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = mdb_get(
            txn,
            guard.dbis[DBI::VectorClockIndex as usize],
            &mut key_val,
            &mut data_val
        );

        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == MDB_NOTFOUND {
                // Return zero hash if not found
                return Ok([0; 32]);
            }
            return Err(from_lmdb_err(getrc));
        }

        let mut vc_hash = [0u8; 32];
        vc_hash.copy_from_slice(
            std::slice::from_raw_parts(data_val.mv_data as *const u8, 32)
        );

        mdb_txn_abort(txn);
        Ok(vc_hash)
    }
}

// ================================================================================================
// MMR METADATA STORAGE FUNCTIONS
// ================================================================================================

/// Store MMR metadata in META DB using a fixed key
pub fn put_mmr_meta(arc_env: &Arc<Mutex<LmdbEnv>>, mmr_meta: &MmrMeta) -> Result<(), Box<dyn std::error::Error>> {
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

        let sc = mdb_put(txn, guard.dbis[DBI::MetaIndex as usize], &mut key_val, &mut data_val, MDB_RESERVE);
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
pub fn get_mmr_meta(arc_env: &Arc<Mutex<LmdbEnv>>) -> Result<Option<MmrMeta>, Box<dyn std::error::Error>> {
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

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::MetaIndex as usize], &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

        // ADD SIZE CHECK
        if slice.len() != std::mem::size_of::<MmrMeta>() {
            mdb_txn_abort(txn);
            tracing::warn!("MMR metadata size mismatch: got {} bytes, expected {} bytes", slice.len(), std::mem::size_of::<MmrMeta>());
            return Ok(None); // Return None instead of panicking
        }

        let meta: &MmrMeta = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);

        let lc = meta.leaf_count;
        let pc = meta.peaks_count;
        tracing::debug!("Retrieved MMR metadata: {} leaves, {} peaks", lc, pc);

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

pub unsafe fn get_events_by_event_type<const TSHIRT_SIZE: usize>(env: &Arc<Mutex<LmdbEnv>>, event_type: u32) -> anyhow::Result<Vec<XaeroInternalEvent<TSHIRT_SIZE>>> {
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
            return Ok(results);
        }

        loop {
            // 5) Extract the event_type from enhanced key format
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);

            // Ensure we have enough bytes for the enhanced key
            if raw_key.len() < std::mem::size_of::<EventKey>() {
                // Skip invalid keys
                let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
                if rc != 0 {
                    break;
                }
                continue;
            }

            // Extract event_type from offset 72 (fixed: no try_into needed)
            let event_type_bytes: u8 = raw_key[72];

            // Check if this is the event type we're looking for
            if event_type as u8 == event_type_bytes {
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
                            std::ptr::copy_nonoverlapping(data_slice.as_ptr(), aligned_buffer.as_mut_ptr() as *mut u8, data_slice.len());
                            let internal_event = aligned_buffer.assume_init();
                            results.push(internal_event);
                        }
                    }
                }
            }

            // 7) Advance the cursor (moved outside the condition)
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
    use xaeroflux_core::{date_time::emit_secs, event::make_pinned, hash::blake_hash_slice, initialize};

    use super::*;
    use crate::*;

    #[test]
    fn test_hash_index_functionality() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().expect("failed to get path")).expect("failed to create env")));

        // Create test event
        let event_data = b"test hash index event";
        let event_hash = blake_hash_slice(event_data);
        let timestamp = emit_secs();
        let event_type = make_pinned(123);
        // Push event (automatically creates hash index)
        push_internal_event_universal(&arc_env, event_data, event_type, timestamp).expect("Failed to push event");

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
        let mmr_meta = MmrMeta {
            root_hash: [42u8; 32],
            peaks_count: 5,
            leaf_count: 1000,
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
        assert!(env.dbis[0] > 0); // AOF
        assert!(env.dbis[1] > 0); // META
        assert!(env.dbis[2] > 0); // HASH_INDEX
    }

    #[test]
    fn test_universal_push_and_enhanced_scan() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().expect("failed to get path")).expect("failed to create env")));

        // Test universal push with enhanced scan
        let base_timestamp = emit_secs();
        let universal_one_eventype = make_pinned(101);
        let universal_two_eventype = make_pinned(102);
        // Push events using universal push function
        push_internal_event_universal(&arc_env, b"universal_one", universal_one_eventype, base_timestamp).expect("Failed to push universal event 1");

        push_internal_event_universal(&arc_env, b"universal_two", universal_two_eventype, base_timestamp + 1).expect("Failed to push universal event 2");

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
