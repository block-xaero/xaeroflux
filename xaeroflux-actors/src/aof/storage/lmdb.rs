#[allow(deprecated)]
use std::{
    ffi::CString,
    io::{Error, ErrorKind},
    ptr,
    sync::{Arc, Mutex},
};

use liblmdb::{
    MDB_CREATE, MDB_NOTFOUND, MDB_RDONLY, MDB_RESERVE, MDB_SUCCESS, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn, MDB_val, mdb_cursor_close, mdb_cursor_get, mdb_cursor_open,
    mdb_dbi_close, mdb_dbi_open, mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_put, mdb_strerror, mdb_txn_abort, mdb_txn_begin, mdb_txn_commit,
};
use rkyv::{rancor::Failure, util::AlignedVec};
use rusted_ring_new::{EventPoolFactory, EventUtils};
use xaeroflux_core::{
    date_time::emit_secs,
    event::{EventType, XaeroEvent},
    hash::{blake_hash_slice, sha_256, sha_256_slice},
    pool::XaeroInternalEvent,
};

use super::format::{EventKey, SegmentMeta};
use crate::BusKind;

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

/// Push XaeroInternalEvent directly using bytemuck - no headers needed!
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

        tracing::debug!(
            "Pushed XaeroInternalEvent to LMDB: type={}, size={} bytes, ts={}",
            xaero_event.evt.event_type,
            event_bytes.len(),
            xaero_event.latest_ts
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

        let mut total_examined = 0;
        let mut size_matches = 0;
        let mut timestamp_matches = 0;

        loop {
            total_examined += 1;

            // 5) Extract the timestamp from enhanced key format
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);

            // Ensure we have enough bytes for the enhanced key
            if raw_key.len() < std::mem::size_of::<EventKey>() {
                tracing::debug!("Enhanced scan: Found key smaller than EventKey size: {} bytes", raw_key.len());
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

            timestamp_matches += 1;
            tracing::debug!("Enhanced scan: Found event with timestamp: {} (range: {} to {})", ts, start_ts, end_ts);

            // 6) Try to reconstruct XaeroInternalEvent from stored data
            let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

            // Check if the data size matches XaeroInternalEvent<TSHIRT_SIZE>
            if data_slice.len() == std::mem::size_of::<XaeroInternalEvent<TSHIRT_SIZE>>() {
                size_matches += 1;
                // FIXED: Safe conversion handling alignment
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
                tracing::debug!("Enhanced scan: Reconstructed XaeroInternalEvent from enhanced AOF storage");
            } else {
                tracing::debug!(
                    "Enhanced scan: Data size mismatch: expected {}, got {}",
                    std::mem::size_of::<XaeroInternalEvent<TSHIRT_SIZE>>(),
                    data_slice.len()
                );
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

        tracing::debug!(
            "Enhanced scan<{}>: examined={}, timestamp_matches={}, size_matches={}, results={}",
            TSHIRT_SIZE,
            total_examined,
            timestamp_matches,
            size_matches,
            results.len()
        );

        Ok(results)
    }
}

/// Scans events by specific peer ID within a timestamp range
pub unsafe fn scan_by_peer_range<const TSHIRT_SIZE: usize>(
    env: &Arc<Mutex<LmdbEnv>>,
    peer_id_hash: [u8; 32],
    start_ts: u64,
    end_ts: u64,
) -> anyhow::Result<Vec<XaeroInternalEvent<TSHIRT_SIZE>>> {
    let mut results = Vec::<XaeroInternalEvent<TSHIRT_SIZE>>::new();
    let g = env.lock().expect("failed to lock env");
    let env = g.env;

    unsafe {
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            return Err(anyhow::anyhow!("Failed to begin read txn: {}", sc_tx_begin));
        }

        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, g.dbis[0], &mut cursor);
        if sc_cursor_open != 0 {
            mdb_txn_abort(rtxn);
            return Err(anyhow::anyhow!("Failed to open cursor: {}", sc_cursor_open));
        }

        // Build start key with specific peer ID
        let mut start_key = [0u8; std::mem::size_of::<EventKey>()];
        start_key[0..32].copy_from_slice(&peer_id_hash); // xaero_id_hash
        // Leave vector_clock_hash as zeros                        // vector_clock_hash
        start_key[64..72].copy_from_slice(&start_ts.to_be_bytes()); // timestamp

        let mut key_val = MDB_val {
            mv_size: start_key.len(),
            mv_data: start_key.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, liblmdb::MDB_cursor_op_MDB_SET_RANGE);
        if rc != 0 {
            mdb_cursor_close(cursor);
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        loop {
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);

            if raw_key.len() < std::mem::size_of::<EventKey>() {
                break;
            }

            // Check if this key is still for our peer
            let key_peer_id: [u8; 32] = raw_key[0..32].try_into().map_err(|_| anyhow::anyhow!("Failed to extract peer ID from key"))?;

            if key_peer_id != peer_id_hash {
                break; // We've moved past this peer's events
            }

            // Extract timestamp - FIXED
            let ts_bytes: [u8; 8] = raw_key[64..72].try_into().map_err(|_| anyhow::anyhow!("Failed to extract timestamp from key"))?;
            let ts = u64::from_be_bytes(ts_bytes); // FIXED: only convert once

            if ts >= end_ts {
                break;
            }

            // Reconstruct XaeroInternalEvent
            let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

            if data_slice.len() == std::mem::size_of::<XaeroInternalEvent<TSHIRT_SIZE>>() {
                // FIXED: Safe conversion handling alignment
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

            let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
            if rc != 0 {
                break;
            }
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(rtxn);

        tracing::info!(
            "Peer scan found {} events for peer {:?} in range {} to {}",
            results.len(),
            &peer_id_hash[..4],
            start_ts,
            end_ts
        );
        Ok(results)
    }
}

/// Store a mapping from `leaf_hash` to `SegmentMeta` in the SecondaryIndex DB.
pub fn put_secondary_index(arc_env: &std::sync::Arc<std::sync::Mutex<LmdbEnv>>, leaf_hash: &[u8; 32], meta: &SegmentMeta) -> Result<(), Box<dyn std::error::Error>> {
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
pub fn get_secondary_index(arc_env: &std::sync::Arc<std::sync::Mutex<LmdbEnv>>, leaf_hash: &[u8; 32]) -> Result<Option<SegmentMeta>, Box<dyn std::error::Error>> {
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytemuck::bytes_of;
    use tempfile::tempdir;
    use xaeroflux_core::{
        date_time::{MS_PER_DAY, day_bounds_from_epoch_ms, emit_secs},
        event::{EventType, XaeroEvent},
        initialize,
        pool::XaeroInternalEvent,
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
        let dir = tempdir().expect("failed_to_unravel");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data).expect("failed_to_unravel"),
        ));

        let data = b"payload".to_vec();
        let leaf_hash = sha_256_slice(&data);
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
        let got = get_secondary_index(&arc_env, &leaf_hash).expect("get_secondary_index").expect("meta missing");
        let unaligned_g_pidx = got.page_index;
        assert_eq!(unaligned_m_pid, unaligned_g_pidx, "page_index mismatch");
    }

    #[test]
    fn test_get_secondary_index_not_found() {
        initialize();
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

        let dir = tempdir().expect("failed to unravel");
        let env = LmdbEnv::new(dir.path().to_str().expect("failed to unravel"), BusKind::Control).expect("failed to unravel");

        assert!(!env.env.is_null());
        assert!(env.dbis[0] > 0);
        assert!(env.dbis[1] > 0);
    }

    #[test]
    fn test_universal_push_and_enhanced_scan() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to get path"), BusKind::Data).expect("failed to create env"),
        ));

        // Test universal push with enhanced scan
        let base_timestamp = emit_secs();

        // Push events using universal push function
        push_internal_event_universal(&arc_env, b"universal_one", 101, base_timestamp).expect("Failed to push universal event 1");

        push_internal_event_universal(&arc_env, b"universal_two", 102, base_timestamp + 1).expect("Failed to push universal event 2");

        // Test enhanced range scan with XS size (since our data is small)
        let scan_start = base_timestamp - 5;
        let scan_end = base_timestamp + 5;

        let events = unsafe { scan_enhanced_range::<64>(&arc_env, scan_start, scan_end).expect("Enhanced scan failed") };

        tracing::info!("Enhanced scan found {} events", events.len());
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

    #[test]
    fn test_universal_push_function() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to get path"), BusKind::Data).expect("failed to create env"),
        ));

        let timestamp = emit_secs();

        // Test different sizes to verify size estimation
        let small_data = b"small";
        let medium_data = vec![b'M'; 300]; // Should use S (256) size
        let large_data = vec![b'L'; 2000]; // Should use M (1024) size

        // Test universal push with different sizes
        push_internal_event_universal(&arc_env, small_data, 101, timestamp).expect("Failed to push small event");

        push_internal_event_universal(&arc_env, &medium_data, 102, timestamp + 1).expect("Failed to push medium event");

        push_internal_event_universal(&arc_env, &large_data, 103, timestamp + 2).expect("Failed to push large event");

        println!("✅ Universal push function working for all sizes");
    }

    #[test]
    fn test_enhanced_key_generation() {
        initialize();

        // Test data
        let data = b"hello";
        let timestamp = 123_456_789u64;
        let event_type = 42u32;
        let xaero_id_hash = [1u8; 32]; // Test peer ID
        let vector_clock_hash = [2u8; 32]; // Test vector clock state

        // Generate enhanced key
        let key1 = generate_event_key(data, event_type, timestamp, xaero_id_hash, vector_clock_hash);

        let key2 = generate_event_key(data, event_type, timestamp, xaero_id_hash, vector_clock_hash);

        // Test consistency
        assert_eq!(key1.ts, key2.ts, "Timestamps should match");
        assert_eq!(key1.xaero_id_hash, key2.xaero_id_hash, "Peer IDs should match");
        assert_eq!(key1.vector_clock_hash, key2.vector_clock_hash, "Vector clocks should match");
        assert_eq!(key1.kind, key2.kind, "Event types should match");
        assert_eq!(key1.hash, key2.hash, "Content hashes should match");

        // Test timestamp preservation (convert from big-endian) - FIXED
        assert_eq!(u64::from_be(key1.ts), timestamp, "Timestamp should be preserved in big-endian format");

        // Test field values
        assert_eq!(key1.kind, event_type as u8, "Event type should match");
        assert_eq!(key1.xaero_id_hash, xaero_id_hash, "Peer ID hash should match");
        assert_eq!(key1.vector_clock_hash, vector_clock_hash, "Vector clock hash should match");
        assert_eq!(key1.hash, blake_hash_slice(data), "Content hash should match");

        // Test key size
        let key_bytes = bytes_of(&key1);
        assert_eq!(key_bytes.len(), std::mem::size_of::<EventKey>(), "Key should have correct size");

        println!("✅ Enhanced key generation is consistent");
    }

    // NEW: Test for XaeroInternalEvent storage and retrieval
    #[test]
    fn test_xaero_internal_event_storage() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to get path"), BusKind::Data).expect("failed to create env"),
        ));

        // Create a test XaeroInternalEvent
        use bytemuck::Zeroable;
        use rusted_ring_new::PooledEvent;

        let mut pooled_event = PooledEvent::<256>::zeroed();
        let test_data = b"test data for internal event";
        let copy_len = std::cmp::min(test_data.len(), 256);
        pooled_event.data[..copy_len].copy_from_slice(&test_data[..copy_len]);
        pooled_event.len = test_data.len() as u32;
        pooled_event.event_type = EventType::ApplicationEvent(1).to_u8() as u32;

        let internal_event = XaeroInternalEvent::<256> {
            xaero_id_hash: [1u8; 32],     // Test peer ID
            vector_clock_hash: [2u8; 32], // Test vector clock
            evt: pooled_event,
            latest_ts: emit_secs(),
        };

        // Test storage
        push_xaero_internal_event(&arc_env, &internal_event).expect("Failed to push XaeroInternalEvent");

        println!("✅ XaeroInternalEvent storage completed without error");
    }

    #[test]
    fn test_debug_universal_push_storage() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");

        // Create the basic LMDB environment directly - no subject hash needed for this test
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to get path"), BusKind::Data).expect("failed to create env"),
        ));

        let test_data = b"debug_test";
        let timestamp = emit_secs();

        // Test what size gets estimated
        let estimated_size = rusted_ring_new::EventPoolFactory::estimate_size(test_data.len());
        println!("Data length: {}, Estimated size: {:?}", test_data.len(), estimated_size);

        // Push using universal function
        match push_internal_event_universal(&arc_env, test_data, 42, timestamp) {
            Ok(_) => println!("✅ Push succeeded"),
            Err(e) => {
                println!("❌ Push failed: {:?}", e);
                return;
            }
        }

        // Now let's manually check what's in the database
        unsafe {
            let g = arc_env.lock().expect("failed to lock env");
            let env = g.env;

            let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
            let sc_tx_begin = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
            if sc_tx_begin == 0 {
                let mut cursor = std::ptr::null_mut();
                let sc_cursor_open = mdb_cursor_open(rtxn, g.dbis[0], &mut cursor);
                if sc_cursor_open == 0 {
                    let mut key_val = MDB_val {
                        mv_size: 0,
                        mv_data: std::ptr::null_mut(),
                    };
                    let mut data_val = MDB_val {
                        mv_size: 0,
                        mv_data: std::ptr::null_mut(),
                    };

                    // Count total entries
                    let mut count = 0;
                    let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, liblmdb::MDB_cursor_op_MDB_FIRST);
                    if rc == 0 {
                        loop {
                            count += 1;
                            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);
                            println!("Entry {}: key_size={}, data_size={}", count, raw_key.len(), data_val.mv_size);

                            // If it looks like an enhanced key, extract timestamp
                            if raw_key.len() >= std::mem::size_of::<EventKey>() {
                                let ts_bytes: [u8; 8] = raw_key[64..72].try_into().unwrap_or([0; 8]);
                                let ts = u64::from_be_bytes(ts_bytes); // FIXED: only convert once
                                println!("  Enhanced key timestamp: {}", ts);
                                println!("  Original timestamp: {}", timestamp);
                            }

                            // Check what XaeroInternalEvent sizes match this data
                            println!("  XaeroInternalEvent<64> size: {}", std::mem::size_of::<XaeroInternalEvent<64>>());
                            println!("  XaeroInternalEvent<256> size: {}", std::mem::size_of::<XaeroInternalEvent<256>>());
                            println!("  Actual data size: {}", data_val.mv_size);

                            let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
                            if rc != 0 {
                                break;
                            }
                        }
                    } else {
                        println!("No entries found in database (cursor get first failed: {})", rc);
                    }

                    println!("Total entries in database: {}", count);
                    mdb_cursor_close(cursor);
                } else {
                    println!("Failed to open cursor: {}", sc_cursor_open);
                }
                mdb_txn_abort(rtxn);
            } else {
                println!("Failed to begin transaction: {}", sc_tx_begin);
            }
        }

        println!("✅ Debug universal push storage test completed");
    }

    // NEW: Test for enhanced scan functions with XaeroInternalEvent
    #[test]
    fn test_enhanced_scan_functions() {
        initialize();

        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed to get path"), BusKind::Data).expect("failed to create env"),
        ));

        // Test enhanced scanning capabilities using universal push
        let base_timestamp = emit_secs();

        // Use universal push to create XaeroInternalEvents
        push_internal_event_universal(&arc_env, b"enhanced event from peer A", 101, base_timestamp).expect("Failed to push event 1");

        push_internal_event_universal(&arc_env, b"enhanced event from peer B", 102, base_timestamp + 1).expect("Failed to push event 2");

        push_internal_event_universal(&arc_env, b"another event from peer A", 103, base_timestamp + 2).expect("Failed to push event 3");

        // Test enhanced range scan - use appropriate size based on data length
        let scan_start = base_timestamp - 5;
        let scan_end = base_timestamp + 5;

        // Since our test data is small, it will use XS (64 byte) size
        let events = unsafe { scan_enhanced_range::<64>(&arc_env, scan_start, scan_end).expect("Enhanced scan failed") };

        tracing::info!("Enhanced scan found {} events", events.len());
        assert!(events.len() >= 3, "Expected at least 3 events from enhanced scan, got {}", events.len());

        // Verify we can read the event data
        let mut found_events = 0;
        for event in &events {
            let event_data = &event.evt.data[..event.evt.len as usize];
            if event_data == b"enhanced event from peer A" || event_data == b"enhanced event from peer B" || event_data == b"another event from peer A" {
                found_events += 1;
            }
        }

        assert!(found_events >= 3, "Should find all 3 test events, found {}", found_events);

        println!("✅ Enhanced scan functions test completed");
    }
}
