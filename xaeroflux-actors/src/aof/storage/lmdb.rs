use std::{ffi::CStr, slice};
#[allow(deprecated)]
use std::{
    ffi::CString,
    io::{Error, ErrorKind},
    ptr,
    sync::{Arc, Mutex},
};

use super::format::{EventKey, MmrMeta};
use crate::read_api::PointQuery;
use anyhow::anyhow;
use iroh::NodeId;
use liblmdb::{
    mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_dbi_close, mdb_dbi_open, mdb_env_create,
    mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_get, mdb_put, mdb_strerror, mdb_txn_abort,
    mdb_txn_begin, mdb_txn_commit, MDB_cursor_op_MDB_NEXT, MDB_dbi, MDB_env, MDB_txn, MDB_val,
    MDB_CREATE, MDB_NODUPDATA, MDB_NOSUBDIR, MDB_NOTFOUND, MDB_RDONLY, MDB_RESERVE, MDB_SUCCESS,
};
use rkyv::{rancor::Failure, util::AlignedVec};
use rusted_ring::{EventPoolFactory, EventSize, EventUtils};
use xaeroflux_core::hash::blake_hash;
use xaeroflux_core::{
    date_time::emit_secs,
    event::{get_base_event_type, is_create_event, is_pinned_event, is_update_event, EventType, XaeroEvent},
    hash::{blake_hash_slice, sha_256, sha_256_slice},
    pool::XaeroInternalEvent,
    vector_clock::XaeroVectorClock,
};
use xaeroid::{cache::xaero_id_hash, XaeroID};

// T-shirt sizes as constants
pub const XS_SIZE: usize = 128;
pub const S_SIZE: usize = 256;
pub const M_SIZE: usize = 512;
pub const L_SIZE: usize = 1024;
pub const XL_SIZE: usize = 2048;

#[repr(usize)]
pub enum DBI {
    // Event storage by size
    EventsXS = 0,
    EventsS = 1,
    EventsM = 2,
    EventsL = 3,
    EventsXL = 4,

    // Metadata and indices
    MetaIndex = 5,
    HashIndex = 6,
    VectorClockIndex = 7,
    XaeroIdNodeIdIndex = 8,
    XaeroIdIndex = 9,
    CurrentStateIndex = 10,
    RelationsIndex = 11,
}

/// A wrapper around an LMDB environment with size-segregated event databases
pub struct LmdbEnv {
    pub env: *mut MDB_env,
    pub dbis: [MDB_dbi; 12],
}

unsafe impl Sync for LmdbEnv {}
unsafe impl Send for LmdbEnv {}

impl LmdbEnv {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(path)?;

        let mut env = ptr::null_mut();
        unsafe {
            tracing::info!("Creating LMDB environment at {}", path);

            mdb_env_create(&mut env);
            mdb_env_set_maxdbs(env, 12); // Increased for multiple size DBs

            // Smaller map size as suggested
            mdb_env_set_mapsize(env, 32 * 1024 * 1024); // 32MB

            const MDB_NOLOCK: u32 = 0x400000;
            const MDB_NOSUBDIR: u32 = 0x4000;

            let db_file = format!("{}/cyan.mdb", path);
            let cs = CString::new(db_file)?;

            let flags = MDB_NOSUBDIR | MDB_NOLOCK | MDB_CREATE;
            let sc_env_open = mdb_env_open(env, cs.as_ptr(), flags, 0o664);

            tracing::info!("mdb_env_open result: {}", sc_env_open);

            if sc_env_open != 0 {
                let err_str = CStr::from_ptr(mdb_strerror(sc_env_open)).to_string_lossy();
                tracing::error!("LMDB open failed: {} - {}", sc_env_open, err_str);
                return Err(Box::new(std::io::Error::from_raw_os_error(sc_env_open)));
            }
        }

        // Open size-based event databases
        let events_xs_dbi = unsafe { open_named_db(env, c"/events_xs".as_ptr())? };
        tracing::debug!("Opened events_xs_dbi = {}", events_xs_dbi);

        let events_s_dbi = unsafe { open_named_db(env, c"/events_s".as_ptr())? };
        tracing::debug!("Opened events_s_dbi = {}", events_s_dbi);

        let events_m_dbi = unsafe { open_named_db(env, c"/events_m".as_ptr())? };
        tracing::debug!("Opened events_m_dbi = {}", events_m_dbi);

        let events_l_dbi = unsafe { open_named_db(env, c"/events_l".as_ptr())? };
        tracing::debug!("Opened events_l_dbi = {}", events_l_dbi);

        let events_xl_dbi = unsafe { open_named_db(env, c"/events_xl".as_ptr())? };
        tracing::debug!("Opened events_xl_dbi = {}", events_xl_dbi);

        // Open metadata databases
        let meta_dbi = unsafe { open_named_db(env, c"/meta_index".as_ptr())? };
        let hash_index_dbi = unsafe { open_named_db(env, c"/hash_index".as_ptr())? };
        let vector_clock_index_dbi = unsafe { open_named_db(env, c"/vector_clock_index".as_ptr())? };
        let xaero_id_node_id_index_dbi = unsafe { open_named_db(env, c"/xaero_id_node_id_index".as_ptr())? };
        let xaero_id_index_dbi = unsafe { open_named_db(env, c"/xaero_id_index".as_ptr())? };
        let current_state_idx_dbi = unsafe { open_named_db(env, c"/aof_current_state_index".as_ptr())? };
        let relations_idx_dbi = unsafe { open_named_db(env, c"/relations_index".as_ptr())? };

        Ok(Self {
            env,
            dbis: [
                events_xs_dbi,
                events_s_dbi,
                events_m_dbi,
                events_l_dbi,
                events_xl_dbi,
                meta_dbi,
                hash_index_dbi,
                vector_clock_index_dbi,
                xaero_id_node_id_index_dbi,
                xaero_id_index_dbi,
                current_state_idx_dbi,
                relations_idx_dbi,
            ],
        })
    }

    /// Get the appropriate DBI for a given event size
    fn get_dbi_for_size<const SIZE: usize>(&self) -> MDB_dbi {
        match SIZE {
            XS_SIZE => self.dbis[DBI::EventsXS as usize],
            S_SIZE => self.dbis[DBI::EventsS as usize],
            M_SIZE => self.dbis[DBI::EventsM as usize],
            L_SIZE => self.dbis[DBI::EventsL as usize],
            XL_SIZE => self.dbis[DBI::EventsXL as usize],
            _ => panic!("Invalid event size: {}", SIZE),
        }
    }

    /// Get DBI based on EventSize enum
    fn get_dbi_for_event_size(&self, size: EventSize) -> MDB_dbi {
        match size {
            EventSize::XS => self.dbis[DBI::EventsXS as usize],
            EventSize::S => self.dbis[DBI::EventsS as usize],
            EventSize::M => self.dbis[DBI::EventsM as usize],
            EventSize::L => self.dbis[DBI::EventsL as usize],
            EventSize::XL => self.dbis[DBI::EventsXL as usize],
            EventSize::XXL => panic!("XXL size not supported"),
        }
    }
}

/// Opens or creates a named database in the LMDB environment
pub unsafe fn open_named_db(env: *mut MDB_env, name_ptr: *const i8) -> Result<MDB_dbi, Box<dyn std::error::Error>> {
    let mut txn = std::ptr::null_mut();
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

pub fn from_lmdb_err_anyhow(code: i32) -> anyhow::Error {
    let cstr = unsafe { mdb_strerror(code) };
    let msg = unsafe { std::ffi::CStr::from_ptr(cstr) }.to_string_lossy().into_owned();
    anyhow!(msg)
}

impl Drop for LmdbEnv {
    fn drop(&mut self) {
        unsafe {
            for dbi in self.dbis.iter() {
                mdb_dbi_close(self.env, *dbi);
            }
            liblmdb::mdb_env_close(self.env);
        }
    }
}

/// Generates event key
pub fn generate_event_key(
    event_data: &[u8],
    event_type: u32,
    timestamp: u64,
    xaero_id_hash: [u8; 32],
    vector_clock_hash: [u8; 32]
) -> EventKey {
    EventKey {
        xaero_id_hash,
        vector_clock_hash,
        ts: timestamp.to_be(),
        kind: event_type as u8,
        hash: blake_hash_slice(event_data),
    }
}

/// Encode size as a single byte for storage
fn encode_size_byte(size: usize) -> u8 {
    match size {
        XS_SIZE => 1,
        S_SIZE => 2,
        M_SIZE => 3,
        L_SIZE => 4,
        XL_SIZE => 5,
        _ => 0,
    }
}

/// Decode size byte back to usize
fn decode_size_byte(byte: u8) -> Option<usize> {
    match byte {
        1 => Some(XS_SIZE),
        2 => Some(S_SIZE),
        3 => Some(M_SIZE),
        4 => Some(L_SIZE),
        5 => Some(XL_SIZE),
        _ => None,
    }
}

/// Push event with size-specific database selection
pub fn push_xaero_internal_event<const TSHIRT_SIZE: usize>(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    xaero_event: &XaeroInternalEvent<TSHIRT_SIZE>
) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        let event_type = get_base_event_type(xaero_event.evt.event_type);
        let env = arc_env.lock().expect("failed to lock env");
        let mut txn = ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn);
        if sc_tx_begin != 0 {
            return Err(from_lmdb_err(sc_tx_begin));
        }

        let event_data = &xaero_event.evt.data[..xaero_event.evt.len as usize];
        let key = generate_event_key(
            event_data,
            get_base_event_type(xaero_event.evt.event_type),
            xaero_event.latest_ts,
            xaero_event.xaero_id_hash,
            xaero_event.vector_clock_hash,
        );

        let key_bytes: &[u8] = bytemuck::bytes_of(&key);
        let mut key_val = MDB_val {
            mv_size: key_bytes.len(),
            mv_data: key_bytes.as_ptr() as *mut libc::c_void,
        };

        // Store the entire XaeroInternalEvent in the appropriate size DB
        let event_bytes = bytemuck::bytes_of(xaero_event);
        let mut data_val = MDB_val {
            mv_size: event_bytes.len(),
            mv_data: std::ptr::null_mut(),
        };

        // Select the correct database based on size
        let dbi = env.get_dbi_for_size::<TSHIRT_SIZE>();

        let sc = mdb_put(txn, dbi, &mut key_val, &mut data_val, MDB_RESERVE);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        std::ptr::copy_nonoverlapping(event_bytes.as_ptr(), data_val.mv_data.cast(), event_bytes.len());

        // Update hash index with size indicator
        let event_hash = blake_hash_slice(event_data);
        let hash_key_bytes = event_hash;
        let mut hash_key_val = MDB_val {
            mv_size: hash_key_bytes.len(),
            mv_data: hash_key_bytes.as_ptr() as *mut libc::c_void,
        };

        // Store key + size indicator in hash index
        let mut hash_value = Vec::with_capacity(std::mem::size_of::<EventKey>() + 1);
        hash_value.extend_from_slice(bytemuck::bytes_of(&key));
        hash_value.push(encode_size_byte(TSHIRT_SIZE));

        let mut hash_data_val = MDB_val {
            mv_size: hash_value.len(),
            mv_data: std::ptr::null_mut(),
        };

        let hash_sc = mdb_put(
            txn,
            env.dbis[DBI::HashIndex as usize],
            &mut hash_key_val,
            &mut hash_data_val,
            MDB_RESERVE
        );
        if hash_sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(hash_sc));
        }

        std::ptr::copy_nonoverlapping(hash_value.as_ptr(), hash_data_val.mv_data.cast(), hash_value.len());

        // Handle CurrentState and Relations updates
        if is_create_event(event_type) || is_update_event(event_type) {
            if event_data.len() >= 64 {
                let mut entity_id = [0u8; 32];
                entity_id.copy_from_slice(&event_data[..32]);
                let mut parent_id = [0u8; 32];
                parent_id.copy_from_slice(&event_data[32..64]);

                // Store in CurrentState with size indicator
                let mut entity_id_key = MDB_val {
                    mv_size: entity_id.len(),
                    mv_data: entity_id.as_ptr() as *mut libc::c_void,
                };

                let mut state_value = Vec::with_capacity(event_bytes.len() + 1);
                state_value.extend_from_slice(event_bytes);
                state_value.push(encode_size_byte(TSHIRT_SIZE));

                let mut entity_data_val = MDB_val {
                    mv_size: state_value.len(),
                    mv_data: state_value.as_ptr() as *mut libc::c_void,
                };

                let sc = mdb_put(
                    txn,
                    env.dbis[DBI::CurrentStateIndex as usize],
                    &mut entity_id_key,
                    &mut entity_data_val,
                    0
                );
                if sc != 0 {
                    mdb_txn_abort(txn);
                    return Err(from_lmdb_err(sc));
                }

                // Handle relations
                if parent_id != [0u8; 32] {
                    let mut parent_key = MDB_val {
                        mv_size: parent_id.len(),
                        mv_data: parent_id.as_ptr() as *mut libc::c_void,
                    };

                    let mut existing_val = MDB_val {
                        mv_size: 0,
                        mv_data: std::ptr::null_mut(),
                    };

                    let mut entities_buffer = Vec::new();
                    let get_sc = mdb_get(
                        txn,
                        env.dbis[DBI::RelationsIndex as usize],
                        &mut parent_key,
                        &mut existing_val
                    );

                    if get_sc == 0 {
                        let existing = slice::from_raw_parts(existing_val.mv_data as *const u8, existing_val.mv_size);
                        let already_present = existing.chunks_exact(32).any(|chunk| chunk == entity_id);
                        if !already_present {
                            entities_buffer.extend_from_slice(existing);
                            entities_buffer.extend_from_slice(&entity_id);
                        }
                    } else if get_sc == MDB_NOTFOUND {
                        entities_buffer.extend_from_slice(&entity_id);
                    } else {
                        mdb_txn_abort(txn);
                        return Err(from_lmdb_err(get_sc));
                    }

                    if !entities_buffer.is_empty() {
                        let mut new_val = MDB_val {
                            mv_size: entities_buffer.len(),
                            mv_data: entities_buffer.as_ptr() as *mut libc::c_void,
                        };

                        let put_sc = mdb_put(
                            txn,
                            env.dbis[DBI::RelationsIndex as usize],
                            &mut parent_key,
                            &mut new_val,
                            0
                        );
                        if put_sc != 0 {
                            mdb_txn_abort(txn);
                            return Err(from_lmdb_err(put_sc));
                        }
                    }
                }
            }
        }

        tracing::debug!(
            "Pushed event to size-{} DB: type={}, ts={}, hash={}",
            TSHIRT_SIZE,
            xaero_event.evt.event_type,
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

/// Universal push function that estimates size and routes to correct DB
pub fn push_internal_event_universal(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    event_data: &[u8],
    event_type: u32,
    timestamp: u64
) -> Result<(), Box<dyn std::error::Error>> {
    let estimated_size = EventPoolFactory::estimate_size(event_data.len());

    match estimated_size {
        rusted_ring::EventSize::XS => {
            let pooled_event = EventUtils::create_pooled_event::<XS_SIZE>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<XS_SIZE> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::S => {
            let pooled_event = EventUtils::create_pooled_event::<S_SIZE>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<S_SIZE> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::M => {
            let pooled_event = EventUtils::create_pooled_event::<M_SIZE>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<M_SIZE> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::L => {
            let pooled_event = EventUtils::create_pooled_event::<L_SIZE>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<L_SIZE> {
                xaero_id_hash: [0u8; 32],
                vector_clock_hash: [0u8; 32],
                evt: pooled_event,
                latest_ts: timestamp,
            };
            push_xaero_internal_event(arc_env, &internal_event)
        }
        rusted_ring::EventSize::XL => {
            let pooled_event = EventUtils::create_pooled_event::<XL_SIZE>(event_data, event_type)?;
            let internal_event = XaeroInternalEvent::<XL_SIZE> {
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

/// Get event by hash with typed size
pub fn get_event_by_hash<const TSHIRT_SIZE: usize>(
    arc_env: &Arc<Mutex<LmdbEnv>>,
    event_hash: [u8; 32],
) -> Result<Option<XaeroInternalEvent<TSHIRT_SIZE>>, Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        // Look up in hash index
        let mut hash_key_val = MDB_val {
            mv_size: event_hash.len(),
            mv_data: event_hash.as_ptr() as *mut _,
        };
        let mut hash_data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(
            txn,
            guard.dbis[DBI::HashIndex as usize],
            &mut hash_key_val,
            &mut hash_data_val
        );
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let hash_value = std::slice::from_raw_parts(
            hash_data_val.mv_data as *const u8,
            hash_data_val.mv_size
        );

        // Extract EventKey (all but last byte)
        let event_key_bytes = &hash_value[..hash_value.len() - 1];

        // Get from appropriate size DB
        let dbi = guard.get_dbi_for_size::<TSHIRT_SIZE>();

        let mut key_val = MDB_val {
            mv_size: event_key_bytes.len(),
            mv_data: event_key_bytes.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let getrc = liblmdb::mdb_get(txn, dbi, &mut key_val, &mut data_val);
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

        match bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT_SIZE>>(data_slice) {
            Ok(event) => {
                mdb_txn_abort(txn);
                Ok(Some(*event))
            }
            Err(_) => {
                // Handle alignment
                let mut aligned: std::mem::MaybeUninit<XaeroInternalEvent<TSHIRT_SIZE>> =
                    std::mem::MaybeUninit::uninit();
                std::ptr::copy_nonoverlapping(
                    data_slice.as_ptr(),
                    aligned.as_mut_ptr() as *mut u8,
                    data_slice.len()
                );
                mdb_txn_abort(txn);
                Ok(Some(aligned.assume_init()))
            }
        }
    }
}

/// Scan events from a specific size database
pub unsafe fn scan_range_sized<const TSHIRT_SIZE: usize>(
    env: &Arc<Mutex<LmdbEnv>>,
    start_ts: u64,
    end_ts: u64
) -> anyhow::Result<Vec<XaeroInternalEvent<TSHIRT_SIZE>>> {
    let mut results = Vec::new();
    let g = env.lock().expect("failed to lock env");
    let env_ptr = g.env;
    let dbi = g.get_dbi_for_size::<TSHIRT_SIZE>();

    unsafe {
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env_ptr, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            return Err(anyhow::anyhow!("Failed to begin read txn: {}", sc_tx_begin));
        }

        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, dbi, &mut cursor);
        if sc_cursor_open != 0 {
            mdb_txn_abort(rtxn);
            return Err(anyhow::anyhow!("Failed to open cursor: {}", sc_cursor_open));
        }

        let mut key_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let rc = mdb_cursor_get(
            cursor,
            &mut key_val,
            &mut data_val,
            liblmdb::MDB_cursor_op_MDB_FIRST
        );
        if rc != 0 {
            mdb_cursor_close(cursor);
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        loop {
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);

            if raw_key.len() >= std::mem::size_of::<EventKey>() {
                let ts_bytes: [u8; 8] = raw_key[64..72].try_into()?;
                let ts = u64::from_be_bytes(ts_bytes);

                if ts >= start_ts && ts < end_ts {
                    let data_slice = std::slice::from_raw_parts(
                        data_val.mv_data as *const u8,
                        data_val.mv_size
                    );

                    // No size checking needed - we know it's the right size!
                    match bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT_SIZE>>(data_slice) {
                        Ok(event) => results.push(*event),
                        Err(_) => {
                            // Handle alignment
                            let mut aligned: std::mem::MaybeUninit<XaeroInternalEvent<TSHIRT_SIZE>> =
                                std::mem::MaybeUninit::uninit();
                            std::ptr::copy_nonoverlapping(
                                data_slice.as_ptr(),
                                aligned.as_mut_ptr() as *mut u8,
                                data_slice.len()
                            );
                            results.push(aligned.assume_init());
                        }
                    }
                } else if ts >= end_ts {
                    break;
                }
            }

            let rc = mdb_cursor_get(
                cursor,
                &mut key_val,
                &mut data_val,
                MDB_cursor_op_MDB_NEXT
            );
            if rc != 0 {
                break;
            }
        }

        mdb_cursor_close(cursor);
        mdb_txn_abort(rtxn);
        Ok(results)
    }
}

/// Get events by event type from sized database
pub unsafe fn get_events_by_event_type<const TSHIRT_SIZE: usize>(
    env: &Arc<Mutex<LmdbEnv>>,
    event_type: u32
) -> anyhow::Result<Vec<XaeroInternalEvent<TSHIRT_SIZE>>> {
    let mut results = Vec::new();
    let g = env.lock().expect("failed to lock env");
    let env_ptr = g.env;
    let dbi = g.get_dbi_for_size::<TSHIRT_SIZE>();

    unsafe {
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env_ptr, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            return Err(anyhow::anyhow!("Failed to begin read txn: {}", sc_tx_begin));
        }

        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, dbi, &mut cursor);
        if sc_cursor_open != 0 {
            mdb_txn_abort(rtxn);
            return Err(anyhow::anyhow!("Failed to open cursor: {}", sc_cursor_open));
        }

        let mut key_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, liblmdb::MDB_cursor_op_MDB_FIRST);
        if rc != 0 {
            mdb_cursor_close(cursor);
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        loop {
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);
            let base_event_type = event_type as u8;

            if raw_key.len() >= 73 {
                let stored_event_type = raw_key[72];

                if base_event_type == stored_event_type {
                    let data_slice = std::slice::from_raw_parts(
                        data_val.mv_data as *const u8,
                        data_val.mv_size
                    );

                    match bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT_SIZE>>(data_slice) {
                        Ok(event) => results.push(*event),
                        Err(_) => {
                            let mut aligned: std::mem::MaybeUninit<XaeroInternalEvent<TSHIRT_SIZE>> =
                                std::mem::MaybeUninit::uninit();
                            std::ptr::copy_nonoverlapping(
                                data_slice.as_ptr(),
                                aligned.as_mut_ptr() as *mut u8,
                                data_slice.len()
                            );
                            results.push(aligned.assume_init());
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

        tracing::info!("get_events_by_event_type: Found {} events of type {} in size-{} DB",
            results.len(), event_type, TSHIRT_SIZE);
        Ok(results)
    }
}

/// Get current state by entity ID with typed size
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

        let getrc = liblmdb::mdb_get(
            txn,
            guard.dbis[DBI::CurrentStateIndex as usize],
            &mut key_val,
            &mut data_val
        );
        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == liblmdb::MDB_NOTFOUND {
                return Ok(None);
            } else {
                return Err(from_lmdb_err(getrc));
            }
        }

        let data = std::slice::from_raw_parts(
            data_val.mv_data as *const u8,
            data_val.mv_size
        );

        // Skip size byte at the end
        let event_data = &data[..data.len() - 1];

        match bytemuck::try_from_bytes::<XaeroInternalEvent<SIZE>>(event_data) {
            Ok(event) => {
                mdb_txn_abort(txn);
                Ok(Some(*event))
            }
            Err(_) => {
                let mut aligned: std::mem::MaybeUninit<XaeroInternalEvent<SIZE>> =
                    std::mem::MaybeUninit::uninit();
                std::ptr::copy_nonoverlapping(
                    event_data.as_ptr(),
                    aligned.as_mut_ptr() as *mut u8,
                    event_data.len()
                );
                mdb_txn_abort(txn);
                Ok(Some(aligned.assume_init()))
            }
        }
    }
}

// All unchanged helper functions below...

pub fn put_xaero_id(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_id: XaeroID) -> Result<[u8; 32], anyhow::Error> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;
    let xaero_id_bytes = bytemuck::bytes_of::<XaeroID>(&xaero_id);
    let xaero_id_hash_calculated = blake_hash_slice(xaero_id_bytes);
    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err_anyhow(rc));
        }

        let mut xaero_id_hash_mdb_val = MDB_val {
            mv_size: xaero_id_hash_calculated.len(),
            mv_data: xaero_id_hash_calculated.as_ptr() as *mut _,
        };

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
            return Err(from_lmdb_err_anyhow(sc));
        }

        mdb_txn_commit(txn);
    }
    Ok(xaero_id_hash_calculated)
}

pub fn get_xaero_id_by_xaero_id_hash(arc_env: &Arc<Mutex<LmdbEnv>>, xaero_id_hash: [u8; 32]) ->
                                                                                             Result<Option<&XaeroID>, anyhow::Error> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err_anyhow(rc));
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
                return Err(from_lmdb_err_anyhow(getrc));
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

        let mut xaero_id_hash = MDB_val {
            mv_size: xaero_id.len(),
            mv_data: xaero_id.as_ptr() as *mut _,
        };

        let node_id_bytes = bytemuck::bytes_of(node_id);
        let mut node_id_val = MDB_val {
            mv_size: node_id_bytes.len(),
            mv_data: node_id_bytes.as_ptr() as *mut _,
        };

        let sc = mdb_put(txn, guard.dbis[DBI::XaeroIdNodeIdIndex as usize], &mut xaero_id_hash, &mut node_id_val, MDB_NODUPDATA);
        if sc != 0 {
            mdb_txn_abort(txn);
            return Err(from_lmdb_err(sc));
        }

        mdb_txn_commit(txn);
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

        let getrc = liblmdb::mdb_get(txn, guard.dbis[DBI::XaeroIdNodeIdIndex as usize], &mut key_val, &mut data_val);
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


pub fn create_or_update_vector_clock(arc_env: &Arc<Mutex<LmdbEnv>>, vector_clock:
&XaeroVectorClock) ->
                                                                                      Result<[u8;
    32], Box<dyn std::error::Error>> {
    let clock_bytes = bytemuck::bytes_of(vector_clock);
    let vector_clock_key = blake_hash("current_vector_clock");
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

pub fn get_current_vector_clock(arc_env: &Arc<Mutex<LmdbEnv>>) -> Result<Option<XaeroVectorClock>,
    Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;
    let vector_clock_key = blake_hash("current_vector_clock");
    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: vector_clock_key.len(),
            mv_data: vector_clock_key.as_ptr() as *mut _,
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


pub fn get_children_entitities_by_entity_id(arc_env: &Arc<Mutex<LmdbEnv>>, entity_id: [u8; 32]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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

        let getrc = mdb_get(txn, guard.dbis[DBI::RelationsIndex as usize], &mut key_val, &mut data_val);

        if getrc != 0 {
            mdb_txn_abort(txn);
            if getrc == MDB_NOTFOUND {
                return Ok(vec![]);
            }
            return Err(from_lmdb_err(getrc));
        }

        let entities_found = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size).to_vec();

        mdb_txn_abort(txn);
        Ok(entities_found)
    }
}

pub fn put_mmr_meta(arc_env: &Arc<Mutex<LmdbEnv>>, mmr_meta: &MmrMeta) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

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

        if slice.len() != std::mem::size_of::<MmrMeta>() {
            mdb_txn_abort(txn);
            tracing::warn!("MMR metadata size mismatch: got {} bytes, expected {} bytes",
                slice.len(), std::mem::size_of::<MmrMeta>());
            return Ok(None);
        }

        let meta: &MmrMeta = bytemuck::from_bytes(slice);
        mdb_txn_abort(txn);

        let lc = meta.leaf_count;
        let pc = meta.peaks_count;
        tracing::debug!("Retrieved MMR metadata: {} leaves, {} peaks", lc, pc);

        Ok(Some(*meta))
    }
}

pub fn put_hash_index(arc_env: &Arc<Mutex<LmdbEnv>>, event_hash: [u8; 32], event_key: &EventKey) -> Result<(), Box<dyn std::error::Error>> {
    let guard = arc_env.lock().expect("failed to lock env");
    let env = guard.env;

    unsafe {
        let mut txn: *mut MDB_txn = std::ptr::null_mut();
        let rc = mdb_txn_begin(env, std::ptr::null_mut(), 0, &mut txn);
        if rc != 0 {
            return Err(from_lmdb_err(rc));
        }

        let mut key_val = MDB_val {
            mv_size: event_hash.len(),
            mv_data: event_hash.as_ptr() as *mut _,
        };

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

        // Check if there's a size byte at the end
        if slice.len() > std::mem::size_of::<EventKey>() {
            // Has size indicator - extract just the EventKey
            let event_key: &EventKey = bytemuck::from_bytes(&slice[..std::mem::size_of::<EventKey>()]);
            mdb_txn_abort(txn);
            Ok(Some(*event_key))
        } else {
            // Legacy format without size indicator
            let event_key: &EventKey = bytemuck::from_bytes(slice);
            mdb_txn_abort(txn);
            Ok(Some(*event_key))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use xaeroflux_core::{date_time::emit_secs, event::make_pinned, hash::blake_hash_slice, initialize};

    #[test]
    fn test_size_segregated_storage() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap()).unwrap()));

        // Test XS size event
        let xs_data = b"tiny";
        let xs_event = XaeroInternalEvent::<XS_SIZE> {
            xaero_id_hash: [1u8; 32],
            vector_clock_hash: [2u8; 32],
            evt: rusted_ring::PooledEvent {
                data: {
                    let mut d = [0u8; XS_SIZE];
                    d[..xs_data.len()].copy_from_slice(xs_data);
                    d
                },
                len: xs_data.len() as u32,
                event_type: 100,
            },
            latest_ts: emit_secs(),
        };

        push_xaero_internal_event(&arc_env, &xs_event).expect("Failed to push XS event");

        // Test L size event
        let l_data = vec![b'x'; 500];
        let l_event = XaeroInternalEvent::<L_SIZE> {
            xaero_id_hash: [3u8; 32],
            vector_clock_hash: [4u8; 32],
            evt: rusted_ring::PooledEvent {
                data: {
                    let mut d = [0u8; L_SIZE];
                    d[..l_data.len()].copy_from_slice(&l_data);
                    d
                },
                len: l_data.len() as u32,
                event_type: 200,
            },
            latest_ts: emit_secs() + 1,
        };

        push_xaero_internal_event(&arc_env, &l_event).expect("Failed to push L event");

        // Verify events are in different databases by scanning
        let xs_events = unsafe {
            scan_range_sized::<XS_SIZE>(&arc_env, 0, u64::MAX).unwrap()
        };
        assert_eq!(xs_events.len(), 1);
        assert_eq!(xs_events[0].evt.event_type, 100);

        let l_events = unsafe {
            scan_range_sized::<L_SIZE>(&arc_env, 0, u64::MAX).unwrap()
        };
        assert_eq!(l_events.len(), 1);
        assert_eq!(l_events[0].evt.event_type, 200);

        println!("✅ Size-segregated storage working correctly");
    }

    #[test]
    fn test_hash_index_with_size() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap()).unwrap()));

        let event_data = b"test event with size";
        let event_hash = blake_hash_slice(event_data);

        // Push M-sized event
        push_internal_event_universal(&arc_env, event_data, 123, emit_secs()).unwrap();

        // Retrieve by hash
        let found = get_event_by_hash::<M_SIZE>(&arc_env, event_hash).unwrap();
        assert!(found.is_some());

        let event = found.unwrap();
        assert_eq!(&event.evt.data[..event.evt.len as usize], event_data);

        println!("✅ Hash index with size indicators working");
    }

    #[test]
    fn test_universal_push_routing() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap()).unwrap()));

        // Test different sizes get routed correctly
        let tiny = vec![b'a'; 10];
        let small = vec![b'b'; 200];
        let medium = vec![b'c'; 400];
        let large = vec![b'd'; 900];

        push_internal_event_universal(&arc_env, &tiny, 1, emit_secs()).unwrap();
        push_internal_event_universal(&arc_env, &small, 2, emit_secs() + 1).unwrap();
        push_internal_event_universal(&arc_env, &medium, 3, emit_secs() + 2).unwrap();
        push_internal_event_universal(&arc_env, &large, 4, emit_secs() + 3).unwrap();

        // Verify each went to the correct database
        let xs = unsafe { get_events_by_event_type::<XS_SIZE>(&arc_env, 1).unwrap() };
        assert_eq!(xs.len(), 1);

        let s = unsafe { get_events_by_event_type::<S_SIZE>(&arc_env, 2).unwrap() };
        assert_eq!(s.len(), 1);

        let m = unsafe { get_events_by_event_type::<M_SIZE>(&arc_env, 3).unwrap() };
        assert_eq!(m.len(), 1);

        let l = unsafe { get_events_by_event_type::<L_SIZE>(&arc_env, 4).unwrap() };
        assert_eq!(l.len(), 1);

        println!("✅ Universal push routing to correct DBs");
    }

    #[test]
    fn test_current_state_with_size() {
        initialize();
        let dir = tempdir().expect("failed to create temp dir");
        let arc_env = Arc::new(Mutex::new(LmdbEnv::new(dir.path().to_str().unwrap()).unwrap()));

        let entity_id = [42u8; 32];
        let parent_id = [43u8; 32];

        let mut event_data = vec![0u8; 100];
        event_data[..32].copy_from_slice(&entity_id);
        event_data[32..64].copy_from_slice(&parent_id);

        // Push as create event
        push_internal_event_universal(&arc_env, &event_data, make_pinned(1), emit_secs()).unwrap();

        // Retrieve current state
        let state = get_current_state_by_entity_id::<XS_SIZE>(&arc_env, entity_id).unwrap();
        assert!(state.is_some());

        let event = state.unwrap();
        let stored_data = &event.evt.data[..event.evt.len as usize];
        assert_eq!(&stored_data[..32], &entity_id);

        println!("✅ Current state tracking with size indicators");
    }
}