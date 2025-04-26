use std::{ffi::CString, fmt::Debug, ptr, time::SystemTime};

use anyhow::{Error, Ok};
use liblmdb::{
    MDB_CREATE, MDB_RDONLY, MDB_RESERVE, MDB_cursor, MDB_cursor_op, MDB_cursor_op_MDB_SET_RANGE,
    MDB_dbi, MDB_env, MDB_val, mdb_cursor_get, mdb_cursor_open, mdb_dbi_close, mdb_dbi_open,
    mdb_env_create, mdb_env_open, mdb_env_set_mapsize, mdb_env_set_maxdbs, mdb_put, mdb_txn_begin,
    mdb_txn_commit,
};
use rkyv::{
    Archive, Archived, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::{Failure, Strategy},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use tracing_subscriber::fmt::format;

use super::{
    date_time::{day_bounds_from_epoch_ms, make_scan_key}, event::ArchivedEvent, XaeroData
};
use crate::{core::event::Event, indexing::hash::sha_256};

pub struct LmdbEnv {
    pub env: *mut MDB_env,
    pub dbis: [MDB_dbi; 2],
}

impl LmdbEnv {
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
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
        let aof_dbi = unsafe { open_named_db(env, b"xaeroflux-aof\0".as_ptr() as *const i8)? };
        let meta_dbi = unsafe { open_named_db(env, b"xaeroflux-meta\0".as_ptr() as *const i8)? };

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

use liblmdb::{
    // from the liblmdb crate
    MDB_CREATE,
    MDB_NEXT,
    MDB_RDONLY,
    MDB_SET_RANGE,
    MDB_dbi,
    MDB_env,
    MDB_txn,
    MDB_val,
    mdb_cursor_get,
    mdb_cursor_open,
    mdb_txn_abort,
    mdb_txn_begin,
    mdb_txn_commit,
};

/// Scan all events whose 8-byte big-endian timestamp prefix is in [start_ms, end_ms).
unsafe fn scan_day(
    env: *mut MDB_env,
    dbi: MDB_dbi,
    start_ms: u64,
    end_ms: u64,
) -> anyhow::Result<()> {
    unsafe {
        // 1) Begin a read txn
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            return Err(anyhow::anyhow!("Failed to begin read txn: {}", sc_tx_begin));
        }
        // 2) Open a cursor on that DB
        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, dbi, &mut cursor);
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
        let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_SET_RANGE);
        if rc != 0 {
            // MDB_NOTFOUND or error: no items ≥ start_key
            mdb_txn_abort(rtxn);
            return Ok(());
        }

        loop {
            // 5) Extract the timestamp prefix from key_val.mv_data
            let raw_key = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);
            let ts = u64::from_be_bytes(raw_key[0..8].try_into().unwrap());
            if ts >= end_ms {
                break;
            }

            // 6) `data_val` now contains your archived bytes, you can do rkyv::archived_root on
            //    them here ...
            let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
            let archived = rkyv::api::high::access::<ArchivedEvent<Vec<u8>>, rkyv::rancor::Error>(data_slice)?;
            // 7) Advance the cursor
            let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_NEXT);
            if rc != 0 {
                // MDB_NOTFOUND = end of DB, or real error
                break;
            }
        }

        // 8) Cleanup
        mdb_txn_abort(rtxn); // read-only txn: abort is OK
        Ok(())
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
pub fn push_event(env: &LmdbEnv, event: &Event<Vec<u8>>) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        let mut txn = ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env.env, ptr::null_mut(), 0, &mut txn);
        if sc_tx_begin != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_begin)));
        }
        let dbi = env.dbis[0];
        let key = generate_key(event)?;
        let key_bytes: &[u8] = bytemuck::bytes_of(&key);
        let mdb_key = &mut MDB_val {
            mv_size: key_bytes.len(),
            mv_data: key_bytes.as_ptr() as *mut libc::c_void,
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
        let sc_tx_commit = mdb_txn_commit(txn);
        if sc_tx_commit != 0 {
            return Err(Box::new(std::io::Error::from_raw_os_error(sc_tx_commit)));
        }
    }
    Ok(())
}

/// Generate a key for the event.
/// The key is a combination of the event timestamp, event type, and a hash of the event data.
pub fn generate_key(event: &Event<Vec<u8>>) -> Result<EventKey, Failure> {
    Ok(EventKey {
        ts: event.ts,
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
