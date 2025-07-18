use std::{
    ptr,
    sync::{Arc, Mutex},
};

use liblmdb::{MDB_RDONLY, MDB_txn, MDB_val, mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_get, mdb_txn_abort, mdb_txn_begin};
use rkyv::util::AlignedVec;
use xaeroflux_core::event::EventType;

use super::{format::SegmentMeta, lmdb::LmdbEnv};

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
    let slice = unsafe { std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size) };
    let mut av = AlignedVec::new();
    av.extend_from_slice(slice);

    // abort txn
    unsafe { mdb_txn_abort(txn) };
    av
}

/// Reads all `SegmentMeta` entries from the META_DB whose composite key
/// `[ts_start (8 bytes BE) ‖ segment_index (8 bytes BE)]` is in `[start, end)`.
///
/// Returns a `Vec<SegmentMeta>` in ascending `ts_start` order.
pub fn iterate_segment_meta_by_range(env: &Arc<Mutex<LmdbEnv>>, start: u64, end: Option<u64>) -> Result<Vec<SegmentMeta>, Box<dyn std::error::Error>> {
    use liblmdb::MDB_cursor_op_MDB_NEXT;
    let mut results = Vec::<SegmentMeta>::new();
    let g = env.lock().expect("failed to lock env");
    let env = g.env;

    unsafe {
        // 1) Begin a read txn
        let mut rtxn: *mut MDB_txn = std::ptr::null_mut();
        let sc_tx_begin = mdb_txn_begin(env, std::ptr::null_mut(), MDB_RDONLY, &mut rtxn);
        if sc_tx_begin != 0 {
            // Empty or uninitialized META DB → no entries
            return Ok(results);
        }

        // 2) Open a cursor on that DB (should be meta DB, index 1)
        let mut cursor = std::ptr::null_mut();
        let sc_cursor_open = mdb_cursor_open(rtxn, g.dbis[1], &mut cursor);
        if sc_cursor_open != 0 {
            // No entries in META DB → clean up and return empty list
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        // 3) Build the start key MDB_val
        // key: [ ts_start (8) ‖ segment_index (8) ]
        let mut start_key = [0u8; 16]; // 8 for start ts, 8 for segment_index
        start_key[..8].copy_from_slice(&start.to_be_bytes());
        // The rest is already zeroed by the array initialization

        let mut key_val = MDB_val {
            mv_size: start_key.len(),
            mv_data: start_key.as_ptr() as *mut _,
        };
        let mut data_val = MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };

        // 4) Position to first key ≥ start_key
        let rc = mdb_cursor_get(cursor, &mut key_val, &mut data_val, liblmdb::MDB_cursor_op_MDB_SET_RANGE);
        if rc != 0 {
            // MDB_NOTFOUND or error: no items ≥ start_key
            mdb_txn_abort(rtxn);
            return Ok(results);
        }

        loop {
            // skip static or non-composite keys (we only want 16-byte ts_start‖segment_index keys)
            if key_val.mv_size != 16 {
                // advance cursor and continue
                let rc_skip = mdb_cursor_get(cursor, &mut key_val, &mut data_val, MDB_cursor_op_MDB_NEXT);
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
            let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
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
