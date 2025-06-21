use std::{
    ptr,
    sync::{Arc, Mutex},
};

use liblmdb::{
    MDB_RDONLY, MDB_txn, MDB_val, mdb_cursor_close, mdb_cursor_get, mdb_cursor_open, mdb_get, mdb_txn_abort,
    mdb_txn_begin,
};
use rkyv::util::AlignedVec;
use xaeroflux_core::{XaeroPoolManager, event::EventType};

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

#[cfg(test)]
mod meta_tests {
    use std::sync::{Arc, Mutex};

    use bytemuck::bytes_of;
    use tempfile::tempdir;
    use xaeroflux_core::{XaeroPoolManager, date_time::emit_secs, event::EventType, initialize};

    use super::*;
    use crate::{BusKind, aof::storage::lmdb::push_xaero_event};

    /// Helper to build a SegmentMeta with predictable fields.
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
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data).expect("failed_to_unravel"),
        ));

        let all = iterate_segment_meta_by_range(&env, 0, None).expect("failed_to_unravel");
        assert!(all.is_empty(), "expected no metas in empty DB");
    }

    #[test]
    fn single_meta_roundtrip() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Data).expect("failed_to_unravel"),
        ));

        let ts = emit_secs();
        let meta = make_meta(ts, ts + 5, 42);

        // Create XaeroEvent with SegmentMeta data
        let xaero_event = XaeroPoolManager::create_xaero_event(
            bytes_of(&meta),
            EventType::MetaEvent(1).to_u8(),
            None,
            None,
            None,
            ts,
        )
        .expect("Failed to create meta event");

        push_xaero_event(&env, &xaero_event).expect("failed to push meta");

        // open‐ended scan should return exactly this one meta
        let all = iterate_segment_meta_by_range(&env, 0, None).expect("failed_to_unravel");
        let unaligned_segment_index = all[0].segment_index;
        assert_eq!(all.len(), 1);
        assert_eq!(unaligned_segment_index, 42);

        // scanning past its ts_start should drop it
        let none = iterate_segment_meta_by_range(&env, ts + 1, None).expect("failed_to_unravel");
        assert!(none.is_empty());
    }

    #[test]
    fn multiple_meta_filter_and_ordering() {
        initialize();
        XaeroPoolManager::init();

        let dir = tempdir().expect("failed_to_unravel");
        let env = Arc::new(Mutex::new(
            LmdbEnv::new(dir.path().to_str().expect("failed_to_unravel"), BusKind::Control).expect("failed_to_unravel"),
        ));

        // create three metas at t=10,20,30
        let metas = [make_meta(10, 15, 0), make_meta(20, 25, 1), make_meta(30, 35, 2)];
        for meta in metas.iter() {
            let xaero_event = XaeroPoolManager::create_xaero_event(
                bytes_of(meta),
                EventType::MetaEvent(1).to_u8(),
                None,
                None,
                None,
                meta.ts_start,
            )
            .expect("Failed to create meta event");

            push_xaero_event(&env, &xaero_event).expect("failed to push meta");
        }

        // scan entire range → all three, in order
        let all = iterate_segment_meta_by_range(&env, 0, None).expect("failed_to_unravel");
        assert_eq!(all.len(), 3);
        assert_eq!(all.iter().map(|m| m.ts_start).collect::<Vec<_>>(), vec![10, 20, 30]);

        // scan [15..30) → only the one at ts_start=20
        let mid = iterate_segment_meta_by_range(&env, 15, Some(30)).expect("failed_to_unravel");
        let unaligned_mid = mid[0].segment_index;
        assert_eq!(mid.len(), 1);
        assert_eq!(unaligned_mid, 1);

        // scan up to 20 → include those at 10 and 20
        let upto = iterate_segment_meta_by_range(&env, 0, Some(20)).expect("failed_to_unravel");
        let unaligned_upto_0 = upto[0].segment_index;
        let unaligned_upto_1 = upto[1].segment_index;
        assert_eq!(upto.len(), 2);
        assert_eq!(unaligned_upto_0, 0);
        assert_eq!(unaligned_upto_1, 1);
    }
}
