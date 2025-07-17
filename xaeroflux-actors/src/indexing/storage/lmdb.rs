use std::{collections::HashMap, error::Error, ffi::CString};

use bytemuck::bytes_of;
use liblmdb::{
    MDB_cursor_op_MDB_GET_MULTIPLE, MDB_cursor_op_MDB_NEXT_MULTIPLE, MDB_dbi, MDB_env, MDB_val, mdb_cursor_get, mdb_cursor_open, mdb_cursor_txn, mdb_get, mdb_txn_begin, mdb_txn_id,
};
use rusted_ring::PooledEvent;
use xaeroflux_core::{pipe::BusKind, pool::XaeroInternalEvent};

use crate::aof::storage::{
    format::EventKey,
    lmdb::{DBI, LmdbEnv},
};

#[repr(C)]
pub struct LmdbVectorSearchDb {
    pub env: LmdbEnv,
}
impl LmdbVectorSearchDb {
    pub fn new() -> Self {
        match LmdbEnv::new("vector_search", BusKind::Data) {
            Ok(e) => LmdbVectorSearchDb { env: e },
            Err(e) => {
                panic!("Error creating LmdbVectorSearchDb due to {}", e);
            }
        }
    }

    pub fn bulk_search<const TSHIRT: usize>(&mut self, event_keys: Vec<EventKey>) -> HashMap<EventKey, &XaeroInternalEvent<{ TSHIRT }>> {
        let mut results = HashMap::new();
        for key in event_keys {
            unsafe {
                let txn = std::ptr::null_mut();
                let begin_rc = mdb_txn_begin(self.env.env, std::ptr::null_mut(), MDB_cursor_op_MDB_GET_MULTIPLE, txn);
                if begin_rc != 0 {
                    panic!("mdb_txn_begin() failed");
                }
                let key_bytes = bytes_of(&key);
                let mut mdb_key = MDB_val {
                    mv_size: key_bytes.len(),
                    mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
                };

                let mut data_val = MDB_val {
                    mv_size: 0,
                    mv_data: std::ptr::null_mut(),
                };
                let rc = mdb_get(*txn, self.env.dbis[0], &mut mdb_key, &mut data_val);
                if rc == 0 {
                    // Found the key, process the data
                    let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
                    let event = bytemuck::from_bytes::<XaeroInternalEvent<TSHIRT>>(data_slice);
                    results.insert(key, event);
                }
            }
        }
        results
    }
}
