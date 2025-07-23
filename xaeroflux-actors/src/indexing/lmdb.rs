use std::{collections::HashMap, error::Error};

use bytemuck::bytes_of;
use liblmdb::{MDB_RDONLY, MDB_txn, MDB_val, mdb_get, mdb_txn_abort, mdb_txn_begin};
use xaeroflux_core::pool::XaeroInternalEvent;

use crate::aof::storage::{format::EventKey, lmdb::LmdbEnv};

#[repr(C)]
pub struct LmdbVectorSearchDb {
    pub env: LmdbEnv,
}

impl Default for LmdbVectorSearchDb {
    fn default() -> Self {
        Self::new()
    }
}

impl LmdbVectorSearchDb {
    pub fn new() -> Self {
        match LmdbEnv::new("vector_search") {
            Ok(e) => LmdbVectorSearchDb { env: e },
            Err(e) => {
                panic!("Error creating LmdbVectorSearchDb due to {}", e);
            }
        }
    }

    /// Bulk search for multiple event keys, returning raw data as Vec<u8>
    pub fn bulk_search_raw(&self, event_keys: Vec<EventKey>) -> HashMap<EventKey, Vec<u8>> {
        let mut results = HashMap::new();

        unsafe {
            // Begin read transaction
            let mut txn: *mut MDB_txn = std::ptr::null_mut();
            let begin_rc = mdb_txn_begin(self.env.env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
            if begin_rc != 0 {
                return results; // Failed to begin transaction
            }

            // Batch read each EventKey
            for event_key in event_keys {
                let key_bytes = bytes_of(&event_key);
                let mut mdb_key = MDB_val {
                    mv_size: key_bytes.len(),
                    mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
                };

                let mut data_val = MDB_val {
                    mv_size: 0,
                    mv_data: std::ptr::null_mut(),
                };

                let rc = mdb_get(txn, self.env.dbis[0], &mut mdb_key, &mut data_val);
                if rc == 0 {
                    // Found the key, copy the data
                    let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
                    results.insert(event_key, data_slice.to_vec());
                }
                // Skip if not found (rc != 0)
            }

            mdb_txn_abort(txn); // Cleanup transaction
        }

        results
    }

    /// Bulk search for multiple event keys, returning parsed XaeroInternalEvent
    pub fn bulk_search_events<const TSHIRT: usize>(&self, event_keys: Vec<EventKey>) -> HashMap<EventKey, XaeroInternalEvent<TSHIRT>> {
        let mut results = HashMap::new();

        unsafe {
            // Begin read transaction
            let mut txn: *mut MDB_txn = std::ptr::null_mut();
            let begin_rc = mdb_txn_begin(self.env.env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
            if begin_rc != 0 {
                return results; // Failed to begin transaction
            }

            // Batch read each EventKey
            for event_key in event_keys {
                let key_bytes = bytes_of(&event_key);
                let mut mdb_key = MDB_val {
                    mv_size: key_bytes.len(),
                    mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
                };

                let mut data_val = MDB_val {
                    mv_size: 0,
                    mv_data: std::ptr::null_mut(),
                };

                let rc = mdb_get(txn, self.env.dbis[0], &mut mdb_key, &mut data_val);
                if rc == 0 {
                    // Found the key, verify size matches expected XaeroInternalEvent
                    if data_val.mv_size == std::mem::size_of::<XaeroInternalEvent<TSHIRT>>() {
                        let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

                        // Parse as XaeroInternalEvent
                        if let Ok(event) = bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT>>(data_slice) {
                            results.insert(event_key, *event);
                        }
                    }
                }
                // Skip if not found or wrong size
            }

            mdb_txn_abort(txn); // Cleanup transaction
        }

        results
    }

    /// Single key lookup for raw data
    pub fn get_raw(&self, event_key: &EventKey) -> Option<Vec<u8>> {
        unsafe {
            let mut txn: *mut MDB_txn = std::ptr::null_mut();
            let begin_rc = mdb_txn_begin(self.env.env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
            if begin_rc != 0 {
                return None;
            }

            let key_bytes = bytes_of(event_key);
            let mut mdb_key = MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
            };

            let mut data_val = MDB_val {
                mv_size: 0,
                mv_data: std::ptr::null_mut(),
            };

            let rc = mdb_get(txn, self.env.dbis[0], &mut mdb_key, &mut data_val);
            let result = if rc == 0 {
                let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);
                Some(data_slice.to_vec())
            } else {
                None
            };

            mdb_txn_abort(txn);
            result
        }
    }

    /// Single key lookup for parsed XaeroInternalEvent
    pub fn get_event<const TSHIRT: usize>(&self, event_key: &EventKey) -> Option<XaeroInternalEvent<TSHIRT>> {
        unsafe {
            let mut txn: *mut MDB_txn = std::ptr::null_mut();
            let begin_rc = mdb_txn_begin(self.env.env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
            if begin_rc != 0 {
                return None;
            }

            let key_bytes = bytes_of(event_key);
            let mut mdb_key = MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
            };

            let mut data_val = MDB_val {
                mv_size: 0,
                mv_data: std::ptr::null_mut(),
            };

            let rc = mdb_get(txn, self.env.dbis[0], &mut mdb_key, &mut data_val);
            let result = if rc == 0 && data_val.mv_size == std::mem::size_of::<XaeroInternalEvent<TSHIRT>>() {
                let data_slice = std::slice::from_raw_parts(data_val.mv_data as *const u8, data_val.mv_size);

                bytemuck::try_from_bytes::<XaeroInternalEvent<TSHIRT>>(data_slice).ok().copied()
            } else {
                None
            };

            mdb_txn_abort(txn);
            result
        }
    }

    /// Check if the database contains a specific event key
    pub fn contains_key(&self, event_key: &EventKey) -> bool {
        unsafe {
            let mut txn: *mut MDB_txn = std::ptr::null_mut();
            let begin_rc = mdb_txn_begin(self.env.env, std::ptr::null_mut(), MDB_RDONLY, &mut txn);
            if begin_rc != 0 {
                return false;
            }

            let key_bytes = bytes_of(event_key);
            let mut mdb_key = MDB_val {
                mv_size: key_bytes.len(),
                mv_data: key_bytes.as_ptr() as *mut std::os::raw::c_void,
            };

            let mut data_val = MDB_val {
                mv_size: 0,
                mv_data: std::ptr::null_mut(),
            };

            let rc = mdb_get(txn, self.env.dbis[0], &mut mdb_key, &mut data_val);
            let exists = rc == 0;

            mdb_txn_abort(txn);
            exists
        }
    }
}

#[cfg(test)]
mod tests {
    use xaeroflux_core::initialize;
    use super::*;
    use crate::aof::storage::format::EventKey;

    #[test]
    fn test_lmdb_vector_search_db_creation() {
        initialize();
        let db = LmdbVectorSearchDb::new();
        // Basic creation test - if it doesn't panic, it worked
        assert!(true);
        println!("✅ LmdbVectorSearchDb created successfully");
    }

    #[test]
    fn test_bulk_search_empty() {
        let db = LmdbVectorSearchDb::new();
        let empty_keys = vec![];
        let results = db.bulk_search_raw(empty_keys);
        assert!(results.is_empty());
        println!("✅ Empty bulk search works correctly");
    }

    #[test]
    fn test_bulk_search_nonexistent_keys() {
        let db = LmdbVectorSearchDb::new();

        let fake_keys = vec![
            EventKey {
                xaero_id_hash: [1; 32],
                vector_clock_hash: [2; 32],
                ts: 12345,
                kind: 1,
                hash: [3; 32],
            },
            EventKey {
                xaero_id_hash: [4; 32],
                vector_clock_hash: [5; 32],
                ts: 67890,
                kind: 2,
                hash: [6; 32],
            },
        ];

        let results = db.bulk_search_raw(fake_keys);
        assert!(results.is_empty());
        println!("✅ Non-existent key bulk search returns empty correctly");
    }

    #[test]
    fn test_contains_key_nonexistent() {
        let db = LmdbVectorSearchDb::new();

        let fake_key = EventKey {
            xaero_id_hash: [99; 32],
            vector_clock_hash: [88; 32],
            ts: 99999,
            kind: 99,
            hash: [77; 32],
        };

        assert!(!db.contains_key(&fake_key));
        println!("✅ Contains key check for non-existent key works correctly");
    }

    #[test]
    fn test_get_raw_nonexistent() {
        let db = LmdbVectorSearchDb::new();

        let fake_key = EventKey {
            xaero_id_hash: [11; 32],
            vector_clock_hash: [22; 32],
            ts: 11111,
            kind: 11,
            hash: [33; 32],
        };

        let result = db.get_raw(&fake_key);
        assert!(result.is_none());
        println!("✅ Get raw for non-existent key returns None correctly");
    }
}
