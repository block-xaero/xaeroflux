use anyhow::Result;
use rocksdb::{Options, DB};
use tempfile::TempDir;
use xaeroflux::core::storage::{RocksDBStorage, Storage}; // update to match your actual crate/module

#[cfg(test)]
fn create_test_db() -> (TempDir, RocksDBStorage) {
    let tmp_dir = TempDir::new().expect("could not create temp dir");

    let db = DB::open(&Options::default(), tmp_dir.path()).expect("failed to open db");

    let storage = RocksDBStorage { db };

    (tmp_dir, storage)
}

#[cfg(test)]
pub fn test_put_and_get() -> Result<()> {
    let (_tmp_dir, mut storage) = create_test_db();

    let key = 42;
    let data = b"zeroid".to_vec();

    storage.put(key, &data)?;
    let fetched = storage.get(key)?;

    assert_eq!(fetched, Some(data));

    Ok(())
}

#[cfg(test)]
pub fn test_get_missing_key() -> Result<()> {
    let (_tmp_dir, mut storage) = create_test_db();

    let result = storage.get(12345)?;
    assert!(result.is_none());

    Ok(())
}

#[test]
fn test_size_is_nonzero_after_put() -> Result<()> {
    let (_tmp_dir, mut storage) = create_test_db();

    storage.put(1, b"hello")?;
    storage.put(2, b"world")?;

    let size = storage.size()?;

    assert!(size > 0, "Expected non-zero DB size after insert");

    Ok(())
}
