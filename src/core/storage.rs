use std::any::Any;

use anyhow::Result;
use rocksdb::Options;

pub trait Storage: Any + Send + Sync {
    fn init(&mut self, path: &str) -> Result<()>;
    fn get(&mut self, index: usize) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, index: usize, data: &[u8]) -> Result<()>;
    fn size(&mut self) -> Result<usize>;
}

pub struct RocksDBStorage {
    pub db: rocksdb::DB,
}

impl Storage for RocksDBStorage {
    fn get(&mut self, index: usize) -> Result<Option<Vec<u8>>> {
        let key = index.to_be_bytes();
        let val = self.db.get(key)?;
        Ok(val.map(|v| v.to_vec()))
    }

    fn put(&mut self, index: usize, data: &[u8]) -> Result<()> {
        let key = index.to_be_bytes();
        self.db.put(key, data)?;
        Ok(())
    }

    fn size(&mut self) -> Result<usize> {
        Ok(self
            .db
            .property_int_value("rocksdb.estimate-live-data-size")?
            .unwrap_or(0) as usize)
    }

    fn init(&mut self, path: &str) -> Result<()> {
        let d = rocksdb::DB::open(&Options::default(), path);
        match d {
            Ok(db) => {
                self.db = db;
                Ok(())
            }
            Err(e) => {
                Err(anyhow::anyhow!("Failed to open database: {}", e))
            }
        }
    }
}
