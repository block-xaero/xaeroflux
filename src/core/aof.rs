use std::any::Any;

use anyhow::Result;

pub trait Storage: Any + Send + Sync {
    fn init(&mut self, path: &str) -> Result<()>;
    fn get(&mut self, index: usize) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, index: usize, data: &[u8]) -> Result<()>;
    fn size(&mut self) -> Result<usize>;
}

