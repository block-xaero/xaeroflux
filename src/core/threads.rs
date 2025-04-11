use std::{sync::Arc, thread};

use super::XaeroData;

/// Marker trait for worker threads
pub trait Worker<T>
where
    T: XaeroData,
{
    fn do_work(&self, work: Arc<T>) -> Result<(), String>;
}
pub struct Pool<T>
where
    T: XaeroData,
{
    pub work: crossbeam::channel::Receiver<T>,
    pub workers: Vec<thread::JoinHandle<()>>,
    pub size: usize,
}

impl<T> Pool<T> where T: XaeroData {}
