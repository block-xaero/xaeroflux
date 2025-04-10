use super::event_buffer::RawEvent;

trait Work: Any + Send + Sync {}

/// Marker trait for worker threads
trait Worker<T>
where
    T: Work,
{
    fn do_work(&self, work: Arc<Work>) -> Result<(), String>;
}
pub struct Pool {
    pub work: crossbeam::channel::Receiver<T>,
    pub workers: Vec<thread::JoinHandle<()>>,
    pub size: usize,
}

impl Pool {
    fn io(&self, size: usize) -> usize {
        let mut i = 0;
        self.size = size;
        for i in 0..self.size {
            let handle = thread::spawn(move || loop {
                let work = self.work.recv();
                match work {
                    Ok(w) => {}
                    Err(_) => {
                        println!("Error receiving work");
                    }
                }
            });
        }
    }
}
