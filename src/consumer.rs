
use bincode::{Decode, Encode};

use crate::event_buffer::RawEvent;

#[derive(Encode, Decode, Debug, Clone)]
pub struct Consumer {}
pub trait ConsumerOps {
    fn consume(&self, event: &RawEvent);
}

impl ConsumerOps for Consumer {
    fn consume(&self, event: &RawEvent) {
        // Implement the consume logic here
        println!("Consuming event: {:?}", event);
    }
}
