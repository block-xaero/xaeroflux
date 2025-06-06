use std::sync::Arc;

use crossbeam::channel::Receiver;
use threadpool::ThreadPool;
use xaeroflux_core::{DISPATCHER_POOL, event::Event};

use crate::{
    Operator, XaeroEvent,
    subject::{Subject, Subscription},
};
use crate::pipe::BusKind;

/// Strategy for wiring `Subject` + pipeline into threads and invoking handlers.
pub trait Materializer: Send + Sync {
    /// Materialize the pipeline, passing each processed event into `handler`.
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) + Send + Sync + 'static>
    ) -> Subscription;
}

/// Materializer using a shared thread pool per subject.
pub struct ThreadPoolForSubjectMaterializer {
    pool: Arc<ThreadPool>,
}

impl Default for ThreadPoolForSubjectMaterializer {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPoolForSubjectMaterializer {
    /// Builds from global configuration.
    pub fn new() -> Self {
        Self {
            pool: Arc::new(DISPATCHER_POOL.get().expect("failed to load pool").clone()),
        }
    }
}

impl Materializer for ThreadPoolForSubjectMaterializer {
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) + Send + Sync + 'static>,
    ) -> Subscription {
        let control_rx = subject.control.source.rx.clone();
        let data_rx = subject.data.source.rx.clone();
        let ops = subject.ops.clone();
        let pool = self.pool.clone();

        let jh = std::thread::spawn(move || {
            // before incoming events are starting to come in
            // we need to funnel in Subject sink with Scan
            if ops.iter().any(|op| matches!(&op, Operator::Scan(_))) {
                let scan_window = ops
                    .iter()
                    .find_map(|op| {
                        if let Operator::Scan(w) = op {
                            Some(w.clone())
                        } else {
                            None
                        }
                    })
                    .expect("scan_window_not_found");
                // fail because something is wrong!

                let sw = &scan_window.start.to_be_bytes();
                let data = bytemuck::bytes_of(sw);
                use xaeroflux_core::event::EventType;
                // Ensure Replay is imported or defined
                use xaeroflux_core::event::SystemEventKind;
                subject
                    .control
                    .sink
                    .tx
                    .send(XaeroEvent {
                        evt: Event::new(
                            data.to_vec(),
                            EventType::SystemEvent(SystemEventKind::Replay).to_u8(),
                        ),
                        merkle_proof: None,
                    })
                    .expect("failed_to_unwrap");
            }
            apply_operators(handler.clone(), ops.clone(), pool.clone(), control_rx);
            apply_operators(handler, ops, pool, data_rx);
        });
        Subscription(jh)
    }
}

fn apply_operators(
    handler: Arc<dyn Fn(XaeroEvent) + Send + Sync>,
    ops: Vec<Operator>,
    pool: Arc<ThreadPool>,
    rx: Receiver<XaeroEvent>,
) {
    for evt in rx.iter() {
        let h = handler.clone();
        let pipeline = ops.clone();
        let mut local_evt = evt.clone();
        pool.execute(move || {
            let mut keep = true;
            for op in &pipeline {
                match op {
                    Operator::Map(f) => local_evt = f(local_evt),
                    Operator::Filter(p) if !p(&local_evt) => {
                        keep = false;
                        break;
                    }
                    Operator::FilterMerkleProofs if local_evt.merkle_proof.is_none() => {
                        keep = false;
                        break;
                    }
                    Operator::Blackhole => {
                        keep = false;
                        break;
                    }
                    _ => {}
                }
            }
            if keep {
                h(local_evt)
            }
        });
    }
}
