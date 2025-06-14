use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

use crossbeam::channel::{Receiver, SendError};
use threadpool::ThreadPool;
use xaeroflux_core::{
    DISPATCHER_POOL,
    date_time::emit_secs,
    event::{Event, EventType, Operator, XaeroEvent},
};

use crate::{
    indexing::storage::format::archive,
    pipe::BusKind,
    subject::{Subject, SubjectBatchContext, Subscription},
};

/// Strategy for wiring `Subject` + pipeline into threads and invoking handlers.
pub trait Materializer: Send + Sync {
    /// Materialize the pipeline, passing each processed event into `handler`.
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) + Send + Sync + 'static>,
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
        let control_rx = subject.control.sink.rx.clone();
        let data_rx = subject.data.sink.rx.clone();
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
                            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
                        ),
                        merkle_proof: None,
                    })
                    .expect("failed_to_unwrap");

                subject
                    .data
                    .sink
                    .tx
                    .send(XaeroEvent {
                        evt: Event::new(
                            data.to_vec(),
                            EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
                        ),
                        merkle_proof: None,
                    })
                    .expect("failed_to_unwrap");
            }

            if ops
                .iter()
                .any(|op| matches!(&op, Operator::BatchMode(_, _, _)))
            {
                subject.batch_mode.load(Ordering::Acquire);
            }
            let control_subject = subject.clone();
            let control_handler = handler.clone();
            let control_ops = ops.clone();
            let control_pool = pool.clone();

            std::thread::spawn(move || {
                apply_operators(
                    control_subject,
                    control_handler,
                    control_ops,
                    control_pool,
                    control_rx,
                    BusKind::Control,
                );
            });
            apply_operators(subject.clone(), handler, ops, pool, data_rx, BusKind::Data);
        });
        Subscription(jh)
    }
}

fn apply_operators(
    subject: Arc<Subject>,
    handler: Arc<dyn Fn(XaeroEvent) + Send + Sync>,
    ops: Vec<Operator>,
    pool: Arc<ThreadPool>,
    rx: Receiver<XaeroEvent>,
    bus_kind: BusKind,
) {
    let mut batch_thread_spawned = false;
    loop {
        if subject.batch_mode.load(Ordering::Acquire) {
            if !batch_thread_spawned {
                let subject_clone = subject.clone();
                let jhr = std::thread::Builder::new()
                    .name("xaero-batch-mode-materializer".to_string())
                    .spawn(move || {
                        process_batch_mode_loop(subject_clone, bus_kind);
                    });
                match jhr {
                    Ok(_) => batch_thread_spawned = true,
                    Err(e) => {
                        tracing::error!("Failed to spawn batch thread: {e:?}");
                        panic!("Failed to spawn batch thread: {e:?}"); // this should *NEVER*
                        // happen!
                    }
                }
            }
            batch_thread_spawned = true;
            // In batch mode - just wait/sleep, let batch thread do the work
            std::thread::sleep(Duration::from_millis(10));
        } else {
            batch_thread_spawned = false;
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
        };
    }
}

fn process_batch_mode_loop(subject: Arc<Subject>, bus_kind: BusKind) {
    // get batch sink rx (someone has to push to sink first) so that process is when its
    // encountered before apply operators is called i guess
    let batch_sink_rx = &subject
        .batch_context
        .as_ref()
        .expect("batch context missing")
        .pipe
        .sink
        .rx
        .clone();

    let sc = subject.clone();
    let mut sub_context = sc.batch_context.as_ref().expect("batch context missing");
    let mut buffer =
        Vec::<XaeroEvent>::with_capacity(sub_context.event_count_threshold.unwrap_or(0));
    // read from batch sink rx and buffer events until duration expires or counts do.
    for (event_count, event) in batch_sink_rx.iter().enumerate() {
        if (emit_secs() - sub_context.init_time < sub_context.duration)
            || sub_context
                .event_count_threshold
                .is_some_and(|size| size > event_count)
        {
            // buffer events
            buffer.push(event.clone());
        } else {
            break;
        }
    }
    // apply operators
    let mut f_result: Option<XaeroEvent> = None;
    for op in sub_context.pipeline.iter() {
        match op {
            Operator::Sort(f) => {
                buffer.sort_by(|a, b| f(a, b));
            }
            Operator::Fold(f) => {
                let acc = XaeroEvent {
                    evt: Default::default(),
                    merkle_proof: None,
                };
                let res = f(acc, buffer.clone());
                f_result = Some(res);
            }
            Operator::Reduce(f) => {
                let res = f(buffer.clone());
                // FIXME: Event type need to know what event type
                let res = XaeroEvent {
                    evt: Event::new(res, EventType::ApplicationEvent(1).to_u8()),
                    merkle_proof: None,
                };
                f_result = Some(res);
            }
            Operator::Blackhole => {
                subject.batch_mode.store(false, Ordering::Release);
            }
            _ => {
                tracing::warn!("do not care about other operators");
            }
        }
    }
    match f_result {
        Some(xae) => {
            shuttle_processed_event_back(&subject.clone(), bus_kind, xae);
        }
        None => {
            tracing::warn!("nothing to do");
        }
    }
    subject.batch_mode.store(false, Ordering::Release);
}

fn shuttle_processed_event_back(subject: &Arc<Subject>, bus_kind: BusKind, res: XaeroEvent) {
    match bus_kind {
        BusKind::Control => {
            let res = subject.control.sink.tx.send(res);
            match res {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("failed to send control event: {e:?}");
                }
            }
        }
        BusKind::Data => {
            let res = subject.data.sink.tx.send(res);
            match res {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("failed to send data event: {e:?}");
                }
            }
        }
    }
}
