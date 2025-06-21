use std::{
    hint::spin_loop,
    sync::{
        atomic::{AtomicBool, Ordering}, Arc,
        Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam::{
    channel::{Receiver, SendError, Sender},
    select,
};
use threadpool::ThreadPool;
use xaeroflux_core::{
    event::{release_event, Event, EventKind, EventType, Operator, SubjectExecutionMode, XaeroEvent},
    pipe::{BusKind, Signal, SignalPipe},
    DISPATCHER_POOL,
};

use crate::{
    indexing::storage::format::archive,
    pipeline_parser::PipelineParser,
    subject::{Subject, SubjectBatchContext, Subscription},
};

/// Strategy for wiring `Subject` + pipeline into threads and invoking handlers.
pub trait Materializer: Send + Sync {
    /// Materialize the pipeline, passing each processed event into `handler`.
    fn materialize(
        &self,
        subject: Arc<Subject>,
        handler: Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static>,
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
        handler: Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync + 'static>,
    ) -> Subscription {
        let ops_scan = subject.ops.clone();

        // Search for scan operations
        let res = ops_scan.iter().any(|op| matches!(&op, Operator::Scan(_)));
        let pool = self.pool.clone();

        if res {
            // Before incoming events start coming in, we need to funnel in Subject sink with Scan
            if ops_scan.iter().any(|op| matches!(&op, Operator::Scan(_))) {
                let scan_window = ops_scan
                    .iter()
                    .find_map(|op| {
                        if let Operator::Scan(w) = op {
                            Some(w.clone())
                        } else {
                            None
                        }
                    })
                    .expect("scan_window_not_found");

                let sw = &scan_window.start.to_be_bytes();
                let data = bytemuck::bytes_of(sw);
                use xaeroflux_core::event::{EventType, SystemEventKind};

                subject
                    .control
                    .sink
                    .tx
                    .send(XaeroEvent {
                        evt: Event::new(
                            data.to_vec(),
                            EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
                        ),
                        ..Default::default()
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
                        ..Default::default()
                    })
                    .expect("failed_to_unwrap");
            }
        }

        // Parse pipeline to determine if we need batch mode
        let (batch_ops, streaming_ops) = PipelineParser::parse(&ops_scan);
        let has_batch_mode = !batch_ops.is_empty();

        let subject_clone = subject.clone();
        let handler_clone = handler.clone();
        let pool_clone = pool.clone();
        let subject_name = subject.name.clone();

        let res_jh = std::thread::Builder::new()
            .name(format!("xaeroflux-materializer-coordinator-{}", subject_name))
            .spawn(move || {
                if has_batch_mode {
                    // Create bounded streaming channels for backpressure
                    let (streaming_control_tx, streaming_control_rx) = crossbeam::channel::bounded(1000);
                    let (streaming_data_tx, streaming_data_rx) = crossbeam::channel::bounded(1000);

                    // Create shutdown coordination
                    let (shutdown_tx, shutdown_rx) = crossbeam::channel::bounded(1);

                    let batch_subject = subject_clone.clone();
                    let batch_handler = handler_clone.clone();
                    let batch_pool = pool_clone.clone();
                    let batch_shutdown_rx = shutdown_rx.clone();

                    let streaming_subject = subject_clone.clone();
                    let streaming_handler = handler_clone.clone();
                    let streaming_shutdown_rx = shutdown_rx.clone();

                    let router_subject = subject_clone.clone();
                    let router_shutdown_rx = shutdown_rx.clone();

                    // Spawn event router
                    let router_handle = std::thread::Builder::new()
                        .name(format!("xaeroflux-event-router-{}", subject_name))
                        .spawn(move || {
                            spawn_event_router(
                                router_subject,
                                streaming_control_tx,
                                streaming_data_tx,
                                router_shutdown_rx,
                            );
                        })
                        .expect("failed to spawn event router");

                    // Spawn batch loop (reads from batch_context pipes)
                    let batch_handle = std::thread::Builder::new()
                        .name(format!("xaeroflux-batch-loop-{}", subject_name))
                        .spawn(move || {
                            spin_batch_loop(batch_subject, batch_pool, batch_handler, batch_shutdown_rx);
                        })
                        .expect("failed to spawn batch loop");

                    // Spawn streaming loop (reads from streaming channels)
                    let streaming_handle = std::thread::Builder::new()
                        .name(format!("xaeroflux-streaming-loop-{}", subject_name))
                        .spawn(move || {
                            spin_streaming_loop_with_channels(
                                streaming_control_rx,
                                streaming_data_rx,
                                streaming_subject.control_signal_pipe.clone(),
                                streaming_subject.data_signal_pipe.clone(),
                                streaming_subject,
                                streaming_handler,
                                streaming_ops,
                                streaming_shutdown_rx,
                            );
                        })
                        .expect("failed to spawn streaming loop");

                    // Wait for all threads to complete
                    let _ = router_handle.join();
                    let _ = batch_handle.join();
                    let _ = streaming_handle.join();
                } else {
                    // Pure streaming mode - use original pipes directly
                    let (shutdown_tx, shutdown_rx) = crossbeam::channel::bounded(1);
                    spin_streaming_loop(
                        subject_clone.control_signal_pipe.clone(),
                        subject_clone.data_signal_pipe.clone(),
                        subject_clone,
                        handler_clone,
                        streaming_ops,
                        pool_clone,
                        shutdown_rx,
                    );
                }
            });

        match res_jh {
            Ok(jh) => Subscription(jh),
            Err(e) => {
                panic!("cannot start xaeroflux-materializer thread: {}", e)
            }
        }
    }
}

// Event Router with graceful shutdown and bounded channels
fn spawn_event_router(
    subject: Arc<Subject>,
    streaming_control_tx: Sender<XaeroEvent>,
    streaming_data_tx: Sender<XaeroEvent>,
    shutdown_rx: Receiver<()>,
) {
    let control_rx = subject.control.sink.rx.clone();
    let data_rx = subject.data.sink.rx.clone();

    loop {
        select! {
            recv(shutdown_rx) -> _ => {
                tracing::info!("Event router received shutdown signal");
                break;
            }

            recv(control_rx) -> event => {
                if let Ok(evt) = event {
                    if let Some(bc) = &subject.batch_context {
                        // Extract route_with function from BufferMode operator
                        if let Some(route_with) = extract_route_with_from_ops(&subject.ops) {
                            if route_with(&evt) {
                                // Send to batch processing (with timeout to avoid blocking)
                                if let Err(e) = bc.control_pipe.sink.tx.try_send(evt) {
                                    match e {
                                        crossbeam::channel::TrySendError::Full(_) => {
                                            tracing::warn!("Batch control channel full, dropping event");
                                        }
                                        crossbeam::channel::TrySendError::Disconnected(_) => {
                                            tracing::error!("Batch control channel disconnected");
                                            break;
                                        }
                                    }
                                }
                            } else {
                                // Send to streaming processing
                                if let Err(e) = streaming_control_tx.try_send(evt) {
                                    match e {
                                        crossbeam::channel::TrySendError::Full(_) => {
                                            tracing::warn!("Streaming control channel full, dropping event");
                                        }
                                        crossbeam::channel::TrySendError::Disconnected(_) => {
                                            tracing::error!("Streaming control channel disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            // No route_with function, send to streaming
                            if let Err(e) = streaming_control_tx.try_send(evt) {
                                match e {
                                    crossbeam::channel::TrySendError::Full(_) => {
                                        tracing::warn!("Streaming control channel full, dropping event");
                                    }
                                    crossbeam::channel::TrySendError::Disconnected(_) => {
                                        tracing::error!("Streaming control channel disconnected");
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // No batch mode, send directly to streaming
                        if let Err(e) = streaming_control_tx.try_send(evt) {
                            match e {
                                crossbeam::channel::TrySendError::Full(_) => {
                                    tracing::warn!("Streaming control channel full, dropping event");
                                }
                                crossbeam::channel::TrySendError::Disconnected(_) => {
                                    tracing::error!("Streaming control channel disconnected");
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    // Control channel closed
                    break;
                }
            }

            recv(data_rx) -> event => {
                if let Ok(evt) = event {
                    if let Some(bc) = &subject.batch_context {
                        // Extract route_with function from BufferMode operator
                        if let Some(route_with) = extract_route_with_from_ops(&subject.ops) {
                            if route_with(&evt) {
                                // Send to batch processing
                                if let Err(e) = bc.data_pipe.sink.tx.try_send(evt) {
                                    match e {
                                        crossbeam::channel::TrySendError::Full(_) => {
                                            tracing::warn!("Batch data channel full, dropping event");
                                        }
                                        crossbeam::channel::TrySendError::Disconnected(_) => {
                                            tracing::error!("Batch data channel disconnected");
                                            break;
                                        }
                                    }
                                }
                            } else {
                                // Send to streaming processing
                                if let Err(e) = streaming_data_tx.try_send(evt) {
                                    match e {
                                        crossbeam::channel::TrySendError::Full(_) => {
                                            tracing::warn!("Streaming data channel full, dropping event");
                                        }
                                        crossbeam::channel::TrySendError::Disconnected(_) => {
                                            tracing::error!("Streaming data channel disconnected");
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            // No route_with function, send to streaming
                            if let Err(e) = streaming_data_tx.try_send(evt) {
                                match e {
                                    crossbeam::channel::TrySendError::Full(_) => {
                                        tracing::warn!("Streaming data channel full, dropping event");
                                    }
                                    crossbeam::channel::TrySendError::Disconnected(_) => {
                                        tracing::error!("Streaming data channel disconnected");
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        // No batch mode, send directly to streaming
                        if let Err(e) = streaming_data_tx.try_send(evt) {
                            match e {
                                crossbeam::channel::TrySendError::Full(_) => {
                                    tracing::warn!("Streaming data channel full, dropping event");
                                }
                                crossbeam::channel::TrySendError::Disconnected(_) => {
                                    tracing::error!("Streaming data channel disconnected");
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    // Data channel closed
                    break;
                }
            }
        }
    }

    tracing::info!("Event router shutting down gracefully");
}

type RouteWithFn = Arc<dyn Fn(&XaeroEvent) -> bool + Send + Sync + 'static>;
// Helper function to extract route_with function from BufferMode operator
fn extract_route_with_from_ops(ops: &[Operator]) -> Option<RouteWithFn> {
    for op in ops {
        if let Operator::BufferMode(_, _, _, route_with) = op {
            return Some(route_with.clone());
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
// Streaming loop with channels, blackhole implementation, and graceful shutdown
fn spin_streaming_loop_with_channels(
    control_rx: Receiver<XaeroEvent>,
    data_rx: Receiver<XaeroEvent>,
    control_streaming_signal_pipe: Arc<SignalPipe>,
    data_streaming_signal_pipe: Arc<SignalPipe>,
    subject: Arc<Subject>,
    handler: Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>,
    ops: Vec<Operator>,
    shutdown_rx: Receiver<()>,
) {
    let control_streaming_signal_rx_clone = control_streaming_signal_pipe.sink.rx.clone();
    let data_streaming_signal_rx_clone = data_streaming_signal_pipe.sink.rx.clone();

    // Blackhole state
    let mut control_blackhole = false;
    let mut data_blackhole = false;

    loop {
        select! {
            recv(shutdown_rx) -> _ => {
                tracing::info!("Streaming loop received shutdown signal");
                break;
            }

            recv(control_streaming_signal_rx_clone) -> signal => {
                match signal {
                    Ok(sig) => {
                        match sig {
                            Signal::Blackhole | Signal::ControlBlackhole => {
                                control_blackhole = true;
                                subject.mode_set.store(Signal::ControlBlackhole, Ordering::SeqCst);
                                tracing::info!("Control blackhole activated - dropping all control events");

                                // Drain all pending control events
                                while let Ok(evt) = control_rx.try_recv() {
                                    release_event(evt, EventKind::Control);
                                }
                            }
                            Signal::ControlKill => {
                                tracing::warn!("Kill switch called ending streaming loop!");
                                subject.mode_set.store(Signal::ControlKill, Ordering::SeqCst);
                                break;
                            }
                            unknown => panic!("invalid signal sent to control_signal_streaming_rx {unknown:?}")
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Control signal channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(data_streaming_signal_rx_clone) -> signal => {
                match signal {
                    Ok(sig) => {
                        match sig {
                            Signal::Blackhole => {
                                data_blackhole = true;
                                subject.mode_set.store(Signal::Blackhole, Ordering::SeqCst);
                                tracing::info!("Data blackhole activated - dropping all data events");

                                // Drain all pending data events
                                while let Ok(evt) = data_rx.try_recv() {
                                    release_event(evt, EventKind::Data);
                                }
                            }
                            Signal::Kill => {
                                tracing::warn!("Kill switch called ending streaming loop!");
                                subject.mode_set.store(Signal::Kill, Ordering::SeqCst);
                                break;
                            }
                            unknown => panic!("invalid signal sent to data_signal_streaming_rx {unknown:?}")
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Data signal channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(control_rx) -> eventrs => {
                match eventrs {
                    Ok(current_event) => {
                        if control_blackhole {
                            // Drop the event in blackhole mode
                            release_event(current_event, EventKind::Control);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject, EventKind::Control);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Control streaming channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(data_rx) -> eventrs => {
                match eventrs {
                    Ok(current_event) => {
                        if data_blackhole {
                            // Drop the event in blackhole mode
                            release_event(current_event, EventKind::Data);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject, EventKind::Data);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Data streaming channel closed: {e:?}");
                        break;
                    }
                }
            }
        }
    }

    tracing::info!("Streaming loop shutting down gracefully");
}

// Original streaming loop for pure streaming mode with blackhole and graceful shutdown
fn spin_streaming_loop(
    control_streaming_signal_pipe: Arc<SignalPipe>,
    data_streaming_signal_pipe: Arc<SignalPipe>,
    subject: Arc<Subject>,
    handler: Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>,
    ops: Vec<Operator>,
    pool: Arc<ThreadPool>,
    shutdown_rx: Receiver<()>,
) {
    let control_rx = subject.control.sink.rx.clone();
    let data_rx = subject.data.sink.rx.clone();
    let control_streaming_signal_rx_clone = control_streaming_signal_pipe.sink.rx.clone();
    let data_streaming_signal_rx_clone = data_streaming_signal_pipe.sink.rx.clone();

    // Blackhole state
    let mut control_blackhole = false;
    let mut data_blackhole = false;

    loop {
        select! {
            recv(shutdown_rx) -> _ => {
                tracing::info!("Pure streaming loop received shutdown signal");
                break;
            }

            recv(control_streaming_signal_rx_clone) -> signal => {
                match signal {
                    Ok(sig) => {
                        match sig {
                            Signal::Blackhole | Signal::ControlBlackhole => {
                                control_blackhole = true;
                                subject.mode_set.store(Signal::ControlBlackhole, Ordering::SeqCst);
                                tracing::info!("Control blackhole activated");

                                // Drain all pending control events
                                while let Ok(evt) = control_rx.try_recv() {
                                    release_event(evt, EventKind::Control);
                                }
                            }
                            Signal::ControlKill => {
                                tracing::warn!("Kill switch called ending loop!");
                                subject.mode_set.store(Signal::ControlKill, Ordering::SeqCst);
                                break;
                            }
                            unknown => panic!("invalid signal sent to control_signal_streaming_rx {unknown:?}")
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Control signal channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(data_streaming_signal_rx_clone) -> signal => {
                match signal {
                    Ok(sig) => {
                        match sig {
                            Signal::Blackhole => {
                                data_blackhole = true;
                                subject.mode_set.store(Signal::Blackhole, Ordering::SeqCst);
                                tracing::info!("Data blackhole activated");

                                // Drain all pending data events
                                while let Ok(evt) = data_rx.try_recv() {
                                    release_event(evt, EventKind::Data);
                                }
                            }
                            Signal::Kill => {
                                tracing::warn!("Kill switch called ending loop!");
                                subject.mode_set.store(Signal::Kill, Ordering::SeqCst);
                                break;
                            }
                            unknown => panic!("invalid signal sent to data_signal_streaming_rx {unknown:?}")
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Data signal channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(control_rx) -> eventrs => {
                match eventrs {
                    Ok(current_event) => {
                        if control_blackhole {
                            release_event(current_event, EventKind::Control);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject, EventKind::Control);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Control channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(data_rx) -> eventrs => {
                match eventrs {
                    Ok(current_event) => {
                        if data_blackhole {
                            release_event(current_event, EventKind::Data);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject, EventKind::Data);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Data channel closed: {e:?}");
                        break;
                    }
                }
            }
        }
    }

    tracing::info!("Pure streaming loop shutting down gracefully");
}

// Helper function to process streaming events
fn process_streaming_event(
    mut current_event: XaeroEvent,
    ops: &[Operator],
    handler: &Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>,
    subject: &Arc<Subject>,
    event_kind: EventKind,
) {
    let mut keep_processing = true;
    let mut event_consumed = false;

    for op in ops.iter() {
        if !keep_processing || event_consumed {
            break;
        }

        match op {
            Operator::Map(f) => {
                current_event = f(current_event);
            }
            Operator::Filter(f) => {
                keep_processing = f(&current_event);
            }
            Operator::FilterMerkleProofs => {
                keep_processing = current_event.merkle_proof.is_some();
            }
            Operator::SystemActors => {
                // System actors only process in streaming mode
                match event_kind {
                    EventKind::Control => {
                        tracing::debug!("Processing control event in system actors");
                    }
                    EventKind::Data => {
                        tracing::debug!("Processing data event in system actors");
                    }
                }
            }
            Operator::Blackhole => {
                keep_processing = false;
            }
            // Skip BufferMode and TransitionTo operators in streaming
            Operator::BufferMode(..) | Operator::TransitionTo(..) => {
                // These are handled by the pipeline parser and router
                continue;
            }
            _ => {
                // Skip unsupported operators in streaming mode
                tracing::debug!("Skipping operator in streaming mode: {:?}", op);
            }
        }
    }

    // Process final event if not filtered out
    if keep_processing && !event_consumed {
        let _res = handler(current_event);
        tracing::debug!("Final streaming processing completed");
    } else if !event_consumed {
        // Event was filtered out - return to pool
        release_event(current_event, event_kind);
    }
}

fn spin_batch_loop(
    subject: Arc<Subject>,
    pool: Arc<ThreadPool>,
    handler: Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>,
    shutdown_rx: Receiver<()>,
) {
    let batch_context = &subject.batch_context;
    let bc = match batch_context {
        None => {
            panic!("batch_context is empty!");
        }
        Some(bc) => bc,
    };

    tracing::info!("batch_context = {:?}", bc);
    let ticker = crossbeam::channel::tick(Duration::from_millis(bc.duration));

    // Collect events until duration expires or count threshold is reached
    let ect = bc.event_count_threshold.unwrap_or(1000);
    let control_buffer = Arc::new(Mutex::new(Vec::with_capacity(ect)));
    let data_buffer = Arc::new(Mutex::new(Vec::with_capacity(ect)));
    let pipeline = bc.pipeline.clone();

    // Listen on batch context pipes
    let control_rx = bc.control_pipe.sink.rx.clone();
    let data_rx = bc.data_pipe.sink.rx.clone();

    loop {
        select! {
            recv(shutdown_rx) -> _ => {
                tracing::info!("Batch loop received shutdown signal");
                break;
            }

            // WINDOW FOR BUFFER HAS PASSED! CRDT Op this data : process data and control buffers
            recv(ticker) -> _ => {
                // Check if control buffer has events before processing
                let control_has_events = {
                    match control_buffer.try_lock() {
                        Ok(buff) => !buff.is_empty(),
                        Err(_) => false,
                    }
                };

                if control_has_events {
                    let control_buf_clone = control_buffer.clone();
                    let pipeline_clone = pipeline.clone();
                    let handler_clone = handler.clone();
                    let subject_clone = subject.clone();

                    pool.execute(move || {
                        let control_ops_result = apply_batch_ops(&control_buf_clone, &pipeline_clone, handler_clone);
                        match control_ops_result {
                            None => {
                                tracing::debug!("No control events to process in this batch window");
                            }
                            Some(crdt_op_res_xaero_event) => {
                                // Send result back to main subject control pipe for streaming processing
                                if let Err(e) = subject_clone.control.sink.tx.try_send(crdt_op_res_xaero_event) {
                                    match e {
                                        crossbeam::channel::TrySendError::Full(_) => {
                                            tracing::warn!("Main control channel full, dropping batch result");
                                        }
                                        crossbeam::channel::TrySendError::Disconnected(_) => {
                                            tracing::error!("Main control channel disconnected");
                                        }
                                    }
                                } else {
                                    tracing::debug!("sent control event folded / CRDT Op'd successfully");
                                }
                            }
                        }
                    });
                }

                // Check if data buffer has events before processing
                let data_has_events = {
                    match data_buffer.try_lock() {
                        Ok(buff) => !buff.is_empty(),
                        Err(_) => false,
                    }
                };

                if data_has_events {
                    let data_buf_clone = data_buffer.clone();
                    let data_pipeline_clone = pipeline.clone();
                    let data_handler_clone = handler.clone();
                    let subject_clone = subject.clone();

                    pool.execute(move || {
                        let data_ops_result = apply_batch_ops(&data_buf_clone, &data_pipeline_clone, data_handler_clone);
                        match data_ops_result {
                            None => {
                                tracing::debug!("No data events to process in this batch window");
                            }
                            Some(crdt_op_res_xaero_event) => {
                                // Send result back to main subject data pipe for streaming processing
                                if let Err(e) = subject_clone.data.sink.tx.try_send(crdt_op_res_xaero_event) {
                                    match e {
                                        crossbeam::channel::TrySendError::Full(_) => {
                                            tracing::warn!("Main data channel full, dropping batch result");
                                        }
                                        crossbeam::channel::TrySendError::Disconnected(_) => {
                                            tracing::error!("Main data channel disconnected");
                                        }
                                    }
                                } else {
                                    tracing::debug!("sent data event folded / CRDT Op'd successfully");
                                }
                            }
                        }
                    });
                }
            }

            // Receive control batch events
            recv(control_rx) -> control_event => {
                match control_event {
                    Ok(event) => {
                        let should_process_immediately = {
                            match control_buffer.try_lock() {
                                Ok(mut buff) => {
                                    buff.push(event);
                                    // Check if we've hit the count threshold
                                    ect > 0 && buff.len() >= ect
                                }
                                Err(e) => {
                                    tracing::warn!("control_buffer lock failed: {e:?}");
                                    false
                                }
                            }
                        };

                        // Process immediately if count threshold reached
                        if should_process_immediately {
                            let control_buf_clone = control_buffer.clone();
                            let pipeline_clone = pipeline.clone();
                            let handler_clone = handler.clone();
                            let subject_clone = subject.clone();

                            pool.execute(move || {
                                let control_ops_result = apply_batch_ops(&control_buf_clone, &pipeline_clone, handler_clone);
                                if let Some(crdt_op_res_xaero_event) = control_ops_result {
                                    if let Err(e) = subject_clone.control.sink.tx.try_send(crdt_op_res_xaero_event) {
                                        match e {
                                            crossbeam::channel::TrySendError::Full(_) => {
                                                tracing::warn!("Main control channel full, dropping batch result");
                                            }
                                            crossbeam::channel::TrySendError::Disconnected(_) => {
                                                tracing::error!("Main control channel disconnected");
                                            }
                                        }
                                    } else {
                                        tracing::debug!("sent control event batch (count threshold) successfully");
                                    }
                                }
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Control batch channel closed: {e:?}");
                        break;
                    }
                }
            }

            recv(data_rx) -> data_event => {
                match data_event {
                    Ok(event) => {
                        let should_process_immediately = {
                            match data_buffer.try_lock() {
                                Ok(mut buff) => {
                                    buff.push(event);
                                    // Check if we've hit the count threshold
                                    ect > 0 && buff.len() >= ect
                                }
                                Err(e) => {
                                    tracing::warn!("data_buffer lock failed: {e:?}");
                                    false
                                }
                            }
                        };

                        // Process immediately if count threshold reached
                        if should_process_immediately {
                            let data_buf_clone = data_buffer.clone();
                            let data_pipeline_clone = pipeline.clone();
                            let data_handler_clone = handler.clone();
                            let subject_clone = subject.clone();

                            pool.execute(move || {
                                let data_ops_result = apply_batch_ops(&data_buf_clone, &data_pipeline_clone, data_handler_clone);
                                if let Some(crdt_op_res_xaero_event) = data_ops_result {
                                    if let Err(e) = subject_clone.data.sink.tx.try_send(crdt_op_res_xaero_event) {
                                        match e {
                                            crossbeam::channel::TrySendError::Full(_) => {
                                                tracing::warn!("Main data channel full, dropping batch result");
                                            }
                                            crossbeam::channel::TrySendError::Disconnected(_) => {
                                                tracing::error!("Main data channel disconnected");
                                            }
                                        }
                                    } else {
                                        tracing::debug!("sent data event batch (count threshold) successfully");
                                    }
                                }
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Data batch channel closed: {e:?}");
                        break;
                    }
                }
            }
        }
    }

    tracing::info!("Batch loop shutting down gracefully");
}

fn apply_batch_ops(
    buffer: &Arc<Mutex<Vec<XaeroEvent>>>,
    pipeline: &[Operator],
    handler: Arc<dyn Fn(XaeroEvent) -> XaeroEvent + Send + Sync>,
) -> Option<XaeroEvent> {
    match buffer.try_lock() {
        Ok(mut buff) => {
            // Early return for empty buffer or empty pipeline
            if pipeline.is_empty() || buff.is_empty() {
                tracing::trace!(
                    "Skipping batch processing: empty buffer ({} events) or pipeline ({} ops)",
                    buff.len(),
                    pipeline.len()
                );
                return None;
            }

            tracing::debug!(
                "Processing batch with {} events and {} operations",
                buff.len(),
                pipeline.len()
            );

            let mut f_result: Option<XaeroEvent> = None;

            for op in pipeline.iter() {
                match op {
                    Operator::Sort(f) => {
                        tracing::trace!("Applying Sort operation to {} events", buff.len());
                        buff.sort_by(|a, b| f(a, b));
                    }
                    Operator::Fold(f) => {
                        tracing::trace!("Applying Fold operation to {} events", buff.len());
                        let acc = XaeroEvent::default();
                        let res = f(acc, buff.clone());
                        f_result = Some(res);
                    }
                    Operator::Reduce(f) => {
                        tracing::trace!("Applying Reduce operation to {} events", buff.len());
                        let res = f(buff.clone());
                        let res = XaeroEvent {
                            evt: Event::new(res, EventType::ApplicationEvent(1).to_u8()),
                            ..Default::default()
                        };
                        f_result = Some(res);
                    }
                    Operator::Blackhole => {
                        tracing::trace!("Applying Blackhole operation - dropping result");
                        return f_result.map(|xaero_event| handler(xaero_event));
                    }
                    op => {
                        tracing::warn!("Unsupported operator in batch mode: {op:?}");
                    }
                }
            }

            // Clear the buffer after processing
            let events_processed = buff.len();
            buff.clear();
            tracing::debug!("Batch processing completed, processed {} events", events_processed);

            // Process the result through the handler
            f_result.map(|xae| handler(xae))
        }
        Err(e) => {
            tracing::warn!("buffer lock failed: {e:?}");
            None
        }
    }
}

#[cfg(test)]
#[ignore]
mod materializer_tests {
    use std::{
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use crossbeam::channel;
    use xaeroflux_core::{
        event::{Event, EventType, Operator, XaeroEvent},
        init_xaero_pool,
    };
    use xaeroflux_macros::subject;

    use super::*;
    use crate::{
        materializer::{Materializer, ThreadPoolForSubjectMaterializer},
        subject::{SubjectBatchOps, SubjectStreamingOps},
    };

    // Realistic event types for collaborative document editing
    const DOC_TEXT_INSERT: u8 = 10;
    const DOC_TEXT_DELETE: u8 = 11;
    const DOC_CURSOR_MOVE: u8 = 12;
    const DOC_SELECTION_CHANGE: u8 = 13;
    const DOC_FORMAT_CHANGE: u8 = 14;
    const DOC_COMMIT_STATE: u8 = 15;

    // Event types for real-time analytics
    const ANALYTICS_PAGE_VIEW: u8 = 20;
    const ANALYTICS_CLICK: u8 = 21;
    const ANALYTICS_CONVERSION: u8 = 22;
    const ANALYTICS_SESSION_START: u8 = 23;
    const ANALYTICS_AGGREGATED_STATS: u8 = 24;

    // Event types for IoT sensor data
    const SENSOR_TEMPERATURE: u8 = 30;
    const SENSOR_HUMIDITY: u8 = 31;
    const SENSOR_PRESSURE: u8 = 32;
    const SENSOR_BATCH_READING: u8 = 33;

    fn setup() {
        let _ = env_logger::builder().is_test(true).try_init();
        init_xaero_pool();
    }

    // Document editing events
    fn create_text_insert(position: u32, text: &str) -> XaeroEvent {
        let mut data = Vec::new();
        data.extend_from_slice(&position.to_le_bytes());
        data.extend_from_slice(text.as_bytes());

        let xid = xaeroid::XaeroID {
            did_peer: [0u8; xaeroid::DID_MAX_LEN],
            did_peer_len: 897,
            secret_key: [0u8; 1281],
            _pad: [0u8; 3],
            credential: xaeroid::XaeroCredential {
                vc: [0u8; 256],
                vc_len: 0,
                proofs: [xaeroid::XaeroProof { zk_proof: [0u8; 32] }; xaeroid::MAX_PROOFS],
                proof_count: 0,
                _pad: [0u8; 1],
            },
        };

        XaeroEvent {
            evt: Event::new(data, EventType::ApplicationEvent(DOC_TEXT_INSERT).to_u8()),
            latest_ts: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("failed_to_unravel")
                    .as_millis() as u64,
            ),
            ..Default::default()
        }
    }

    fn create_cursor_move(user_id: &str, position: u32) -> XaeroEvent {
        let mut data = Vec::new();
        data.extend_from_slice(user_id.as_bytes());
        data.push(0); // null terminator
        data.extend_from_slice(&position.to_le_bytes());

        let xid = xaeroid::XaeroID {
            did_peer: [0u8; xaeroid::DID_MAX_LEN],
            did_peer_len: user_id.len() as u16,
            secret_key: [0u8; 1281],
            _pad: [0u8; 3],
            credential: xaeroid::XaeroCredential {
                vc: [0u8; 256],
                vc_len: 0,
                proofs: [xaeroid::XaeroProof { zk_proof: [0u8; 32] }; xaeroid::MAX_PROOFS],
                proof_count: 0,
                _pad: [0u8; 1],
            },
        };

        XaeroEvent {
            evt: Event::new(data, EventType::ApplicationEvent(DOC_CURSOR_MOVE).to_u8()),
            latest_ts: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("failed_to_unravel")
                    .as_millis() as u64,
            ),
            ..Default::default()
        }
    }

    // Analytics events
    fn create_page_view(page: &str, user_id: &str) -> XaeroEvent {
        let mut data = Vec::new();
        data.extend_from_slice(page.as_bytes());
        data.push(0);
        data.extend_from_slice(user_id.as_bytes());

        let xid = xaeroid::XaeroID {
            did_peer: [0u8; xaeroid::DID_MAX_LEN],
            did_peer_len: user_id.len() as u16,
            secret_key: [0u8; 1281],
            _pad: [0u8; 3],
            credential: xaeroid::XaeroCredential {
                vc: [0u8; 256],
                vc_len: 0,
                proofs: [xaeroid::XaeroProof { zk_proof: [0u8; 32] }; xaeroid::MAX_PROOFS],
                proof_count: 0,
                _pad: [0u8; 1],
            },
        };

        XaeroEvent {
            evt: Event::new(data, EventType::ApplicationEvent(ANALYTICS_PAGE_VIEW).to_u8()),
            latest_ts: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("failed_to_unravel")
                    .as_millis() as u64,
            ),
            ..Default::default()
        }
    }

    fn create_click_event(element: &str, x: u16, y: u16) -> XaeroEvent {
        let mut data = Vec::new();
        data.extend_from_slice(element.as_bytes());
        data.push(0);
        data.extend_from_slice(&x.to_le_bytes());
        data.extend_from_slice(&y.to_le_bytes());

        XaeroEvent {
            evt: Event::new(data, EventType::ApplicationEvent(ANALYTICS_CLICK).to_u8()),
            latest_ts: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("failed_to_unravel")
                    .as_millis() as u64,
            ),
            ..Default::default()
        }
    }

    // IoT sensor events
    fn create_temperature_reading(sensor_id: &str, temp_celsius: f32) -> XaeroEvent {
        let mut data = Vec::new();
        data.extend_from_slice(sensor_id.as_bytes());
        data.push(0);
        data.extend_from_slice(&temp_celsius.to_le_bytes());

        let xid = xaeroid::XaeroID {
            did_peer: [0u8; xaeroid::DID_MAX_LEN],
            did_peer_len: sensor_id.len() as u16,
            secret_key: [0u8; 1281],
            _pad: [0u8; 3],
            credential: xaeroid::XaeroCredential {
                vc: [0u8; 256],
                vc_len: 0,
                proofs: [xaeroid::XaeroProof { zk_proof: [0u8; 32] }; xaeroid::MAX_PROOFS],
                proof_count: 0,
                _pad: [0u8; 1],
            },
        };

        XaeroEvent {
            evt: Event::new(data, EventType::ApplicationEvent(SENSOR_TEMPERATURE).to_u8()),
            latest_ts: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("failed_to_unravel")
                    .as_millis() as u64,
            ),
            ..Default::default()
        }
    }

    // Routing functions for different use cases
    fn is_document_operation(event: &XaeroEvent) -> bool {
        matches!(
            event.evt.event_type,
            EventType::ApplicationEvent(DOC_TEXT_INSERT)
                | EventType::ApplicationEvent(DOC_TEXT_DELETE)
                | EventType::ApplicationEvent(DOC_FORMAT_CHANGE)
        )
    }

    fn is_analytics_aggregatable(event: &XaeroEvent) -> bool {
        matches!(
            event.evt.event_type,
            EventType::ApplicationEvent(ANALYTICS_PAGE_VIEW)
                | EventType::ApplicationEvent(ANALYTICS_CLICK)
                | EventType::ApplicationEvent(ANALYTICS_CONVERSION)
        )
    }

    fn is_sensor_data(event: &XaeroEvent) -> bool {
        matches!(
            event.evt.event_type,
            EventType::ApplicationEvent(SENSOR_TEMPERATURE)
                | EventType::ApplicationEvent(SENSOR_HUMIDITY)
                | EventType::ApplicationEvent(SENSOR_PRESSURE)
        )
    }

    #[ignore]
    #[test]
    fn test_collaborative_document_editing() {
        setup();

        let subject = subject!("workspace/docs/object/shared_doc_1");
        let committed_operations = Arc::new(Mutex::new(Vec::new()));

        // Setup buffer for batching document operations every 200ms
        let buffered = subject.buffer(
            Duration::from_millis(200),
            Some(10), // Process immediately if 10 operations accumulate
            vec![
                // Sort operations by timestamp to maintain order
                Operator::Sort(Arc::new(|a, b| a.latest_ts.unwrap_or(0).cmp(&b.latest_ts.unwrap_or(0)))),
                // Fold operations into a commit state
                Operator::Fold(Arc::new(|_acc, events| {
                    let operation_count = events.len() as u32;
                    let system_xid = xaeroid::XaeroID {
                        did_peer: [0u8; xaeroid::DID_MAX_LEN],
                        did_peer_len: 6, // "system"
                        secret_key: [0u8; 1281],
                        _pad: [0u8; 3],
                        credential: xaeroid::XaeroCredential {
                            vc: [0u8; 256],
                            vc_len: 0,
                            proofs: [xaeroid::XaeroProof { zk_proof: [0u8; 32] }; xaeroid::MAX_PROOFS],
                            proof_count: 0,
                            _pad: [0u8; 1],
                        },
                    };
                    XaeroEvent {
                        evt: Event::new(
                            operation_count.to_le_bytes().to_vec(),
                            EventType::ApplicationEvent(DOC_COMMIT_STATE).to_u8(),
                        ),
                        latest_ts: Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("failed_to_unravel")
                                .as_millis() as u64,
                        ),
                        ..Default::default()
                    }
                })),
            ],
            Arc::new(is_document_operation),
        );

        let operations_clone = committed_operations.clone();
        let _sub = buffered.subscribe(move |event| {
            match event.evt.event_type {
                EventType::ApplicationEvent(DOC_COMMIT_STATE) => {
                    operations_clone.lock().expect("failed_to_unravel").push(event.clone());
                }
                EventType::ApplicationEvent(DOC_CURSOR_MOVE) => {
                    // Cursor moves are processed in streaming mode (immediate)
                    tracing::debug!("Cursor move processed in streaming mode");
                }
                _ => {}
            }
            event
        });

        thread::sleep(Duration::from_millis(50));

        // Simulate collaborative editing session
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(0, "Hello "))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(6, "world"))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_cursor_move("user1", 11))
            .expect("failed_to_unravel"); // Streaming
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(11, "!"))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_cursor_move("user2", 5))
            .expect("failed_to_unravel"); // Streaming

        // Wait for batch to complete
        thread::sleep(Duration::from_millis(300));

        let final_operations = committed_operations.lock().expect("failed_to_unravel");
        assert_eq!(final_operations.len(), 1, "Should have one committed batch");

        // Verify the batch contains 3 text operations (cursor moves are streaming)
        let commit_event = &final_operations[0];
        let operation_count_bytes: [u8; 4] = commit_event.evt.data[0..4].try_into().expect("failed_to_unravel");
        assert_eq!(u32::from_le_bytes(operation_count_bytes), 3);
    }

    #[ignore]
    #[test]
    fn test_real_time_analytics_aggregation() {
        setup();

        let subject = subject!("workspace/analytics/object/dashboard_main");
        let aggregated_stats = Arc::new(Mutex::new(Vec::new()));

        // Buffer analytics events every 5 seconds for aggregation
        let buffered = subject.buffer(
            Duration::from_millis(100), // Shortened for test
            None,
            vec![
                // Reduce to create aggregated statistics
                Operator::Reduce(Arc::new(|events| {
                    let page_views = events
                        .iter()
                        .filter(|e| matches!(e.evt.event_type, EventType::ApplicationEvent(ANALYTICS_PAGE_VIEW)))
                        .count() as u32;

                    let clicks = events
                        .iter()
                        .filter(|e| matches!(e.evt.event_type, EventType::ApplicationEvent(ANALYTICS_CLICK)))
                        .count() as u32;

                    let mut result = Vec::new();
                    result.extend_from_slice(&page_views.to_le_bytes());
                    result.extend_from_slice(&clicks.to_le_bytes());
                    result
                })),
            ],
            Arc::new(is_analytics_aggregatable),
        );

        let stats_clone = aggregated_stats.clone();
        let _sub = buffered.subscribe(move |event| {
            if matches!(event.evt.event_type, EventType::ApplicationEvent(1)) {
                // Reduce result
                stats_clone.lock().expect("failed_to_unravel").push(event.clone());
            }
            event
        });

        thread::sleep(Duration::from_millis(25));

        // Simulate web traffic
        subject
            .data
            .sink
            .tx
            .send(create_page_view("/home", "user1"))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_page_view("/products", "user2"))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_click_event("buy-button", 100, 200))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_page_view("/checkout", "user1"))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_click_event("menu", 50, 30))
            .expect("failed_to_unravel");

        thread::sleep(Duration::from_millis(150));

        let final_stats = aggregated_stats.lock().expect("failed_to_unravel");
        assert_eq!(final_stats.len(), 1);

        let stats_event = &final_stats[0];
        let page_views_bytes: [u8; 4] = stats_event.evt.data[0..4].try_into().expect("failed_to_unravel");
        let clicks_bytes: [u8; 4] = stats_event.evt.data[4..8].try_into().expect("failed_to_unravel");

        assert_eq!(u32::from_le_bytes(page_views_bytes), 3); // 3 page views
        assert_eq!(u32::from_le_bytes(clicks_bytes), 2); // 2 clicks
    }

    #[ignore]
    #[test]
    fn test_iot_sensor_data_batching() {
        setup();

        let subject = subject!("workspace/iot/object/greenhouse_zone_a");
        let sensor_batches = Arc::new(Mutex::new(Vec::new()));

        // Batch sensor readings every 500ms or when 5 readings accumulate
        let buffered = subject.buffer(
            Duration::from_millis(80), // Shortened for test
            Some(5),
            vec![
                // Sort by sensor ID, then timestamp
                Operator::Sort(Arc::new(|a, b| a.latest_ts.unwrap_or(0).cmp(&b.latest_ts.unwrap_or(0)))),
                // Fold into batch reading
                Operator::Fold(Arc::new(|_acc, events| {
                    let reading_count = events.len() as u32;
                    let avg_temp: f32 = events
                        .iter()
                        .filter_map(|e| {
                            if matches!(e.evt.event_type, EventType::ApplicationEvent(SENSOR_TEMPERATURE)) {
                                // Extract temperature value
                                let null_pos = e.evt.data.iter().position(|&b| b == 0)?;
                                if e.evt.data.len() >= null_pos + 5 {
                                    let temp_bytes: [u8; 4] = e.evt.data[null_pos + 1..null_pos + 5].try_into().ok()?;
                                    Some(f32::from_le_bytes(temp_bytes))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .sum::<f32>()
                        / events.len() as f32;

                    let mut data = Vec::new();
                    data.extend_from_slice(&reading_count.to_le_bytes());
                    data.extend_from_slice(&avg_temp.to_le_bytes());

                    XaeroEvent {
                        evt: Event::new(data, EventType::ApplicationEvent(SENSOR_BATCH_READING).to_u8()),
                        latest_ts: Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("failed_to_unravel")
                                .as_millis() as u64,
                        ),
                        ..Default::default()
                    }
                })),
            ],
            Arc::new(is_sensor_data),
        );

        let batches_clone = sensor_batches.clone();
        let _sub = buffered.subscribe(move |event| {
            if matches!(event.evt.event_type, EventType::ApplicationEvent(SENSOR_BATCH_READING)) {
                batches_clone.lock().expect("failed_to_unravel").push(event.clone());
            }
            event
        });

        thread::sleep(Duration::from_millis(20));

        // Simulate sensor readings from greenhouse
        subject
            .data
            .sink
            .tx
            .send(create_temperature_reading("temp_01", 22.5))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_temperature_reading("temp_02", 23.1))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_temperature_reading("temp_03", 21.8))
            .expect("failed_to_unravel");

        thread::sleep(Duration::from_millis(100));

        let final_batches = sensor_batches.lock().expect("failed_to_unravel");
        assert_eq!(final_batches.len(), 1);

        let batch_event = &final_batches[0];
        let reading_count_bytes: [u8; 4] = batch_event.evt.data[0..4].try_into().expect("failed_to_unravel");
        let avg_temp_bytes: [u8; 4] = batch_event.evt.data[4..8].try_into().expect("failed_to_unravel");

        assert_eq!(u32::from_le_bytes(reading_count_bytes), 3);
        let avg_temp = f32::from_le_bytes(avg_temp_bytes);
        assert!(
            (avg_temp - 22.47).abs() < 0.1,
            "Average temperature should be ~22.47°C, got {}",
            avg_temp
        );
    }

    #[ignore]
    #[test]
    fn test_mixed_streaming_and_batch_processing() {
        setup();

        let subject = subject!("workspace/game/object/session_match_123");
        let batch_results = Arc::new(Mutex::new(Vec::new()));
        let streaming_results = Arc::new(Mutex::new(Vec::new()));

        // Game events: batch important state changes, stream UI updates
        let mixed_subject = subject.buffer(
            Duration::from_millis(60),
            None,
            vec![Operator::Fold(Arc::new(|_acc, events| XaeroEvent {
                evt: Event::new(
                    events.len().to_le_bytes().to_vec(),
                    EventType::ApplicationEvent(DOC_COMMIT_STATE).to_u8(),
                ),
                ..Default::default()
            }))],
            Arc::new(|event: &XaeroEvent| {
                // Batch important document operations, stream cursor moves
                matches!(event.evt.event_type, EventType::ApplicationEvent(DOC_TEXT_INSERT))
            }),
        );

        let batch_clone = batch_results.clone();
        let streaming_clone = streaming_results.clone();
        let _sub = mixed_subject.subscribe(move |event| {
            match event.evt.event_type {
                EventType::ApplicationEvent(DOC_COMMIT_STATE) => {
                    batch_clone.lock().expect("failed_to_unravel").push(event.clone());
                }
                EventType::ApplicationEvent(DOC_CURSOR_MOVE) => {
                    streaming_clone.lock().expect("failed_to_unravel").push(event.clone());
                }
                _ => {}
            }
            event
        });

        thread::sleep(Duration::from_millis(20));

        // Mix of state changes (batched) and UI updates (streamed)
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(0, "player1_move"))
            .expect("failed_to_unravel"); // Batched
        subject
            .data
            .sink
            .tx
            .send(create_cursor_move("player1", 100))
            .expect("failed_to_unravel"); // Streamed
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(10, "player2_move"))
            .expect("failed_to_unravel"); // Batched
        subject
            .data
            .sink
            .tx
            .send(create_cursor_move("player2", 200))
            .expect("failed_to_unravel"); // Streamed

        thread::sleep(Duration::from_millis(100));

        let batch_res = batch_results.lock().expect("failed_to_unravel");
        let streaming_res = streaming_results.lock().expect("failed_to_unravel");

        assert_eq!(batch_res.len(), 1, "Should have one batch of state changes");
        assert_eq!(streaming_res.len(), 2, "Should have two streamed UI updates");

        // Verify batch contains 2 state changes
        let count_bytes: [u8; 8] = batch_res[0].evt.data[0..8].try_into().expect("failed_to_unravel");
        assert_eq!(i64::from_le_bytes(count_bytes), 2);
    }

    #[ignore]
    #[test]
    fn test_graceful_shutdown_with_active_batches() {
        setup();

        let subject = subject!("workspace/test/object/shutdown");
        let results = Arc::new(Mutex::new(Vec::new()));

        let buffered = subject.buffer(
            Duration::from_millis(1000), // Long window
            None,
            vec![Operator::Fold(Arc::new(|_acc, events| XaeroEvent {
                evt: Event::new(
                    events.len().to_le_bytes().to_vec(),
                    EventType::ApplicationEvent(DOC_COMMIT_STATE).to_u8(),
                ),
                ..Default::default()
            }))],
            Arc::new(is_document_operation),
        );

        let results_clone = results.clone();
        let sub = buffered.subscribe(move |event| {
            results_clone.lock().expect("failed_to_unravel").push(event.clone());
            event
        });

        thread::sleep(Duration::from_millis(20));

        // Send some events
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(0, "test"))
            .expect("failed_to_unravel");
        subject
            .data
            .sink
            .tx
            .send(create_text_insert(4, " data"))
            .expect("failed_to_unravel");

        thread::sleep(Duration::from_millis(50));

        // Drop subscription to trigger shutdown
        drop(sub);

        thread::sleep(Duration::from_millis(100));

        // System should shutdown gracefully without panics
        // This test verifies the shutdown coordination works properly
    }

    #[ignore]
    #[test]
    fn test_high_throughput_sensor_batching() {
        setup();

        let subject = subject!("workspace/iot/object/datacenter_rack_1");
        let batch_count = Arc::new(Mutex::new(0));

        // High-frequency batching for performance testing
        let buffered = subject.buffer(
            Duration::from_millis(30), // Very short window
            Some(20),                  // Process when 20 readings accumulate
            vec![Operator::Reduce(Arc::new(|events| {
                // Simple count aggregation
                (events.len() as u32).to_le_bytes().to_vec()
            }))],
            Arc::new(is_sensor_data),
        );

        let count_clone = batch_count.clone();
        let _sub = buffered.subscribe(move |event| {
            if matches!(event.evt.event_type, EventType::ApplicationEvent(1)) {
                *count_clone.lock().expect("failed_to_unravel") += 1;
            }
            event
        });

        thread::sleep(Duration::from_millis(10));

        // Send 45 sensor readings rapidly
        for i in 0..45 {
            let temp = 20.0 + (i as f32) * 0.1;
            subject
                .data
                .sink
                .tx
                .send(create_temperature_reading(&format!("sensor_{}", i % 5), temp))
                .expect("failed_to_unravel");
        }

        thread::sleep(Duration::from_millis(100));

        let final_count = *batch_count.lock().expect("failed_to_unravel");
        // Should have at least 2 batches (45 events / 20 per batch = 2.25)
        // Some may be triggered by time window
        assert!(
            final_count >= 2,
            "Should have processed at least 2 batches, got {}",
            final_count
        );
    }
}
