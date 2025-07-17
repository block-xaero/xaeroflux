use std::{
    hint::spin_loop,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam::{
    channel::{Receiver, SendError, Sender},
    select,
};
use rusted_ring::AllocationError;
use threadpool::ThreadPool;
use xaeroflux_core::{
    DISPATCHER_POOL, XEPM, XaeroPoolManager,
    event::{EventType, Operator, SubjectExecutionMode, XaeroEvent},
    pipe::{BusKind, Signal, SignalPipe},
};

use crate::{
    pipeline_parser::PipelineParser,
    subject::{Subject, SubjectBatchContext, Subscription},
};

/// Strategy for wiring `Subject` + pipeline into threads and invoking handlers.
pub trait Materializer: Send + Sync {
    /// Materialize the pipeline, passing each processed event into `handler`.
    /// Updated to use Arc<XaeroEvent> for zero-copy ring buffer access.
    fn materialize(&self, subject: Arc<Subject>, handler: Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static>) -> Subscription;
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
    fn materialize(&self, subject: Arc<Subject>, handler: Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync + 'static>) -> Subscription {
        let ops_scan = subject.ops.clone();

        // Search for scan operations
        let res = ops_scan.iter().any(|op| matches!(&op, Operator::Scan(_)));
        let pool = self.pool.clone();

        if res {
            // Before incoming events start coming in, we need to funnel in Subject sink with Scan
            if ops_scan.iter().any(|op| matches!(&op, Operator::Scan(_))) {
                let scan_window = ops_scan
                    .iter()
                    .find_map(|op| if let Operator::Scan(w) = op { Some(w.clone()) } else { None })
                    .expect("scan_window_not_found");

                let sw = &scan_window.start.to_be_bytes();
                let data = bytemuck::bytes_of(sw);
                use xaeroflux_core::event::{EventType, SystemEventKind};

                // Create XaeroEvents using XaeroPoolManager
                let control_event = XaeroPoolManager::create_xaero_event(
                    data,
                    EventType::SystemEvent(SystemEventKind::ReplayControl).to_u8(),
                    None,
                    None,
                    None,
                    xaeroflux_core::date_time::emit_secs(),
                )
                .expect("Failed to create control event");

                let data_event = XaeroPoolManager::create_xaero_event(
                    data,
                    EventType::SystemEvent(SystemEventKind::ReplayData).to_u8(),
                    None,
                    None,
                    None,
                    xaeroflux_core::date_time::emit_secs(),
                )
                .expect("Failed to create data event");

                subject.control.sink.tx.send(control_event).expect("failed_to_unwrap");

                subject.data.sink.tx.send(data_event).expect("failed_to_unwrap");
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
                            spawn_event_router(router_subject, streaming_control_tx, streaming_data_tx, router_shutdown_rx);
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
// Updated to handle Arc<XaeroEvent>
fn spawn_event_router(subject: Arc<Subject>, streaming_control_tx: Sender<Arc<XaeroEvent>>, streaming_data_tx: Sender<Arc<XaeroEvent>>, shutdown_rx: Receiver<()>) {
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

type RouteWithFn = Arc<dyn Fn(&Arc<XaeroEvent>) -> bool + Send + Sync + 'static>;
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
// Updated to handle Arc<XaeroEvent>
fn spin_streaming_loop_with_channels(
    control_rx: Receiver<Arc<XaeroEvent>>,
    data_rx: Receiver<Arc<XaeroEvent>>,
    control_streaming_signal_pipe: Arc<SignalPipe>,
    data_streaming_signal_pipe: Arc<SignalPipe>,
    subject: Arc<Subject>,
    handler: Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync>,
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
                                    // Events are Arc<XaeroEvent>, so they'll be automatically dropped
                                    // when the Arc goes out of scope
                                    drop(evt);
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
                                    // Events are Arc<XaeroEvent>, so they'll be automatically dropped
                                    drop(evt);
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
                            drop(current_event);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject,
                                BusKind::Control);
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
                            drop(current_event);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject,
                                BusKind::Data);
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
// Updated to handle Arc<XaeroEvent>
fn spin_streaming_loop(
    control_streaming_signal_pipe: Arc<SignalPipe>,
    data_streaming_signal_pipe: Arc<SignalPipe>,
    subject: Arc<Subject>,
    handler: Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync>,
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
                                    drop(evt);
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
                                    drop(evt);
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
                            drop(current_event);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject,
                                BusKind::Control);
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
                            drop(current_event);
                        } else {
                            process_streaming_event(current_event, &ops, &handler, &subject,
                                BusKind::Data);
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
// Updated to handle Arc<XaeroEvent> and use zero-copy data access
fn process_streaming_event(
    mut current_event: Arc<XaeroEvent>,
    ops: &[Operator],
    handler: &Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync>,
    subject: &Arc<Subject>,
    event_kind: BusKind,
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
                    BusKind::Control => {
                        tracing::debug!("Processing control event in system actors");
                    }
                    BusKind::Data => {
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
    }
    // Note: No need to explicitly release events - Arc<XaeroEvent> handles reference counting
}

// Updated to handle Arc<XaeroEvent> and AllocationError
fn spin_batch_loop(subject: Arc<Subject>, pool: Arc<ThreadPool>, handler: Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync>, shutdown_rx: Receiver<()>) {
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

// Updated to handle Arc<XaeroEvent> and proper error handling with AllocationError
fn apply_batch_ops(
    buffer: &Arc<Mutex<Vec<Arc<XaeroEvent>>>>,
    pipeline: &[Operator],
    handler: Arc<dyn Fn(Arc<XaeroEvent>) -> Arc<XaeroEvent> + Send + Sync>,
) -> Option<Arc<XaeroEvent>> {
    match buffer.try_lock() {
        Ok(mut buff) => {
            // Early return for empty buffer or empty pipeline
            if pipeline.is_empty() || buff.is_empty() {
                tracing::trace!("Skipping batch processing: empty buffer ({} events) or pipeline ({} ops)", buff.len(), pipeline.len());
                return None;
            }

            tracing::debug!("Processing batch with {} events and {} operations", buff.len(), pipeline.len());

            let mut f_result: Option<Arc<XaeroEvent>> = None;

            for op in pipeline.iter() {
                match op {
                    Operator::Sort(f) => {
                        tracing::trace!("Applying Sort operation to {} events", buff.len());
                        buff.sort_by(|a, b| f(a, b));
                    }
                    Operator::Fold(f) => {
                        tracing::trace!("Applying Fold operation to {} events", buff.len());
                        let acc = Arc::new(None);
                        match f(acc, buff.clone()) {
                            Ok(result) => {
                                f_result = Some(result);
                            }
                            Err(e) => {
                                tracing::error!("Fold operation failed: {:?}", e);
                                // Continue with other operations or return early
                                continue;
                            }
                        }
                    }
                    Operator::Reduce(f) => {
                        tracing::trace!("Applying Reduce operation to {} events", buff.len());
                        match f(buff.clone()) {
                            Ok(result) => {
                                f_result = Some(result);
                            }
                            Err(e) => {
                                tracing::error!("Reduce operation failed: {:?}", e);
                                // Continue with other operations or return early
                                continue;
                            }
                        }
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

#[allow(deprecated)]
#[cfg(test)]
mod materializer_tests {
    use std::{
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use crossbeam::channel;
    use rusted_ring::{AllocationError, PoolId};
    use xaeroflux_core::{
        event::{EventType, Operator, XaeroEvent},
        init_xaero_pool,
    };
    use xaeroflux_macros::subject;

    use super::*;
    use crate::{
        materializer::{Materializer, ThreadPoolForSubjectMaterializer},
        subject::{SubjectBatchOps, SubjectStreamingOps},
    };

    // Updated test helper functions to use XaeroPoolManager
    fn create_test_event(data: &[u8], event_type: u8) -> Arc<XaeroEvent> {
        XaeroPoolManager::create_xaero_event(data, event_type, None, None, None, xaeroflux_core::date_time::emit_secs()).expect("Failed to create test event")
    }

    #[allow(clippy::type_complexity)]
    // Example updated CRDT fold operation for testing
    fn create_test_fold_op() -> Arc<dyn Fn(Arc<XaeroEvent>, Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError> + Send + Sync> {
        Arc::new(|_acc, events| {
            let count = events.len() as u32;
            XaeroPoolManager::create_xaero_event(
                &count.to_le_bytes(),
                EventType::ApplicationEvent(1).to_u8(),
                None,
                None,
                None,
                xaeroflux_core::date_time::emit_secs(),
            )
            .map_err(|e| AllocationError::EventCreation("failed to create test event"))
        })
    }

    #[allow(clippy::type_complexity)]
    // Example updated CRDT reduce operation for testing
    fn create_test_reduce_op() -> Arc<dyn Fn(Vec<Arc<XaeroEvent>>) -> Result<Arc<XaeroEvent>, AllocationError> + Send + Sync> {
        Arc::new(|events| {
            let total_size: usize = events.iter().map(|e| e.data().len()).sum();
            XaeroPoolManager::create_xaero_event(
                &(total_size as u32).to_le_bytes(),
                EventType::ApplicationEvent(2).to_u8(),
                None,
                None,
                None,
                xaeroflux_core::date_time::emit_secs(),
            )
            .map_err(|e| AllocationError::EventCreation("failed to create test event"))
        })
    }

    fn setup() {
        let _ = env_logger::builder().is_test(true).try_init();
        init_xaero_pool();
        XaeroPoolManager::init(); // Initialize ring buffer pools
    }

    #[ignore]
    #[test]
    fn test_zero_copy_data_access() {
        setup();

        let subject = subject!("workspace/test/object/zero_copy");
        let processed_data = Arc::new(Mutex::new(Vec::new()));

        let data_clone = processed_data.clone();
        let _sub = subject.subscribe(move |event| {
            // Test zero-copy data access
            let data = event.data(); // This should be zero-copy access via PooledEventPtr
            data_clone.lock().expect("failed_to_unravel").push(data.to_vec());
            event
        });

        thread::sleep(Duration::from_millis(20));

        // Send test data
        let test_data = b"zero-copy test data";
        let test_event = create_test_event(test_data, 1);
        subject.data.sink.tx.send(test_event).expect("failed to send");

        thread::sleep(Duration::from_millis(50));

        let final_data = processed_data.lock().expect("failed_to_unravel");
        assert_eq!(final_data.len(), 1);
        assert_eq!(final_data[0], test_data);
    }

    #[ignore]
    #[test]
    fn test_batch_processing_with_allocation_errors() {
        setup();

        let subject = subject!("workspace/test/object/batch_errors");
        let results = Arc::new(Mutex::new(Vec::new()));

        // Create a fold operation that might fail
        let fold_op = Arc::new(|_acc: Arc<Option<XaeroEvent>>, events: Vec<Arc<XaeroEvent>>| -> Result<Arc<XaeroEvent>, AllocationError> {
            if events.len() > 5 {
                // Simulate allocation failure for large batches
                Err(AllocationError::PoolFull(PoolId::L))
            } else {
                XaeroPoolManager::create_xaero_event(
                    &(events.len() as u32).to_le_bytes(),
                    EventType::ApplicationEvent(1).to_u8(),
                    None,
                    None,
                    None,
                    xaeroflux_core::date_time::emit_secs(),
                )
                .map_err(|e| AllocationError::EventCreation("failed to create test event"))
            }
        });

        let buffered = subject.buffer(
            Duration::from_millis(100),
            Some(3), // Small batch size
            vec![Operator::Fold(fold_op)],
            Arc::new(|_: &Arc<XaeroEvent>| true),
        );

        let results_clone = results.clone();
        let _sub = buffered.subscribe(move |event| {
            results_clone.lock().expect("failed_to_unravel").push(event.clone());
            event
        });

        thread::sleep(Duration::from_millis(20));

        // Send exactly 3 events (should succeed)
        for i in 0..3 {
            let event = create_test_event(&[i], 1);
            subject.data.sink.tx.send(event).expect("failed to send");
        }

        thread::sleep(Duration::from_millis(150));

        let final_results = results.lock().expect("failed_to_unravel");
        assert_eq!(final_results.len(), 1); // Should have one successful batch result
    }

    #[test]
    pub fn test_a() {}
}
