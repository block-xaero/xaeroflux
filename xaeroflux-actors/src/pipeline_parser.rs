use xaeroflux_core::event::{Operator, SubjectExecutionMode};

pub struct PipelineParser;

impl PipelineParser {
    pub fn parse(ops: &[Operator]) -> (Vec<Operator>, Vec<Operator>) {
        let mut batch_operators = Vec::new();
        let mut streaming_operators = Vec::new();
        let mut mode = ProcessingMode::Streaming; // Default mode

        for op in ops {
            match op {
                Operator::BufferMode(_, _, pipeline, _) => {
                    // Add the buffer pipeline to batch operators
                    batch_operators.extend(pipeline.clone());
                    mode = ProcessingMode::Batch;
                }
                Operator::TransitionTo(SubjectExecutionMode::Buffer, SubjectExecutionMode::Streaming) => {
                    // Explicit transition from batch to streaming
                    mode = ProcessingMode::Streaming;
                }
                Operator::TransitionTo(SubjectExecutionMode::Streaming, SubjectExecutionMode::Buffer) => {
                    // Explicit transition from streaming to batch
                    mode = ProcessingMode::Batch;
                }
                _ => {
                    // Route operator based on current mode
                    match mode {
                        ProcessingMode::Batch => batch_operators.push(op.clone()),
                        ProcessingMode::Streaming => streaming_operators.push(op.clone()),
                    }
                }
            }
        }

        (batch_operators, streaming_operators)
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessingMode {
    Batch,
    Streaming,
}
