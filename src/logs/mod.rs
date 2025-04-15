pub mod diagnostics;
pub mod probe;
pub mod wal;

use std::sync::Once;
static INIT: Once = Once::new();

#[cfg(feature = "diagnostics")]
pub fn default_log_level() -> &'static str {
    "trace"
}

#[cfg(not(feature = "diagnostics"))]
fn default_log_level() -> &'static str {
    "warn"
}

/// Single initialization function for diagnostic logging.
pub fn init_logging() {
    INIT.call_once(|| {
        let env_filter = tracing_subscriber::EnvFilter::new(default_log_level());
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .finish(),
        )
        .expect("Failed to set global default subscriber");
        tracing::trace!("Logging initialized");
    });
}
