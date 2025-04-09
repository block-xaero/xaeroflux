use std::cell::OnceCell;

use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use OnceCell::sync::Lazy;

#[cfg(feature = "diagnostics")]
pub mod Diagostics {
    pub struct Diagnostics {
        pub logger: FmtSubscriber,
    }
    static LOG: Diagnostics = OnceCell::new();
    impl Diagnostics {
        pub fn init() -> &'static Diagnostics {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(Level::INFO)
                .finish();
            tracing::subscriber::set_global_default(subscriber.clone())
                .expect("setting default subscriber failed");
            info!("Diagnostics initialized");
            Diagnostics { logger: subscriber }
        }
    }
    pub fn get_logger() -> &'static Diagnostics {
        LOG.get_or_init(|| Diagnostics::init())
    }
}
#[cfg(not(feature = "diagnostics"))]
pub mod diagnostics {
    // Provide dummy implementations to satisfy the API.
    pub struct Diagnostics;

    impl Diagnostics {
        pub fn init() -> Self {
            Diagnostics
        }
    }
}
