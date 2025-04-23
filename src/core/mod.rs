use std::{any::Any, fmt::Debug, sync::OnceLock};

pub mod aof;
pub mod config;
pub mod event;
pub mod listeners;

use figlet_rs::FIGfont;
use tracing::info;

use crate::logs::init_logging;

pub trait XaeroData: Any + Send + Sync + Clone + Debug + Default {}

impl<T> XaeroData for T where T: Any + Send + Sync + Clone + Debug + Default {}
pub static CONF: OnceLock<config::Config> = OnceLock::new();

/// Initialize the XaeroFlux core components here.
pub fn initialize() {
    init_logging();
    show_banner();
    load_config();
    info!("XaeroFlux initialized");
}

/// Load the configuration file and parse it.
/// The configuration file is expected to be in TOML format.
/// The default path is `xaeroflux.toml`.
/// You can override this by setting the `XAERO_CONFIG` environment variable.
pub fn load_config() -> &'static config::Config {
    CONF.get_or_init(|| {
        let path = std::env::var("XAERO_CONFIG").unwrap_or_else(|_| "xaeroflux.toml".into());
        let s = std::fs::read_to_string(path).expect("read config");
        toml::from_str(&s).expect("parse config")
    })
}

/// Shows the XaeroFlux banner using the FIGlet font.
pub fn show_banner() {
    info!("XaeroFlux initializing...");
    let slant = FIGfont::standard().expect("load slant font");
    let v = env!("CARGO_PKG_VERSION");
    let x = format!("XAER0FLUX v. {}", v);
    let s = x.as_str();
    let figure = slant.convert(s).expect("convert text");
    tracing::info!("\n{}", figure);
}
#[cfg(test)]
mod tests {
    use rkyv::{Archive, Deserialize, Serialize, rancor::Failure};

    use super::*;

    #[test]
    fn test_initialize() {
        initialize();
        assert!(CONF.get().is_some());
        assert_eq!(CONF.get().expect("failed to load config").name, "xaeroflux");
        assert_eq!(CONF.get().expect("failed to load config").version, 1_u64);
    }

    #[test]
    fn test_load_config() {
        initialize();
        let config = load_config();
        assert_eq!(config.name, "xaeroflux");
        assert_eq!(config.version, 1_u64);
    }
    #[test]
    fn test_xaero_data() {
        initialize();

        #[derive(Archive, Serialize, Deserialize, Debug, Clone, Default)]
        struct TestData {
            id: u32,
            name: String,
        }
        // No explicit implementation needed as the blanket implementation covers this.
        let data = TestData {
            id: 1,
            name: "Test".to_string(),
        };
        assert_eq!(data.id, 1);
        assert_eq!(data.name, "Test");
        // serialize
        let d = rkyv::to_bytes::<Failure>(&data).expect("failed to serialize");
        assert!(!d.is_empty());
    }
    #[test]
    fn test_event_type() {
        initialize();
        let e = event::EventType::SystemEvent(event::SystemEventKind::Start);
        let event = event::EventType::from_u8(1);
        assert_eq!(event, e);
    }
    #[test]
    fn test_event() {
        initialize();
        let data = event::EventType::from_u8(0);
        let event = event::Event::<event::EventType>::new(data.clone(), 0);
        assert_eq!(event.event_type, data);
        assert_eq!(
            event.version,
            CONF.get().expect("failed to load config").version
        );
    }
}
