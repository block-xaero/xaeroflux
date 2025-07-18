//! Core initialization and utilities for xaeroflux.
//!
//! This module provides:
//! - Global configuration loading and access (`load_config`, `CONF`).
//! - Initialization of global thread pools for dispatch and I/O.
//! - Serialization and deserialization helpers for rkyv.
//! - Application startup (`initialize`) with logging and banner.
#![feature(trivial_bounds)]
pub mod config;
pub mod date_time;
pub mod event;
pub mod hash;
pub mod keys;
pub mod logs;
pub mod merkle_tree;
pub mod network;
pub mod pipe;
pub mod pool;
pub mod size;
pub mod sys;
pub mod system_paths;
mod vector_clock;
pub mod vector_clock_actor;

use std::{
    env,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use figlet_rs::FIGfont;
use threadpool::ThreadPool;
use tracing::info;

use crate as xaeroflux_core;
pub use crate::{config::Config, logs::init_logging};

pub static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Returns a unique, thread-safe `u64` ID.
pub fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}
/// Global, singleton configuration instance.
///
/// Initialized by `load_config` and reused thereafter.
pub static CONF: OnceLock<config::Config> = OnceLock::new();

/// Global thread pool for dispatching work to worker threads.
pub static DISPATCHER_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// Global thread pool for performing I/O-bound tasks.
pub static IO_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// `XaeroEvent` thread pool for dispatching work to worker threads.
pub static XAERO_DISPATCHER_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// Global runtime for peer-to-peer networking tasks.
pub static P2P_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

// Automatically initializes on first access
pub fn shutdown_all_pools() -> Result<(), Box<dyn std::error::Error>> {
    let dpo = DISPATCHER_POOL.get();
    match dpo.as_ref() {
        None => {
            tracing::warn!("Dispatcher pool cannot be killed because it is unavailable!")
        }
        Some(dp) => {
            dp.join();
        }
    }

    let iopo = IO_POOL.get();
    match iopo.as_ref() {
        None => {
            tracing::warn!("Dispatcher pool cannot be killed because it is unavailable!")
        }
        Some(iop) => {
            iop.join();
        }
    }

    let xdpo = XAERO_DISPATCHER_POOL.get();
    match xdpo.as_ref() {
        None => {
            tracing::warn!("Dispatcher pool cannot be killed because it is unavailable!")
        }
        Some(xdp) => {
            xdp.join();
        }
    }
    if P2P_RUNTIME.get().is_some() {
        tracing::info!("P2P runtime will shutdown automatically on process exit");
    } else {
        tracing::warn!("P2P runtime was not initialized");
    }
    Ok(())
}
/// Initializes the global P2P Tokio runtime.
///
/// Uses the `threads.num_worker_threads` setting from configuration,
/// defaulting to at least one thread, and stores it in `P2P_RUNTIME`.
pub fn init_p2p_runtime() -> &'static tokio::runtime::Runtime {
    P2P_RUNTIME.get_or_init(|| {
        let conf = CONF.get_or_init(Config::default);
        let threads = conf.threads.num_worker_threads.max(2);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .thread_name("xaeroflux-p2p")
            .build()
            .expect("Failed to create P2P runtime")
    })
}

pub fn init_xaero_pool() {
    XAERO_DISPATCHER_POOL.get_or_init(|| {
        let conf = CONF.get_or_init(Config::default);
        let no_of_worker_threads = conf.threads.num_worker_threads.max(2);
        ThreadPool::new(no_of_worker_threads)
    });
}

/// Initializes the global dispatcher thread pool.
///
/// Uses the `threads.num_worker_threads` setting from configuration,
/// defaulting to at least one thread, and stores it in `DISPATCHER_POOL`.
pub fn init_global_dispatcher_pool() {
    DISPATCHER_POOL.get_or_init(|| {
        let conf = CONF.get_or_init(Config::default);
        let no_of_worker_threads = conf.threads.num_worker_threads.max(2);
        ThreadPool::new(no_of_worker_threads)
    });
}

/// Initializes the global I/O thread pool.
///
/// Uses the `threads.num_io_threads` setting from configuration,
/// defaulting to at least one thread, and stores it in `IO_POOL`.
pub fn init_global_io_pool() {
    IO_POOL.get_or_init(|| {
        let conf = CONF.get_or_init(Config::default);
        let num_io_threads = conf.threads.num_io_threads.max(1);

        ThreadPool::new(num_io_threads)
    });
}

/// Perform global initialization of xaeroflux core.
///
/// - Loads and validates configuration (`xaeroflux.toml`).
/// - Initializes dispatcher and I/O thread pools.
/// - Sets up logging and displays startup banner.
///
/// # Panics
/// Will panic if the configuration name is not "xaeroflux".
pub fn initialize() {
    #[cfg(not(test))]
    xaeroflux_core::size::init(); // Initialize the size module
    xaeroflux_core::size::init();
    let project_root = env!("CARGO_MANIFEST_DIR");
    let cfg_path = format!("{}/xaeroflux.toml", project_root);
    unsafe { env::set_var("XAERO_CONFIG", &cfg_path) };
    let config = load_config();
    if config.name != "xaeroflux" {
        panic!("Invalid config file. Expected 'xaeroflux'.");
    }
    init_global_dispatcher_pool();
    init_global_io_pool();
    init_xaero_pool();
    init_logging(); // Initialize the logging system
    show_banner();
    info!("XaeroFlux initialized");
}

/// Load or retrieve the global configuration.
///
/// Reads the `XAERO_CONFIG` environment variable or defaults to
/// `xaeroflux.toml` in the project root, parses it via `toml`.
///
/// # Panics
/// Will panic if the file cannot be read or parsed.
pub fn load_config() -> &'static config::Config {
    CONF.get_or_init(|| {
        let path = std::env::var("XAERO_CONFIG").unwrap_or_else(|_| "xaeroflux.toml".into());
        let s = std::fs::read_to_string(path).expect("read config");
        toml::from_str(&s).expect("parse config")
    })
}

/// Display the ASCII art banner for xaeroflux startup.
///
/// Uses FIGfont to render "XAER0FLUX v.{version}" and logs it.
pub fn show_banner() {
    info!("XaeroFlux initializing...");
    let slant = FIGfont::standard().expect("load slant font");
    let v = env!("CARGO_PKG_VERSION");
    let x = format!("XAER0FLUX v. {v}");
    let s = x.as_str();
    let figure = slant.convert(s).expect("convert text");
    tracing::info!("\n{}", figure);
}
#[cfg(test)]
mod tests {
    use rkyv::{Archive, Deserialize, Serialize, rancor::Failure};
    use xaeroflux_core::event;

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
}
