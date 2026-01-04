// src/bin/xaeroflux_bootstrap.rs
// Bootstrap server for XaeroFlux peer discovery
//
// Build: cargo build --release --bin xaeroflux_bootstrap --target x86_64-unknown-linux-gnu
// Run:   RELAY_URL=https://quic.dev.cyan.blockxaero.io ./xaeroflux_bootstrap
//
// Environment variables:
//   RELAY_URL     - Custom relay URL (default: uses iroh public relays)
//   DISCOVERY_KEY - Discovery key (default: cyan-dev)
//   DB_PATH       - SQLite database path (default: /opt/cyan/data/bootstrap.db)
//   NO_N0         - Set to "1" to disable n0's public DNS discovery

use std::env;
use xaeroflux::XaeroFlux;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("xaeroflux=info".parse()?)
                .add_directive("iroh=warn".parse()?)
                .add_directive("iroh_gossip=info".parse()?),
        )
        .init();

    // Parse config from environment
    let relay_url = env::var("RELAY_URL").ok();
    let discovery_key = env::var("DISCOVERY_KEY").unwrap_or_else(|_| "cyan-dev".to_string());
    let db_path = env::var("DB_PATH").unwrap_or_else(|_| "/opt/cyan/data/bootstrap.db".to_string());
    let no_n0 = env::var("NO_N0").map(|v| v == "1").unwrap_or(false);

    println!("🚀 XaeroFlux Bootstrap Server starting...");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("   Discovery Key: {}", discovery_key);
    println!("   DB Path:       {}", db_path);
    println!("   Relay URL:     {}", relay_url.as_deref().unwrap_or("(default iroh relays)"));
    println!("   N0 Discovery:  {}", if no_n0 { "disabled" } else { "enabled" });
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Ensure data directory exists
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Build XaeroFlux
    let mut builder = XaeroFlux::builder()
        .discovery_key(&discovery_key)
        .db_path(&db_path);

    if let Some(url) = relay_url {
        builder = builder.relay_url(url);
    }

    if no_n0 {
        builder = builder.no_n0_discovery();
    }

    let xf = builder.build().await?;

    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  ✅ BOOTSTRAP NODE RUNNING");
    println!();
    println!("  NODE ID: {}", xf.node_id);
    println!();
    println!("  Add this to cyan-backend as bootstrap peer!");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();

    // Stay alive and log events
    let mut event_rx = xf.event_rx;
    let mut heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(60));

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                println!("📨 Event received: {} from {} ({} bytes)",
                    &event.id[..8],
                    &event.source[..8],
                    event.payload.len()
                );
            }
            _ = heartbeat.tick() => {
                println!("💓 Heartbeat - Node ID: {}", xf.node_id);
            }
        }
    }
}