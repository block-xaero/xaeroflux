// test_discovery.rs - Example test file
use bytemuck::Zeroable;
use xaeroflux_actors::networking::{
    discovery::DHTDiscovery,
    iroh::{IrohState, XaeroDHTDiscovery},
};
use xaeroflux_core::{P2P_RUNTIME, init_p2p_runtime};
use xaeroid::XaeroID;
use std::collections::HashSet;
use xaeroflux_core::date_time::emit_secs;

pub fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    // Initialize P2P runtime
    let p2p_runtime = init_p2p_runtime();

    println!("🚀 Starting XaeroFlux P2P Discovery Test");

    // Run everything on the P2P runtime
    p2p_runtime.block_on(async {
        // Generate or load XaeroID
        let mut xaero_id = XaeroID::zeroed();
        xaero_id.secret_key[0..8].copy_from_slice(&emit_secs().to_le_bytes());
        println!("🔑 XaeroID: {}", hex::encode(bytemuck::bytes_of(&xaero_id)));

        // Create Iroh state
        println!("🌐 Initializing Iroh endpoint...");
        let iroh_state = IrohState::new(&xaero_id).await;
        println!("📡 Node ID: {}", iroh_state.endpoint.node_id());

        // Create discovery
        let discovery = XaeroDHTDiscovery::new(iroh_state);
        println!("🔍 Discovery initialized");

        // Wait a moment for the endpoint to fully initialize
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        println!("🔍 Starting continuous local network discovery...");
        println!("💡 Press Ctrl+C to stop");
        println!("🏠 Run this on different devices on the same WiFi network");

        let mut known_peers = HashSet::new();
        let mut scan_count = 0;

        loop {
            scan_count += 1;
            println!("\n🔄 Discovery scan #{}", scan_count);

            // Discover local network peers
            match discovery.discover_local_network_peers().await {
                Ok(peers) => {
                    if !peers.is_empty() {
                        println!("🎉 Found {} XaeroFlux peers:", peers.len());

                        for (i, peer) in peers.iter().enumerate() {
                            let peer_id = hex::encode(&peer.xaero_id_hash[..8]);

                            if known_peers.insert(peer_id.clone()) {
                                println!("  🆕 NEW Peer {}: XaeroID={}, ",
                                         i + 1,
                                         peer_id,
                                         // peer.sync_state.event_count,
                                         // peer.bootstrap_priority
                                );
                            } else {
                                println!("  ♻️  Known Peer {}: XaeroID={}", i + 1, peer_id);
                            }
                        }

                        println!("📊 Total unique peers discovered: {}", known_peers.len());
                    } else {
                        println!("🔍 No XaeroFlux peers found (scan #{})", scan_count);
                        if scan_count == 1 {
                            println!("💡 Waiting for other devices to come online...");
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Discovery failed on scan #{}: {}", scan_count, e);
                }
            }

            // Wait between scans
            println!("⏳ Waiting 10 seconds before next scan...");
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    });
}