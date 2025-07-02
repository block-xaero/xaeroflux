// test_discovery.rs - Example test file
use bytemuck::Zeroable;
use xaeroflux_actors::networking::{
    discovery::DHTDiscovery,
    iroh::{IrohState, XaeroDHTDiscovery},
};
use xaeroflux_core::{P2P_RUNTIME, init_p2p_runtime};
use xaeroid::XaeroID;

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
        let xaero_id = XaeroID::zeroed(); // You might want to load from file for consistent testing
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

        println!("🔍 Starting local network discovery...");

        // Discover local network peers
        match discovery.discover_local_network_peers().await {
            Ok(peers) => {
                println!("\n🎉 Discovery Results:");
                println!("Found {} XaeroFlux peers on local network", peers.len());

                for (i, peer) in peers.iter().enumerate() {
                    println!(
                        "  Peer {}: XaeroID={}, Events={}, Priority={}",
                        i + 1,
                        hex::encode(&peer.xaero_id_hash[..8]),
                        peer.sync_state.event_count,
                        peer.bootstrap_priority
                    );
                }

                if peers.is_empty() {
                    println!("💡 To test discovery:");
                    println!("   1. Run this same program in another terminal");
                    println!("   2. Both should be on the same WiFi network");
                    println!("   3. Wait a few seconds and they should discover each other");
                }
            }
            Err(e) => {
                println!("❌ Discovery failed: {}", e);
            }
        }

        // Keep running to allow other nodes to discover us
        println!("\n⏳ Keeping node alive for 30 seconds to allow discovery by other nodes...");
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        // Run discovery one more time to see if any new peers joined
        println!("🔄 Running final discovery check...");
        match discovery.discover_local_network_peers().await {
            Ok(peers) => {
                println!("Final count: {} XaeroFlux peers", peers.len());
            }
            Err(e) => {
                println!("Final discovery failed: {}", e);
            }
        }

        println!("✅ Test completed");
    });
}

