use anyhow::Result;
use clap::{Parser, Subcommand};
use xaeroflux::{generate_event_id, Event, XaeroFlux};

#[derive(Parser)]
#[command(name = "xaeroflux-cli")]
#[command(about = "XaeroFlux CLI - P2P Event Log Demo", long_about = None)]
struct Cli {
    /// Discovery key - peers with same key sync together
    #[arg(short = 'k', long, default_value = "demo")]
    discovery_key: String,

    /// Database file path
    #[arg(short = 'd', long, default_value = "xaeroflux.db")]
    db: String,

    /// Bootstrap peers (EndpointId strings)
    #[arg(long = "bootstrap", value_name = "ENDPOINT_ID")]
    bootstrap: Vec<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send a single event
    Send {
        /// Event payload (any text)
        message: String,
    },
    /// Receive and print events (runs forever)
    Recv,
    /// Interactive mode - send multiple events
    Interactive,
    /// Show node ID and exit
    Info,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("xaeroflux=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Send { message } => {
            send_event(&cli.discovery_key, &cli.db, &cli.bootstrap, message).await?;
        }
        Commands::Recv => {
            receive_events(&cli.discovery_key, &cli.db, &cli.bootstrap).await?;
        }
        Commands::Interactive => {
            interactive_mode(&cli.discovery_key, &cli.db, &cli.bootstrap).await?;
        }
        Commands::Info => {
            show_info(&cli.discovery_key, &cli.db, &cli.bootstrap).await?;
        }
    }

    Ok(())
}

async fn send_event(
    discovery_key: &str,
    db_path: &str,
    bootstrap: &[String],
    message: String,
) -> Result<()> {
    println!("Initializing XaeroFlux...");
    let xf = XaeroFlux::new_with_bootstrap(
        discovery_key.to_string(),
        db_path.to_string(),
        bootstrap.to_vec(),
    )
        .await?;

    let now = chrono::Utc::now().timestamp() as u64;
    let event = Event {
        id: generate_event_id(&message, &xf.node_id, now),
        payload: message.clone(),
        source: xf.node_id.clone(),
        ts: now,
    };

    println!("Sending event...");
    println!("  ID: {}", event.id);
    println!("  Payload: {}", event.payload);
    println!("  Source: {}", event.source);

    xf.event_tx.send(event)?;

    // Give it a moment to broadcast
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    println!("✓ Event sent and broadcast");

    Ok(())
}

async fn receive_events(
    discovery_key: &str,
    db_path: &str,
    bootstrap: &[String],
) -> Result<()> {
    println!("Initializing XaeroFlux...");
    let mut xf = XaeroFlux::new_with_bootstrap(
        discovery_key.to_string(),
        db_path.to_string(),
        bootstrap.to_vec(),
    )
        .await?;

    println!("Node ID: {}", xf.node_id);
    println!("Discovery Key: {}", discovery_key);
    println!("Listening for events... (Ctrl+C to exit)\n");

    while let Some(event) = xf.event_rx.recv().await {
        let timestamp = chrono::DateTime::from_timestamp(event.ts as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "unknown".to_string());

        println!("ID: {}", event.id);
        println!("Source: {}", event.source);
        println!("Time: {}", timestamp);
        println!("Payload: {}", event.payload);
        println!();
    }

    Ok(())
}

async fn interactive_mode(
    discovery_key: &str,
    db_path: &str,
    bootstrap: &[String],
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};

    println!("Initializing XaeroFlux...");
    let mut xf = XaeroFlux::new_with_bootstrap(
        discovery_key.to_string(),
        db_path.to_string(),
        bootstrap.to_vec(),
    )
        .await?;

    println!("Node ID: {}", xf.node_id);
    println!("Discovery Key: {}", discovery_key);
    println!("\nInteractive Mode - Type messages and press Enter");
    println!("Type 'quit' to exit\n");

    let node_id = xf.node_id.clone();
    let tx = xf.event_tx.clone();

    // Spawn receiver task
    tokio::spawn(async move {
        while let Some(event) = xf.event_rx.recv().await {
            println!("\n[{}] {}", &event.source[..8], event.payload);
            print!("> ");
            use std::io::Write;
            std::io::stdout().flush().unwrap();
        }
    });

    // Read from stdin
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    print!("> ");
    use std::io::Write;
    std::io::stdout().flush()?;

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                println!("\nEOF, exiting");
                break;
            }
            Ok(_) => {
                let message = line.trim().to_string();
                if message == "quit" {
                    println!("Exiting...");
                    break;
                }

                if !message.is_empty() {
                    let now = chrono::Utc::now().timestamp() as u64;
                    let event = Event {
                        id: generate_event_id(&message, &node_id, now),
                        payload: message,
                        source: node_id.clone(),
                        ts: now,
                    };

                    if let Err(e) = tx.send(event) {
                        eprintln!("Failed to send event: {}", e);
                        break;
                    }
                }

                print!("> ");
                std::io::stdout().flush()?;
            }
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                break;
            }
        }
    }

    Ok(())
}

async fn show_info(
    discovery_key: &str,
    db_path: &str,
    bootstrap: &[String],
) -> Result<()> {
    println!("Initializing XaeroFlux...");
    let xf = XaeroFlux::new_with_bootstrap(
        discovery_key.to_string(),
        db_path.to_string(),
        bootstrap.to_vec(),
    )
        .await?;

    println!("\n╔═══════════════════════════════════════════════════════╗");
    println!("║              XaeroFlux Node Info                      ║");
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ Node ID:        {:<38} ║", &xf.node_id[..38]);
    println!("║ Discovery Key:  {:<38} ║", discovery_key);
    println!("║ Database:       {:<38} ║", db_path);
    println!("╚═══════════════════════════════════════════════════════╝\n");

    println!("Topic: xsp-1.0/{}/events", discovery_key);

    Ok(())
}