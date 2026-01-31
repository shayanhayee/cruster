//! Chess Cluster - Distributed Chess Server
//!
//! Run with: `cargo run --package chess-cluster`

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive("chess_cluster=info".parse()?))
        .init();

    tracing::info!("Chess Cluster starting...");

    // TODO: Parse CLI args, load config, start cluster
    // This will be implemented in M3

    tracing::info!("Chess Cluster shutdown");
    Ok(())
}
