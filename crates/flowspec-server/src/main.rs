use flowspec_server::{config::Config, container::Container, mcp_server};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::load().map_err(|e| anyhow::anyhow!("{e}"))?;
    let container = Arc::new(Container::build(config)?);

    // Boot recovery: turn every persisted `Running` run into ordinary events
    // before we start accepting new requests, so re-attach happens up front.
    container.scheduler.recover().await;

    let shutdown = CancellationToken::new();
    let ctrl_c = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrl_c.cancel();
    });

    let listen_addr = container.config.listen_addr.clone();
    let result = mcp_server::serve(&listen_addr, container.clone(), shutdown).await;

    // Abort in-flight step/hook wait tasks; their devkitd jobs keep running
    // and are re-attached by `recover()` on the next boot.
    container.scheduler.shutdown();

    result?;
    Ok(())
}
