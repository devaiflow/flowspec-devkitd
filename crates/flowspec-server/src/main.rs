use flowspec_server::platform::{poller, pump};
use flowspec_server::{config::Config, container::Container, mcp_server};
use std::sync::Arc;
use std::time::Duration;
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

    // The platform connector is entirely absent when `config.platform` is
    // unset -- a pure-OpenClaw deployment spawns nothing extra here.
    let mut connector_handles = Vec::new();
    if let (Some(client), Some(platform_config)) =
        (&container.platform_client, &container.config.platform)
    {
        let poller_shutdown = shutdown.child_token();
        connector_handles.push(tokio::spawn(poller::run(
            client.clone(),
            container.state_store.clone(),
            container.scheduler.clone(),
            poller::PollerConfig {
                poll_interval: Duration::from_secs(platform_config.poll_interval_secs),
            },
            poller_shutdown,
        )));

        let pump_shutdown = shutdown.child_token();
        connector_handles.push(tokio::spawn(pump::run(
            client.clone(),
            container.state_store.clone(),
            pump::PumpConfig {
                poll_interval: Duration::from_secs(platform_config.poll_interval_secs),
                event_batch_size: platform_config.event_batch_size,
            },
            pump_shutdown,
        )));
    }

    let listen_addr = container.config.listen_addr.clone();
    let result = mcp_server::serve(&listen_addr, container.clone(), shutdown).await;

    // Abort in-flight step/hook wait tasks; their devkitd jobs keep running
    // and are re-attached by `recover()` on the next boot.
    container.scheduler.shutdown();

    // The connector loops observe the same shutdown token (via child
    // tokens) and exit on their own; just wait for them so `main` doesn't
    // return while they're mid-request.
    for handle in connector_handles {
        let _ = handle.await;
    }

    result?;
    Ok(())
}
