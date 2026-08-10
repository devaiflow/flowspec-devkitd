mod error;
mod server;

pub use error::{ErrorKind, ToolFailure};
pub use server::FlowspecServer;

use crate::container::Container;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Mounts the MCP server at `/mcp` and serves it over Streamable HTTP until
/// `shutdown` fires. Graceful: in-flight requests finish, no new ones accepted.
pub async fn serve(
    listen_addr: &str,
    container: Arc<Container>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    // allowed_hosts is DNS-rebinding protection (rmcp 3.x): any inbound Host
    // header not in this list gets 403 Forbidden. Set explicitly rather than
    // inheriting rmcp's loopback-only default, so a non-loopback listen_addr
    // (e.g. for OpenClaw over Tailscale) actually works -- see
    // docs/openclaw-validation.md.
    let allowed_hosts = container.config.allowed_hosts.clone();
    let service: StreamableHttpService<FlowspecServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(FlowspecServer::new(container.clone())),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_allowed_hosts(allowed_hosts)
                .with_cancellation_token(shutdown.child_token()),
        );

    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    tracing::info!(%listen_addr, "flowspec-devkitd listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled_owned().await })
        .await
}
