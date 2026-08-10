mod support;

use rmcp::{ServiceExt, model::CallToolRequestParams, transport::StreamableHttpClientTransport};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn ping_answers_pong_over_streamable_http() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    drop(listener); // release the ephemeral port for mcp_server::serve to rebind

    let container = support::fake_container(Vec::new(), Default::default());

    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let listen_addr = addr.to_string();
    let server = tokio::spawn(async move {
        flowspec_server::mcp_server::serve(&listen_addr, container, server_shutdown)
            .await
            .unwrap();
    });

    // Give the listener a moment to come up.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
    let client = ().serve(transport).await?;

    let result = client.call_tool(CallToolRequestParams::new("ping")).await?;

    let text = result
        .content
        .iter()
        .find_map(|c| match c {
            rmcp::model::ContentBlock::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .expect("ping returns text content");
    assert_eq!(text, "pong");

    client.cancel().await?;
    shutdown.cancel();
    server.await?;
    Ok(())
}
