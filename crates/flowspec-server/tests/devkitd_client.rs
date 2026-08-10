//! Contract tests for `DevkitdClient` against an in-process stub devkitd MCP
//! server (rmcp 0.16 `ServerHandler`, following the `tests/hello_mcp.rs`
//! pattern). Covers the wire contract documented in `docs/devkitd-dev.md`:
//! start/poll sequencing, envelope decode, truncation, error-mapping by
//! call-site, deadline->cancel, and transport-blip backoff.

use flowspec_app::ports::{Devkitd, DevkitdError, StartRequest};
use flowspec_server::devkitd::{DevkitdClient, DevkitdClientConfig};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData as McpError, ServerHandler};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Stub devkitd server
// ---------------------------------------------------------------------------

/// What a "start" call (any tool name that isn't `job-status`/`job-cancel`)
/// does.
#[derive(Clone)]
enum StartMode {
    /// Accept, minting `job_id`.
    Accept(String),
    /// Synchronous rejection — `isError:true`, plain text, no job created.
    Reject(String),
}

#[derive(Default)]
struct StubState {
    /// (tool name, args) for every call received, in order.
    calls: Vec<(String, Option<Value>)>,
    start_mode: Option<StartMode>,
    /// job_id -> scripted status envelopes; the last entry repeats once the
    /// script is exhausted (so a terminal state stays terminal).
    job_script: HashMap<String, Vec<Value>>,
    job_progress: HashMap<String, usize>,
}

#[derive(Clone, Default)]
struct Stub {
    state: Arc<Mutex<StubState>>,
}

impl Stub {
    fn new() -> Self {
        Self::default()
    }

    fn set_start_accept(&self, job_id: impl Into<String>) {
        self.state.lock().unwrap().start_mode = Some(StartMode::Accept(job_id.into()));
    }

    fn set_start_reject(&self, message: impl Into<String>) {
        self.state.lock().unwrap().start_mode = Some(StartMode::Reject(message.into()));
    }

    fn script_job(&self, job_id: impl Into<String>, envelopes: Vec<Value>) {
        let mut guard = self.state.lock().unwrap();
        guard.job_script.insert(job_id.into(), envelopes);
    }

    fn calls(&self) -> Vec<(String, Option<Value>)> {
        self.state.lock().unwrap().calls.clone()
    }

    fn call_count(&self, tool: &str) -> usize {
        self.calls().iter().filter(|(t, _)| t == tool).count()
    }
}

impl Stub {
    async fn handle_call_tool(
        &self,
        request: CallToolRequestParams,
    ) -> Result<CallToolResult, McpError> {
        let args_value = request.arguments.clone().map(Value::Object);
        {
            let mut guard = self.state.lock().unwrap();
            guard
                .calls
                .push((request.name.to_string(), args_value.clone()));
        }

        match request.name.as_ref() {
            "job-status" => {
                let job_id = args_value
                    .as_ref()
                    .and_then(|v| v.get("job_id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let mut guard = self.state.lock().unwrap();
                let Some(script) = guard.job_script.get(&job_id).cloned() else {
                    return Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                        "unknown job_id: {job_id}"
                    ))]));
                };
                let idx = guard.job_progress.entry(job_id.clone()).or_insert(0);
                let capped = (*idx).min(script.len() - 1);
                if *idx < script.len() - 1 {
                    *idx += 1;
                }
                let envelope = script[capped].clone();
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    envelope.to_string(),
                )]))
            }
            "job-cancel" => {
                let job_id = args_value
                    .as_ref()
                    .and_then(|v| v.get("job_id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let guard = self.state.lock().unwrap();
                if guard.job_script.contains_key(&job_id) {
                    Ok(CallToolResult::success(vec![ContentBlock::text(
                        json!({"cancelled": true}).to_string(),
                    )]))
                } else {
                    Ok(CallToolResult::error(vec![ContentBlock::text(format!(
                        "unknown job_id: {job_id}"
                    ))]))
                }
            }
            _ => {
                let mode = self.state.lock().unwrap().start_mode.clone();
                match mode {
                    Some(StartMode::Accept(job_id)) => {
                        Ok(CallToolResult::success(vec![ContentBlock::text(
                            json!({"job_id": job_id}).to_string(),
                        )]))
                    }
                    Some(StartMode::Reject(message)) => {
                        Ok(CallToolResult::error(vec![ContentBlock::text(message)]))
                    }
                    None => Ok(CallToolResult::error(vec![ContentBlock::text(
                        "stub: no start_mode configured",
                    )])),
                }
            }
        }
    }
}

/// `ServerHandler::call_tool` returns a native `impl Future` (rmcp isn't
/// `#[async_trait]`-based), so `Stub`'s scriptable logic lives in a plain
/// async method and this thin newtype wires it into the trait directly.
struct StubHandler(Stub);

impl ServerHandler for StubHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default()
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResponse, McpError>> + Send + '_ {
        let fut = self.0.handle_call_tool(request);
        async move { fut.await.map(Into::into) }
    }
}

/// Spawns the stub on `addr`, returning the join handle and a shutdown token.
/// `require_token`, when set, wraps the router in bearer-auth middleware.
fn spawn_stub(
    addr: SocketAddr,
    stub: Stub,
    require_token: Option<String>,
) -> (tokio::task::JoinHandle<()>, CancellationToken) {
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();

    let handle = tokio::spawn(async move {
        let service: StreamableHttpService<StubHandler, LocalSessionManager> =
            StreamableHttpService::new(
                move || Ok(StubHandler(stub.clone())),
                Arc::new(LocalSessionManager::default()),
                StreamableHttpServerConfig::default()
                    .with_cancellation_token(server_shutdown.child_token()),
            );
        let mut router = axum::Router::new().nest_service("/mcp", service);
        if let Some(token) = require_token {
            router = router.layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let token = token.clone();
                    async move {
                        let ok = req
                            .headers()
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v == format!("Bearer {token}"))
                            .unwrap_or(false);
                        if ok {
                            next.run(req).await
                        } else {
                            axum::http::StatusCode::UNAUTHORIZED.into_response()
                        }
                    }
                },
            ));
        }
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_shutdown.cancelled_owned().await })
            .await
            .unwrap();
    });

    (handle, shutdown)
}

use axum::response::IntoResponse;

async fn reserve_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn start_stub(stub: Stub) -> (SocketAddr, tokio::task::JoinHandle<()>, CancellationToken) {
    let addr = reserve_addr().await;
    let (handle, shutdown) = spawn_stub(addr, stub, None);
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle, shutdown)
}

fn client_config(addr: SocketAddr) -> DevkitdClientConfig {
    let mut config = DevkitdClientConfig::new(format!("http://{addr}/mcp"));
    config.poll_interval = Duration::from_millis(10);
    config
}

fn noop_liveness() -> flowspec_app::ports::LivenessSink {
    Arc::new(|| {})
}

fn done_envelope(exit_code: i64, stdout: &str, stderr: &str) -> Value {
    json!({ "state": "done", "stdout": stdout, "stderr": stderr, "exit_code": exit_code })
}

// ---------------------------------------------------------------------------
// 1-3: start()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_sends_timeout_as_string_and_passes_args_through_unstringified() {
    let stub = Stub::new();
    stub.set_start_accept("job-1");
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let handle = client
        .start(StartRequest {
            tool: "agent-run".to_string(),
            args: json!({ "cli": "opencode", "count": 3 }),
            timeout_seconds: Some(42),
        })
        .await
        .unwrap();
    assert_eq!(handle.0, "job-1");

    let calls = stub.calls();
    let (_, args) = calls.iter().find(|(t, _)| t == "agent-run").unwrap();
    let args = args.clone().unwrap();
    assert_eq!(args["_timeout_seconds"], Value::String("42".to_string()));
    // Non-scalar / non-string values pass through un-stringified.
    assert_eq!(args["count"], json!(3));
    assert_eq!(args["cli"], json!("opencode"));

    shutdown.cancel();
}

#[tokio::test]
async fn start_without_timeout_omits_arg() {
    let stub = Stub::new();
    stub.set_start_accept("job-1");
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    client
        .start(StartRequest {
            tool: "agent-run".to_string(),
            args: json!({ "cli": "opencode" }),
            timeout_seconds: None,
        })
        .await
        .unwrap();

    let calls = stub.calls();
    let (_, args) = calls.iter().find(|(t, _)| t == "agent-run").unwrap();
    let args = args.clone().unwrap();
    assert!(args.get("_timeout_seconds").is_none());

    shutdown.cancel();
}

#[tokio::test]
async fn start_drops_null_valued_args() {
    let stub = Stub::new();
    stub.set_start_accept("job-1");
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    client
        .start(StartRequest {
            tool: "agent-run".to_string(),
            args: json!({ "cli": "opencode", "verbose": Value::Null }),
            timeout_seconds: None,
        })
        .await
        .unwrap();

    let calls = stub.calls();
    let (_, args) = calls.iter().find(|(t, _)| t == "agent-run").unwrap();
    let args = args.clone().unwrap();
    assert!(
        args.get("verbose").is_none(),
        "null-valued arg must be omitted, not sent as null: {args:?}"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn start_sync_rejection_maps_to_tool_error() {
    let stub = Stub::new();
    stub.set_start_reject("missing required argument: cli");
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let err = client
        .start(StartRequest {
            tool: "agent-run".to_string(),
            args: json!({}),
            timeout_seconds: None,
        })
        .await
        .unwrap_err();

    match err {
        DevkitdError::ToolError {
            exit_code, stderr, ..
        } => {
            assert_eq!(exit_code, -1);
            assert!(stderr.contains("missing required argument"));
        }
        other => panic!("expected ToolError, got {other:?}"),
    }
    assert_eq!(
        stub.call_count("job-status"),
        0,
        "no job created on rejection"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// 4-13: wait()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wait_polls_until_done_reporting_liveness() {
    let stub = Stub::new();
    stub.script_job(
        "job-1",
        vec![
            json!({"state": "received"}),
            json!({"state": "running"}),
            json!({"state": "running"}),
            done_envelope(0, "ok", ""),
        ],
    );
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let pulses = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let pulses_clone = pulses.clone();
    let liveness: flowspec_app::ports::LivenessSink = Arc::new(move || {
        pulses_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    });

    let out = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            liveness,
        )
        .await
        .unwrap();
    assert_eq!(out.output, json!("ok"));
    assert_eq!(pulses.load(std::sync::atomic::Ordering::SeqCst), 4);

    shutdown.cancel();
}

#[tokio::test]
async fn wait_decodes_json_stdout_to_structured_value() {
    let stub = Stub::new();
    stub.script_job("job-1", vec![done_envelope(0, r#"{"a":1}"#, "")]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let out = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap();
    assert_eq!(out.output, json!({"a": 1}));

    shutdown.cancel();
}

#[tokio::test]
async fn wait_falls_back_to_string_stdout() {
    let stub = Stub::new();
    stub.script_job("job-1", vec![done_envelope(0, "plain text", "")]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let out = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap();
    assert_eq!(out.output, json!("plain text"));

    shutdown.cancel();
}

#[tokio::test]
async fn wait_truncates_oversized_stdout_with_marker() {
    let stub = Stub::new();
    let big = "x".repeat(2000);
    stub.script_job("job-1", vec![done_envelope(0, &big, "")]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let mut config = client_config(addr);
    config.max_step_output_kb = 1;
    let client = DevkitdClient::new(config);

    let out = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap();
    let Value::String(s) = out.output else {
        panic!("expected string output")
    };
    assert!(s.len() < 2000);
    assert!(s.contains("truncated at 1 KB by flowspec"));

    shutdown.cancel();
}

#[tokio::test]
async fn wait_maps_nonzero_exit_to_tool_error() {
    let stub = Stub::new();
    stub.script_job("job-1", vec![done_envelope(7, "partial out", "boom")]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    match err {
        DevkitdError::ToolError {
            exit_code,
            stdout,
            stderr,
        } => {
            assert_eq!(exit_code, 7);
            assert_eq!(stdout, "partial out");
            assert_eq!(stderr, "boom");
        }
        other => panic!("expected ToolError, got {other:?}"),
    }

    shutdown.cancel();
}

#[tokio::test]
async fn wait_truncates_tool_error_streams() {
    let stub = Stub::new();
    let big = "e".repeat(2000);
    stub.script_job("job-1", vec![done_envelope(1, "", &big)]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let mut config = client_config(addr);
    config.max_step_output_kb = 1;
    let client = DevkitdClient::new(config);

    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    match err {
        DevkitdError::ToolError { stderr, .. } => {
            assert!(stderr.len() < 2000);
            assert!(stderr.contains("truncated at 1 KB by flowspec"));
        }
        other => panic!("expected ToolError, got {other:?}"),
    }

    shutdown.cancel();
}

#[tokio::test]
async fn wait_maps_sentinel_minus2_to_timeout() {
    let stub = Stub::new();
    stub.script_job("job-1", vec![done_envelope(-2, "", "execution timed out")]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DevkitdError::Timeout));

    shutdown.cancel();
}

#[tokio::test]
async fn wait_maps_sentinel_minus1_to_spawn_tool_error() {
    let stub = Stub::new();
    stub.script_job(
        "job-1",
        vec![done_envelope(-1, "", "script not found: x.sh")],
    );
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    match err {
        DevkitdError::ToolError {
            exit_code, stderr, ..
        } => {
            assert_eq!(exit_code, -1);
            assert!(stderr.contains("script not found"));
        }
        other => panic!("expected ToolError, got {other:?}"),
    }

    shutdown.cancel();
}

#[tokio::test]
async fn wait_maps_external_cancel_without_exit_code_to_cancelled() {
    let stub = Stub::new();
    // Wire-exact: {"state":"cancelled"} — no exit_code field at all.
    stub.script_job("job-1", vec![json!({"state": "cancelled"})]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DevkitdError::Cancelled));

    shutdown.cancel();
}

#[tokio::test]
async fn wait_unknown_job_maps_to_interrupted() {
    let stub = Stub::new(); // no job scripted -> isError "unknown job_id"
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-ghost".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DevkitdError::Interrupted));

    shutdown.cancel();
}

#[tokio::test]
async fn wait_deadline_exceeded_issues_cancel_then_times_out() {
    let stub = Stub::new();
    // Stays running forever; the deadline backstop must fire.
    stub.script_job("job-1", vec![json!({"state": "running"})]);
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    let deadline = SystemTime::now() + Duration::from_millis(60);
    let err = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            Some(deadline),
            noop_liveness(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DevkitdError::Timeout));
    assert!(
        stub.call_count("job-cancel") >= 1,
        "deadline backstop must call job-cancel"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// 14-15: transport blips
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wait_survives_transport_blip_within_backoff() {
    let stub = Stub::new();
    stub.script_job(
        "job-1",
        vec![json!({"state": "running"}), done_envelope(0, "ok", "")],
    );
    let addr = reserve_addr().await;
    let (server1, shutdown1) = spawn_stub(addr, stub.clone(), None);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut config = client_config(addr);
    config.poll_interval = Duration::from_millis(20);
    config.poll_retry_delays = vec![Duration::from_millis(80); 10];
    let client = Arc::new(DevkitdClient::new(config));

    let wait_client = client.clone();
    let wait_task = tokio::spawn(async move {
        wait_client
            .wait(
                &flowspec_app::ports::JobHandle("job-1".to_string()),
                None,
                noop_liveness(),
            )
            .await
    });

    // Let the first poll or two succeed, then take the server down briefly.
    tokio::time::sleep(Duration::from_millis(60)).await;
    shutdown1.cancel();
    let _ = server1.await;

    // Blip: 150ms with nothing listening (well within the 800ms retry budget).
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (server2, shutdown2) = spawn_stub(addr, stub.clone(), None);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let out = tokio::time::timeout(Duration::from_secs(5), wait_task)
        .await
        .expect("wait() should finish within 5s")
        .unwrap()
        .unwrap();
    assert_eq!(out.output, json!("ok"));

    shutdown2.cancel();
    let _ = server2.await;
}

#[tokio::test]
async fn wait_unreachable_after_backoff_budget() {
    let stub = Stub::new();
    stub.script_job("job-1", vec![json!({"state": "running"})]);
    let addr = reserve_addr().await;
    let (server1, shutdown1) = spawn_stub(addr, stub.clone(), None);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut config = client_config(addr);
    config.poll_interval = Duration::from_millis(10);
    config.poll_retry_delays = vec![Duration::from_millis(20); 3]; // ~60ms budget
    let client = DevkitdClient::new(config);

    shutdown1.cancel();
    let _ = server1.await;

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        ),
    )
    .await
    .expect("wait() should give up within 5s")
    .unwrap_err();
    assert!(matches!(err, DevkitdError::Unreachable));
}

// ---------------------------------------------------------------------------
// 16-18: cancel() and auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_unknown_job_is_ok() {
    let stub = Stub::new(); // no job registered
    let (addr, _server, shutdown) = start_stub(stub.clone()).await;
    let client = DevkitdClient::new(client_config(addr));

    client
        .cancel(&flowspec_app::ports::JobHandle("job-ghost".to_string()))
        .await
        .expect("cancel of an already-gone job is idempotent success");

    shutdown.cancel();
}

#[tokio::test]
async fn bearer_token_reaches_server() {
    let stub = Stub::new();
    stub.script_job("job-1", vec![done_envelope(0, "ok", "")]);
    let addr = reserve_addr().await;
    let (server, shutdown) = spawn_stub(addr, stub.clone(), Some("secret-token".to_string()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Without a token: every call is rejected at the HTTP layer -> Unreachable.
    let client_no_token = DevkitdClient::new(client_config(addr));
    let err = client_no_token
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, DevkitdError::Unreachable));

    // With the right token: succeeds.
    let mut config = client_config(addr);
    config.auth_token = Some("secret-token".to_string());
    let client = DevkitdClient::new(config);
    let out = client
        .wait(
            &flowspec_app::ports::JobHandle("job-1".to_string()),
            None,
            noop_liveness(),
        )
        .await
        .unwrap();
    assert_eq!(out.output, json!("ok"));

    shutdown.cancel();
    let _ = server.await;
}
