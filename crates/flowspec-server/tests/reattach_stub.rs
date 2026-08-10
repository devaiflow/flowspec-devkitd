//! CI-safe proof of the Phase 4 headline property — through the *real*
//! `DevkitdClient`, without a live devkitd: `job_id` is persisted before
//! `wait` is ever called, so a flowspec restart (`kill -9` analog: abort +
//! drop, no graceful shutdown of the in-flight task) loses zero work.
//! Scheduler A starts a run whose job stays `running` forever; we drop it
//! mid-flight; Scheduler B, built fresh over the *same* SQLite file and the
//! *same* stub devkitd server (now reconfigured to report `done`), recovers
//! and the run completes.

use flowspec_app::ports::{SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::InMemoryFlowSource;
use flowspec_app::use_cases::{queries, start_flow, start_flow::StartFlowRequest};
use flowspec_domain::flow::types::FlowFile;
use flowspec_server::devkitd::{DevkitdClient, DevkitdClientConfig};
use flowspec_server::state::SqliteStore;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use rmcp::{ErrorData as McpError, ServerHandler};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Minimal scriptable stub (trimmed copy of tests/devkitd_client.rs's Stub —
// integration test binaries can't share code without a `tests/common`
// module, and this test only needs "accept a job" + "swap its script").
// ---------------------------------------------------------------------------

#[derive(Default)]
struct StubState {
    script: Vec<Value>,
    progress: usize,
}

#[derive(Clone, Default)]
struct Stub {
    state: Arc<Mutex<StubState>>,
}

impl Stub {
    fn set_script(&self, script: Vec<Value>) {
        let mut guard = self.state.lock().unwrap();
        guard.script = script;
        guard.progress = 0;
    }
}

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
        let stub = self.0.clone();
        async move {
            let result: CallToolResult = if request.name.as_ref() == "job-status" {
                let mut guard = stub.state.lock().unwrap();
                if guard.script.is_empty() {
                    CallToolResult::error(vec![ContentBlock::text("unknown job_id")])
                } else {
                    let idx = guard.progress.min(guard.script.len() - 1);
                    if guard.progress < guard.script.len() - 1 {
                        guard.progress += 1;
                    }
                    let envelope = guard.script[idx].clone();
                    CallToolResult::success(vec![ContentBlock::text(envelope.to_string())])
                }
            } else if request.name.as_ref() == "job-cancel" {
                // Any other tool (job-cancel, or the plugin start call)
                // succeeds -- this test never exercises cancel or rejection.
                CallToolResult::success(vec![ContentBlock::text(
                    json!({"cancelled": true}).to_string(),
                )])
            } else {
                CallToolResult::success(vec![ContentBlock::text(
                    json!({"job_id": "job-reattach-1"}).to_string(),
                )])
            };
            Ok(result.into())
        }
    }
}

fn spawn_stub(addr: SocketAddr, stub: Stub) -> (tokio::task::JoinHandle<()>, CancellationToken) {
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
        let router = axum::Router::new().nest_service("/mcp", service);
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_shutdown.cancelled_owned().await })
            .await
            .unwrap();
    });
    (handle, shutdown)
}

fn config() -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1,
        deadline_margin_secs: 1,
        default_step_timeout_secs: 3600,
        max_step_output_kb: 256,
        executor_cli_tool: "agent-run".to_string(),
    }
}

fn load_flow() -> flowspec_domain::flow::types::FlowDefinition {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join("linear.yaml");
    let text = std::fs::read_to_string(path).unwrap();
    let file: FlowFile = serde_yaml_ng::from_str(&text).unwrap();
    file.into_definitions().into_iter().next().unwrap()
}

#[tokio::test]
async fn recovery_reattaches_across_a_flowspec_restart_via_the_real_adapter() {
    let stub = Stub::default();
    stub.set_script(vec![json!({"state": "running"})]); // stays running forever

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    };
    let (server, server_shutdown) = spawn_stub(addr, stub.clone());
    sleep(Duration::from_millis(50)).await;

    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("flowspec.db");

    // --- Scheduler A: start the run, let the step's job_id land in SQLite ---
    let run_id = {
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&db_path).unwrap());
        let devkitd = Arc::new(DevkitdClient::new(DevkitdClientConfig::new(format!(
            "http://{addr}/mcp"
        ))));
        let flows = Arc::new(InMemoryFlowSource::new(vec![load_flow()]));
        let scheduler = Arc::new(Scheduler::new(store.clone(), devkitd.clone(), config()));

        let run_id = start_flow::start_flow(
            store.clone(),
            scheduler.clone(),
            flows.clone(),
            &config(),
            StartFlowRequest {
                flow_name: "linear".to_string(),
                version_req: None,
                inputs: json!({"message": "hello"}),
                trigger: Value::Null,
                idempotency_key: None,
            },
        )
        .await
        .unwrap()
        .run_id;

        // Wait until the job_id is actually persisted (proves the property
        // this test exists to demonstrate, not just assume).
        timeout(Duration::from_secs(5), async {
            loop {
                let record = store.load_run(&run_id).await.unwrap();
                if record.latest_steps().iter().any(|s| s.job_id.is_some()) {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("job_id was never persisted");

        // The `kill -9` analog: abort in-flight tasks without draining or
        // cancelling the devkitd job, then drop everything. No graceful
        // shutdown sequencing beyond what `Scheduler::shutdown` already does.
        scheduler.shutdown();
        run_id
    };

    // --- Reconfigure the (still-running) stub to report success ---
    stub.set_script(vec![json!({
        "state": "done",
        "stdout": "PLAN.md",
        "stderr": "",
        "exit_code": 0
    })]);

    // --- Scheduler B: fresh process over the same SQLite file + same stub ---
    let store_b: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&db_path).unwrap());
    let devkitd_b = Arc::new(DevkitdClient::new(DevkitdClientConfig::new(format!(
        "http://{addr}/mcp"
    ))));
    let scheduler_b = Arc::new(Scheduler::new(store_b.clone(), devkitd_b.clone(), config()));

    scheduler_b.recover().await;

    let status = timeout(Duration::from_secs(5), async {
        loop {
            let status = queries::get_run_status(
                store_b.clone(),
                None,
                queries::GetRunStatusRequest {
                    run_id: run_id.clone(),
                },
            )
            .await
            .unwrap();
            if status.phase != "running" {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("run did not reach a terminal phase after recovery");

    assert_eq!(
        status.phase, "completed",
        "re-attached run should complete via the real adapter, not just avoid crashing"
    );

    server_shutdown.cancel();
    let _ = server.await;
}
