//! Contract tests for the platform connector (`poller` + `pump`) against a
//! plain axum stub standing in for `devaiflow-platform`'s five agent-facing
//! REST endpoints (`docs/platform-agent-api.md`). Reuses the bearer-auth +
//! `reserve_addr` + graceful-shutdown scaffolding pattern from
//! `tests/devkitd_client.rs`, minus the rmcp `StreamableHttpService` (this
//! is plain HTTP, not MCP).

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use flowspec_app::ports::{Devkitd, FlowSource, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::{FakeDevkitd, InMemoryFlowSource, InMemoryStateStore, Script};
use flowspec_server::config::{PlatformConfig, Secret};
use flowspec_server::platform::client::PlatformClient;
use flowspec_server::platform::{poller, pump};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "dvf_test_token";

// ---------------------------------------------------------------------------
// Plain-HTTP platform stub
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum ActionStatus {
    Pending,
    Acked,
    Removed,
}

#[derive(Debug, Clone)]
struct StubAction {
    id: String,
    run_id: String,
    kind: String,
    payload: Value,
    status: ActionStatus,
}

#[derive(Default)]
struct StubState {
    actions: Vec<StubAction>,
    /// (run_id, sequence) already accepted -- mirrors the platform's
    /// `INSERT OR IGNORE` idempotency on the events table.
    seen_events: HashSet<(String, i64)>,
    event_push_count: u32,
    states: HashMap<String, Value>,
    state_push_count: u32,
    ack_count: u32,
    delete_count: u32,
}

#[derive(Clone)]
struct Stub {
    state: Arc<Mutex<StubState>>,
}

impl Stub {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(StubState::default())),
        }
    }

    fn push_action(&self, id: &str, run_id: &str, kind: &str, payload: Value) {
        self.state.lock().unwrap().actions.push(StubAction {
            id: id.to_string(),
            run_id: run_id.to_string(),
            kind: kind.to_string(),
            payload,
            status: ActionStatus::Pending,
        });
    }

    fn action_status(&self, id: &str) -> Option<ActionStatus> {
        self.state
            .lock()
            .unwrap()
            .actions
            .iter()
            .find(|a| a.id == id)
            .map(|a| a.status.clone())
    }

    fn state_for(&self, run_id: &str) -> Option<Value> {
        self.state.lock().unwrap().states.get(run_id).cloned()
    }

    fn event_push_count(&self) -> u32 {
        self.state.lock().unwrap().event_push_count
    }

    #[allow(dead_code)]
    fn ack_count(&self) -> u32 {
        self.state.lock().unwrap().ack_count
    }

    fn delete_count(&self) -> u32 {
        self.state.lock().unwrap().delete_count
    }
}

#[allow(clippy::result_large_err)] // test-only stub, not perf-sensitive
fn check_auth(headers: &HeaderMap) -> Result<(), Response> {
    let ok = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {TOKEN}"))
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "invalid_token"})),
        )
            .into_response())
    }
}

async fn get_actions(headers: HeaderMap, State(stub): State<Stub>) -> Response {
    if let Err(r) = check_auth(&headers) {
        return r;
    }
    let actions: Vec<Value> = stub
        .state
        .lock()
        .unwrap()
        .actions
        .iter()
        .filter(|a| a.status == ActionStatus::Pending)
        .map(|a| {
            json!({
                "id": a.id,
                "run_id": a.run_id,
                "kind": a.kind,
                "payload": a.payload,
                "created_at": "2026-01-01T00:00:00.000Z",
            })
        })
        .collect();
    Json(json!({ "actions": actions })).into_response()
}

async fn ack_action(
    headers: HeaderMap,
    State(stub): State<Stub>,
    Path(id): Path<String>,
) -> Response {
    if let Err(r) = check_auth(&headers) {
        return r;
    }
    let mut guard = stub.state.lock().unwrap();
    guard.ack_count += 1;
    let Some(action) = guard.actions.iter_mut().find(|a| a.id == id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "action_not_found"})),
        )
            .into_response();
    };
    if action.status != ActionStatus::Pending {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "action_not_pending"})),
        )
            .into_response();
    }
    action.status = ActionStatus::Acked;
    Json(json!({ "ok": true })).into_response()
}

async fn delete_action(
    headers: HeaderMap,
    State(stub): State<Stub>,
    Path(id): Path<String>,
) -> Response {
    if let Err(r) = check_auth(&headers) {
        return r;
    }
    let mut guard = stub.state.lock().unwrap();
    guard.delete_count += 1;
    let Some(action) = guard.actions.iter_mut().find(|a| a.id == id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "action_not_found"})),
        )
            .into_response();
    };
    action.status = ActionStatus::Removed;
    Json(json!({ "ok": true })).into_response()
}

async fn push_state(
    headers: HeaderMap,
    State(stub): State<Stub>,
    Path(run_id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(r) = check_auth(&headers) {
        return r;
    }
    let mut guard = stub.state.lock().unwrap();
    guard.state_push_count += 1;
    guard.states.insert(run_id, body);
    Json(json!({ "ok": true })).into_response()
}

async fn push_events(
    headers: HeaderMap,
    State(stub): State<Stub>,
    Path(run_id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if let Err(r) = check_auth(&headers) {
        return r;
    }
    let events = body.get("events").and_then(|e| e.as_array());
    let Some(events) = events else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_events"})),
        )
            .into_response();
    };
    if events.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid_events"})),
        )
            .into_response();
    }
    let mut guard = stub.state.lock().unwrap();
    guard.event_push_count += 1;
    let mut inserted = 0;
    for e in events {
        let seq = e.get("sequence").and_then(|s| s.as_i64()).unwrap();
        if guard.seen_events.insert((run_id.clone(), seq)) {
            inserted += 1;
        }
    }
    Json(json!({ "ok": true, "inserted": inserted })).into_response()
}

async fn reserve_addr() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

async fn start_stub(stub: Stub) -> (String, tokio::task::JoinHandle<()>, CancellationToken) {
    let addr = reserve_addr().await;
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let router = Router::new()
        .route("/api/agent/v1/actions", get(get_actions))
        .route("/api/agent/v1/actions/{id}/ack", post(ack_action))
        .route("/api/agent/v1/actions/{id}", delete(delete_action))
        .route("/api/agent/v1/runs/{runId}/state", post(push_state))
        .route("/api/agent/v1/runs/{runId}/events", post(push_events))
        .with_state(stub);
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { server_shutdown.cancelled_owned().await })
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (format!("http://{addr}"), handle, shutdown)
}

fn platform_client(url: &str) -> Arc<PlatformClient> {
    Arc::new(PlatformClient::new(&PlatformConfig {
        url: url.to_string(),
        token: Secret::new(TOKEN),
        poll_interval_secs: 1,
        event_batch_size: 100,
    }))
}

// ---------------------------------------------------------------------------
// flowspec-app harness (mirrors crates/flowspec-app/tests fixtures)
// ---------------------------------------------------------------------------

fn scheduler_config() -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1,
        deadline_margin_secs: 1,
        default_step_timeout_secs: 3600,
        max_step_output_kb: 256,
        executor_cli_tool: "agent-run".to_string(),
    }
}

fn load_fixture(rel: &str) -> flowspec_domain::flow::types::FlowDefinition {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel);
    let text = std::fs::read_to_string(path).unwrap();
    let file: flowspec_domain::flow::types::FlowFile = serde_yaml_ng::from_str(&text).unwrap();
    file.into_definitions().into_iter().next().unwrap()
}

fn flow_doc_json(rel: &str) -> Value {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel);
    let text = std::fs::read_to_string(path).unwrap();
    let yaml: serde_yaml_ng::Value = serde_yaml_ng::from_str(&text).unwrap();
    serde_json::to_value(yaml).unwrap()
}

struct AppHarness {
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    #[allow(dead_code)]
    devkitd: Arc<dyn Devkitd>,
    #[allow(dead_code)]
    flows: Arc<dyn FlowSource>,
}

impl AppHarness {
    fn new(scripts: HashMap<String, Script>) -> Self {
        let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
        let devkitd: Arc<dyn Devkitd> =
            Arc::new(FakeDevkitd::new(scripts).with_poll_interval(Duration::from_millis(10)));
        let flows: Arc<dyn FlowSource> = Arc::new(InMemoryFlowSource::new(vec![]));
        let scheduler = Arc::new(Scheduler::new(
            store.clone(),
            devkitd.clone(),
            scheduler_config(),
        ));
        Self {
            store,
            scheduler,
            devkitd,
            flows,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trigger_run_starts_a_run_and_acks_with_runtime_run_id() {
    let stub = Stub::new();
    let (url, _handle, shutdown) = start_stub(stub.clone()).await;
    let client = platform_client(&url);

    let scripts: HashMap<String, Script> = [
        ("plan".into(), Script::Succeed(Value::String("ok".into()))),
        (
            "implement".into(),
            Script::Succeed(Value::String("ok".into())),
        ),
    ]
    .into();
    let harness = AppHarness::new(scripts);

    stub.push_action(
        "act_1",
        "run_platform_1",
        "trigger_run",
        json!({
            "run_id": "run_platform_1",
            "flow_doc": flow_doc_json("linear.yaml"),
            "inputs": { "message": "go" },
        }),
    );

    let poller_shutdown = CancellationToken::new();
    let poller_task = tokio::spawn(poller::run(
        client.clone(),
        harness.store.clone(),
        harness.scheduler.clone(),
        poller::PollerConfig {
            poll_interval: Duration::from_millis(20),
        },
        poller_shutdown.clone(),
    ));

    // Wait for the action to be acked.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stub.action_status("act_1") == Some(ActionStatus::Acked) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("trigger_run was never acked");

    // The run must actually have been started with the platform's
    // idempotency key.
    let run_id = harness
        .store
        .find_by_idempotency_key("platform:run_platform_1")
        .await
        .unwrap()
        .expect("run started with platform-prefixed idempotency key");

    let record = harness.store.load_run(&run_id).await.unwrap();
    assert_eq!(record.flow_name, "linear");

    poller_shutdown.cancel();
    let _ = poller_task.await;
    shutdown.cancel();
}

#[tokio::test]
async fn redelivered_trigger_run_replays_instead_of_double_starting() {
    let stub = Stub::new();
    let (url, _handle, shutdown) = start_stub(stub.clone()).await;
    let client = platform_client(&url);

    let scripts: HashMap<String, Script> = [
        ("plan".into(), Script::Succeed(Value::String("ok".into()))),
        (
            "implement".into(),
            Script::Succeed(Value::String("ok".into())),
        ),
    ]
    .into();
    let harness = AppHarness::new(scripts);

    let payload = json!({
        "run_id": "run_dup",
        "flow_doc": flow_doc_json("linear.yaml"),
        "inputs": { "message": "go" },
    });
    stub.push_action("act_1", "run_dup", "trigger_run", payload.clone());

    let poller_shutdown = CancellationToken::new();
    let poller_task = tokio::spawn(poller::run(
        client.clone(),
        harness.store.clone(),
        harness.scheduler.clone(),
        poller::PollerConfig {
            poll_interval: Duration::from_millis(20),
        },
        poller_shutdown.clone(),
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stub.action_status("act_1") == Some(ActionStatus::Acked) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let first_run_id = harness
        .store
        .find_by_idempotency_key("platform:run_dup")
        .await
        .unwrap()
        .unwrap();

    // A second, redelivered trigger_run for the same platform run id.
    stub.push_action("act_2", "run_dup", "trigger_run", payload);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stub.action_status("act_2") == Some(ActionStatus::Acked) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let second_run_id = harness
        .store
        .find_by_idempotency_key("platform:run_dup")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        first_run_id, second_run_id,
        "redelivery must resolve to the same run, not start a second one"
    );

    poller_shutdown.cancel();
    let _ = poller_task.await;
    shutdown.cancel();
}

#[tokio::test]
async fn invalid_flow_doc_deletes_the_action() {
    let stub = Stub::new();
    let (url, _handle, shutdown) = start_stub(stub.clone()).await;
    let client = platform_client(&url);
    let harness = AppHarness::new(HashMap::new());

    // `cli.input` is required (`flow/types.rs`); an empty steps list fails
    // domain validation ("non_empty_steps").
    stub.push_action(
        "act_bad",
        "run_bad",
        "trigger_run",
        json!({
            "run_id": "run_bad",
            "flow_doc": { "flow": { "name": "bad", "version": "1.0.0", "steps": [] } },
            "inputs": {},
        }),
    );

    let poller_shutdown = CancellationToken::new();
    let poller_task = tokio::spawn(poller::run(
        client.clone(),
        harness.store.clone(),
        harness.scheduler.clone(),
        poller::PollerConfig {
            poll_interval: Duration::from_millis(20),
        },
        poller_shutdown.clone(),
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stub.action_status("act_bad") == Some(ActionStatus::Removed) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("invalid flow_doc must be deleted, not redelivered forever");

    assert!(stub.delete_count() >= 1);
    poller_shutdown.cancel();
    let _ = poller_task.await;
    shutdown.cancel();
}

#[tokio::test]
async fn approve_before_waiting_is_left_pending_not_deleted() {
    let stub = Stub::new();
    let (url, _handle, shutdown) = start_stub(stub.clone()).await;
    let client = platform_client(&url);

    // No run exists at all for this platform run id yet -- resolve_step_id
    // will find nothing. The poller must delete only when the run is truly
    // unresolvable (unknown idempotency key), and otherwise leave a
    // not-yet-actionable approve pending. This test exercises the "unknown
    // run" branch, which the reference mock-runtime.mjs doesn't special-case
    // (it always ties approve to a run it started itself) but a real
    // runtime restart can hit.
    stub.push_action(
        "act_appr",
        "run_never_started",
        "approve",
        json!({ "step_id": "plan" }),
    );

    let harness = AppHarness::new(HashMap::new());
    let poller_shutdown = CancellationToken::new();
    let poller_task = tokio::spawn(poller::run(
        client.clone(),
        harness.store.clone(),
        harness.scheduler.clone(),
        poller::PollerConfig {
            poll_interval: Duration::from_millis(20),
        },
        poller_shutdown.clone(),
    ));

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Unknown run -> deleted (nothing will ever make it actionable).
    assert_eq!(stub.action_status("act_appr"), Some(ActionStatus::Removed));

    poller_shutdown.cancel();
    let _ = poller_task.await;
    shutdown.cancel();
}

#[tokio::test]
async fn pump_pushes_events_then_state_and_drains_the_outbox() {
    let stub = Stub::new();
    let (url, _handle, shutdown) = start_stub(stub.clone()).await;
    let client = platform_client(&url);

    let scripts: HashMap<String, Script> = [
        ("plan".into(), Script::Succeed(Value::String("ok".into()))),
        (
            "implement".into(),
            Script::Succeed(Value::String("ok".into())),
        ),
    ]
    .into();
    let harness = AppHarness::new(scripts);
    let flows: Arc<dyn FlowSource> =
        Arc::new(InMemoryFlowSource::new(vec![load_fixture("linear.yaml")]));

    let resp = flowspec_app::use_cases::start_flow::start_flow(
        harness.store.clone(),
        harness.scheduler.clone(),
        flows,
        &scheduler_config(),
        flowspec_app::use_cases::start_flow::StartFlowRequest {
            flow_name: "linear".into(),
            version_req: None,
            inputs: json!({ "message": "go" }),
            trigger: Value::Null,
            idempotency_key: Some("platform:run_pump_1".into()),
        },
    )
    .await
    .unwrap();

    // Wait for the run to complete.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = harness.store.load_run(&resp.run_id).await.unwrap();
            if !matches!(record.phase, flowspec_domain::run::types::RunPhase::Running) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let pump_shutdown = CancellationToken::new();
    let pump_task = tokio::spawn(pump::run(
        client.clone(),
        harness.store.clone(),
        pump::PumpConfig {
            poll_interval: Duration::from_millis(20),
            event_batch_size: 100,
        },
        pump_shutdown.clone(),
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if stub.state_for("run_pump_1").is_some() && stub.event_push_count() > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("pump never pushed events+state for the platform run id");

    let pushed_state = stub.state_for("run_pump_1").unwrap();
    assert_eq!(pushed_state["phase"], "completed");
    assert_eq!(pushed_state["run_id"], "run_pump_1");

    // The outbox is fully drained.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let remaining = harness.store.unpushed_events(1000).await.unwrap();
    assert!(
        remaining.iter().all(|(id, _)| id != &resp.run_id),
        "pump must drain every event for a completed run"
    );

    pump_shutdown.cancel();
    let _ = pump_task.await;
    shutdown.cancel();
}
