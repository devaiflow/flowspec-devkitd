//! Phase 5 tool-surface tests: driven through a real rmcp client against an
//! in-process server wired to `flowspec-app`'s in-memory fakes (no live
//! devkitd, no SQLite file).

mod support;

use flowspec_app::testkit::Script;
use rmcp::model::{CallToolRequestParams, ContentBlock};
use rmcp::{ServiceExt, transport::StreamableHttpClientTransport};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

type Client = rmcp::service::RunningService<rmcp::RoleClient, ()>;

struct TestServer {
    client: Client,
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(
        flows: Vec<flowspec_domain::flow::types::FlowDefinition>,
        scripts: HashMap<String, Script>,
    ) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener);

        let container = support::fake_container(flows, scripts);
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let listen_addr = addr.to_string();
        let handle = tokio::spawn(async move {
            flowspec_server::mcp_server::serve(&listen_addr, container, server_shutdown)
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
        let client = ().serve(transport).await?;

        Ok(TestServer {
            client,
            shutdown,
            handle,
        })
    }

    async fn call(&self, name: &str, args: Value) -> rmcp::model::CallToolResult {
        let arguments = match args {
            Value::Object(map) => Some(map),
            Value::Null => None,
            other => panic!("tool arguments must be a JSON object, got {other}"),
        };
        let mut params = CallToolRequestParams::new(name.to_string());
        if let Some(arguments) = arguments {
            params = params.with_arguments(arguments);
        }
        self.client.call_tool(params).await.unwrap()
    }

    async fn shutdown(self) {
        let _ = self.client.cancel().await;
        self.shutdown.cancel();
        self.handle.await.unwrap();
    }
}

fn text_of(result: &rmcp::model::CallToolResult) -> String {
    result
        .content
        .iter()
        .find_map(|c| match c {
            ContentBlock::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .expect("tool result has text content")
}

fn structured_of(result: &rmcp::model::CallToolResult) -> Value {
    result
        .structured_content
        .clone()
        .expect("tool result has structured content")
}

fn human_loop_flow() -> Vec<flowspec_domain::flow::types::FlowDefinition> {
    vec![support::load_fixture("human-loop.yaml")]
}

fn human_loop_scripts() -> HashMap<String, Script> {
    [
        (
            "plan".into(),
            Script::Succeed(Value::String("PLAN.md".into())),
        ),
        ("implement".into(), Script::Succeed(Value::Null)),
    ]
    .into()
}

// ---------------------------------------------------------------------------
// 1. Schema snapshot -- the tool contract is a reviewed artifact.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tool_schema_snapshot() -> anyhow::Result<()> {
    let server = TestServer::start(Vec::new(), HashMap::new()).await?;

    let mut tools = server.client.list_all_tools().await?;
    tools.sort_by(|a, b| a.name.cmp(&b.name));

    let summary: Vec<Value> = tools
        .into_iter()
        .map(|t| {
            json!({
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
                "output_schema": t.output_schema,
            })
        })
        .collect();

    insta::assert_yaml_snapshot!("tool_schema", summary);

    server.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Per-tool happy path -- dual emit (content text + structured_content).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_flows_returns_flow_with_declared_inputs() -> anyhow::Result<()> {
    let server = TestServer::start(human_loop_flow(), HashMap::new()).await?;

    let result = server.call("list_flows", Value::Null).await;
    assert_eq!(result.is_error, Some(false));
    let structured = structured_of(&result);
    let flows = structured["flows"].as_array().unwrap();
    assert_eq!(flows.len(), 1);
    assert_eq!(flows[0]["name"], "human-loop");
    assert_eq!(flows[0]["inputs"][0]["name"], "message");
    assert_eq!(flows[0]["inputs"][0]["required"], true);
    assert!(text_of(&result).contains("human-loop"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn start_flow_then_get_run_status_and_pending_approvals_and_get_step_output()
-> anyhow::Result<()> {
    let server = TestServer::start(human_loop_flow(), human_loop_scripts()).await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "human-loop",
                "inputs": { "message": "add feature" },
                "trigger": null,
                "idempotency_key": "lifecycle-1",
            }),
        )
        .await;
    assert_eq!(start.is_error, Some(false));
    let start_body = structured_of(&start);
    let run_id = start_body["run_id"].as_str().unwrap().to_string();
    assert_eq!(start_body["replayed"], false);

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let status = server
        .call("get_run_status", json!({ "run_id": run_id }))
        .await;
    assert_eq!(status.is_error, Some(false));
    let status_body = structured_of(&status);
    assert_eq!(status_body["phase"], "running");
    assert_eq!(status_body["active_steps"][0]["step_id"], "plan");

    let pending = server
        .call("pending_approvals", json!({ "run_id": run_id }))
        .await;
    let pending_body = structured_of(&pending);
    let waiting = pending_body["pending"].as_array().unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0]["step_id"], "plan");

    let approve = server
        .call(
            "approve_step",
            json!({ "run_id": run_id, "step_id": "plan", "comment": "lgtm" }),
        )
        .await;
    assert_eq!(approve.is_error, Some(false));

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let output = server
        .call(
            "get_step_output",
            json!({ "run_id": run_id, "step_id": "plan" }),
        )
        .await;
    assert_eq!(output.is_error, Some(false));
    let output_body = structured_of(&output);
    assert_eq!(output_body["output"], "PLAN.md");

    let terminal = server
        .call("get_run_status", json!({ "run_id": run_id }))
        .await;
    let terminal_body = structured_of(&terminal);
    assert_eq!(terminal_body["phase"], "completed");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn reject_step_records_feedback_and_reruns() -> anyhow::Result<()> {
    let server = TestServer::start(human_loop_flow(), human_loop_scripts()).await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "human-loop",
                "inputs": { "message": "add feature" },
                "trigger": null,
                "idempotency_key": null,
            }),
        )
        .await;
    let run_id = structured_of(&start)["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let reject = server
        .call(
            "reject_step",
            json!({ "run_id": run_id, "step_id": "plan", "feedback": "missing edge case" }),
        )
        .await;
    assert_eq!(reject.is_error, Some(false));

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let first_attempt = server
        .call(
            "get_step_output",
            json!({ "run_id": run_id, "step_id": "plan", "attempt": 1 }),
        )
        .await;
    let body = structured_of(&first_attempt);
    assert_eq!(body["feedback"], "missing edge case");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cancel_run_moves_run_to_cancelled() -> anyhow::Result<()> {
    let server = TestServer::start(
        vec![support::load_fixture("linear.yaml")],
        [
            (
                "plan".into(),
                Script::Succeed(Value::String("PLAN.md".into())),
            ),
            ("implement".into(), Script::Succeed(Value::Null)),
        ]
        .into(),
    )
    .await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "linear",
                "inputs": { "message": "add feature" },
                "trigger": null,
                "idempotency_key": null,
            }),
        )
        .await;
    let run_id = structured_of(&start)["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let cancel = server.call("cancel_run", json!({ "run_id": run_id })).await;
    assert_eq!(cancel.is_error, Some(false));
    let body = structured_of(&cancel);
    assert_eq!(body["phase"], "cancelled");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn list_runs_filters_by_phase() -> anyhow::Result<()> {
    let server = TestServer::start(
        vec![support::load_fixture("linear.yaml")],
        [
            (
                "plan".into(),
                Script::Succeed(Value::String("PLAN.md".into())),
            ),
            ("implement".into(), Script::Succeed(Value::Null)),
        ]
        .into(),
    )
    .await?;

    server
        .call(
            "start_flow",
            json!({
                "flow_name": "linear",
                "inputs": { "message": "add feature" },
                "trigger": null,
                "idempotency_key": null,
            }),
        )
        .await;

    let all = server
        .call(
            "list_runs",
            json!({ "flow_name": null, "phase": null, "limit": null }),
        )
        .await;
    assert_eq!(structured_of(&all)["runs"].as_array().unwrap().len(), 1);

    let none = server
        .call(
            "list_runs",
            json!({ "flow_name": null, "phase": "failed", "limit": null }),
        )
        .await;
    assert_eq!(structured_of(&none)["runs"].as_array().unwrap().len(), 0);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn get_run_tree_returns_the_root_with_no_children_for_a_leaf_run() -> anyhow::Result<()> {
    let server = TestServer::start(
        vec![support::load_fixture("linear.yaml")],
        [
            (
                "plan".into(),
                Script::Succeed(Value::String("PLAN.md".into())),
            ),
            ("implement".into(), Script::Succeed(Value::Null)),
        ]
        .into(),
    )
    .await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "linear",
                "inputs": { "message": "add feature" },
                "trigger": null,
                "idempotency_key": null,
            }),
        )
        .await;
    let run_id = structured_of(&start)["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let tree = server
        .call("get_run_tree", json!({ "run_id": run_id.clone() }))
        .await;
    assert_eq!(tree.is_error, Some(false));
    let body = structured_of(&tree);
    assert_eq!(body["run_id"], run_id);
    assert_eq!(body["children"].as_array().unwrap().len(), 0);

    server.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Error contract.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn approve_a_non_waiting_step_returns_not_approvable() -> anyhow::Result<()> {
    let server = TestServer::start(
        vec![support::load_fixture("linear.yaml")],
        [
            (
                "plan".into(),
                Script::Succeed(Value::String("PLAN.md".into())),
            ),
            ("implement".into(), Script::Succeed(Value::Null)),
        ]
        .into(),
    )
    .await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "linear",
                "inputs": { "message": "add feature" },
                "trigger": null,
                "idempotency_key": null,
            }),
        )
        .await;
    let run_id = structured_of(&start)["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    let approve = server
        .call(
            "approve_step",
            json!({ "run_id": run_id, "step_id": "plan", "comment": null }),
        )
        .await;
    assert_eq!(approve.is_error, Some(true));
    let text = text_of(&approve);
    assert!(text.contains("not_approvable"));
    assert!(text.contains("pending_approvals"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn unknown_run_id_returns_run_not_found() -> anyhow::Result<()> {
    let server = TestServer::start(Vec::new(), HashMap::new()).await?;

    let status = server
        .call("get_run_status", json!({ "run_id": "does-not-exist" }))
        .await;
    assert_eq!(status.is_error, Some(true));
    assert!(text_of(&status).contains("run_not_found"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn unknown_flow_returns_flow_not_found() -> anyhow::Result<()> {
    let server = TestServer::start(Vec::new(), HashMap::new()).await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "no-such-flow",
                "inputs": {},
                "trigger": null,
                "idempotency_key": null,
            }),
        )
        .await;
    assert_eq!(start.is_error, Some(true));
    assert!(text_of(&start).contains("flow_not_found"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn bad_phase_filter_returns_invalid_phase() -> anyhow::Result<()> {
    let server = TestServer::start(Vec::new(), HashMap::new()).await?;

    let result = server
        .call(
            "list_runs",
            json!({ "flow_name": null, "phase": "not-a-phase", "limit": null }),
        )
        .await;
    assert_eq!(result.is_error, Some(true));
    let text = text_of(&result);
    assert!(text.contains("invalid_phase"));
    assert!(text.contains("running"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn malformed_arguments_surface_as_a_tool_result_error() -> anyhow::Result<()> {
    let server = TestServer::start(Vec::new(), HashMap::new()).await?;

    // rmcp 3.x: `Parameters<T>` deserialization failures are converted from
    // a JSON-RPC -32602 protocol error into a tool-result error (is_error:
    // true, plain text) -- the same "let the host recover conversationally"
    // philosophy our own ToolFailure channel uses (mcp_server/error.rs).
    // Under 0.16 this was a transport-level -32602; that framing no longer
    // holds after the upgrade.
    let result = server
        .client
        .call_tool(
            // Missing every required field.
            CallToolRequestParams::new("start_flow").with_arguments(serde_json::Map::new()),
        )
        .await?;
    assert_eq!(result.is_error, Some(true));
    let text = text_of(&result);
    assert!(text.contains("failed to deserialize parameters"));
    assert!(text.contains("flow_name"));

    server.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Idempotency.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_flow_twice_with_same_key_replays_the_run() -> anyhow::Result<()> {
    let server = TestServer::start(human_loop_flow(), human_loop_scripts()).await?;

    let req = json!({
        "flow_name": "human-loop",
        "inputs": { "message": "add feature" },
        "trigger": null,
        "idempotency_key": "same-key",
    });

    let first = server.call("start_flow", req.clone()).await;
    let first_body = structured_of(&first);
    assert_eq!(first_body["replayed"], false);

    let second = server.call("start_flow", req).await;
    let second_body = structured_of(&second);
    assert_eq!(second_body["replayed"], true);
    assert_eq!(second_body["run_id"], first_body["run_id"]);

    server.shutdown().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Hook failure visibility -- the homelab case this feature fixes: a
// `before_run` hook fails, the run goes `failed` with zero steps, and
// (before this) get_run_status gave no indication anywhere of why.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_run_status_surfaces_a_failed_before_run_hook() -> anyhow::Result<()> {
    let flows = vec![support::load_fixture("hooks-gating-failure.yaml")];
    let scripts: HashMap<String, Script> = [(
        "gate".into(),
        Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
            stdout: String::new(),
            stderr: "PROVISIONER_PASSWORD env var is not set".into(),
            exit_code: 1,
        }),
    )]
    .into();
    let server = TestServer::start(flows, scripts).await?;

    let start = server
        .call(
            "start_flow",
            json!({
                "flow_name": "hooks-gating-failure",
                "inputs": { "message": "go" },
                "trigger": null,
                "idempotency_key": "hook-failure-1",
            }),
        )
        .await;
    assert_eq!(start.is_error, Some(false));
    let run_id = structured_of(&start)["run_id"]
        .as_str()
        .unwrap()
        .to_string();

    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    let status = server
        .call("get_run_status", json!({ "run_id": run_id }))
        .await;
    assert_eq!(status.is_error, Some(false));
    let body = structured_of(&status);
    assert_eq!(body["phase"], "failed");
    assert_eq!(body["steps"].as_array().unwrap().len(), 0);

    let run_hooks = body["run_hooks"].as_array().unwrap();
    assert_eq!(run_hooks.len(), 1);
    assert_eq!(run_hooks[0]["hook"], "gate");
    assert_eq!(run_hooks[0]["phase"], "before_run");
    assert_eq!(run_hooks[0]["status"], "failed");
    assert_eq!(run_hooks[0]["failure"]["kind"], "tool_error");
    assert_eq!(run_hooks[0]["failure"]["exit_code"], 1);
    assert_eq!(
        run_hooks[0]["failure"]["stderr"],
        "PROVISIONER_PASSWORD env var is not set"
    );

    // Same content, verified via the raw text block too (not just structured_content).
    let text = text_of(&status);
    assert!(text.contains("PROVISIONER_PASSWORD"));

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn get_run_status_on_unknown_run_is_a_run_not_found_error() -> anyhow::Result<()> {
    let server = TestServer::start(human_loop_flow(), human_loop_scripts()).await?;

    let status = server
        .call("get_run_status", json!({ "run_id": "does-not-exist" }))
        .await;
    assert_eq!(status.is_error, Some(true));
    let text = text_of(&status);
    assert!(text.contains("does-not-exist"), "text was: {text}");

    server.shutdown().await;
    Ok(())
}
