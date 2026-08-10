use flowspec_app::ports::{RunId, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::{FakeDevkitd, InMemoryFlowSource, InMemoryStateStore, Script};
use flowspec_app::use_cases::queries::{
    self, GetStepOutputRequest, ListRunsRequest, PendingApprovalsRequest, QueryError,
};
use flowspec_app::use_cases::start_flow::{self, StartFlowRequest};
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

fn config() -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1,
        deadline_margin_secs: 1,
        default_step_timeout_secs: 3600,
        max_step_output_kb: 256,
        executor_cli_tool: "agent-run".to_string(),
    }
}

fn load_fixture(rel: &str) -> FlowDefinition {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel);
    let text = std::fs::read_to_string(path).unwrap();
    let file: FlowFile = serde_yaml_ng::from_str(&text).unwrap();
    file.into_definitions().into_iter().next().unwrap()
}

struct Harness {
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    flows: Arc<InMemoryFlowSource>,
}

impl Harness {
    fn new(flows: Vec<FlowDefinition>, scripts: HashMap<String, Script>) -> Self {
        let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
        let devkitd: Arc<FakeDevkitd> =
            Arc::new(FakeDevkitd::new(scripts).with_poll_interval(Duration::from_millis(10)));
        let flows: Arc<InMemoryFlowSource> = Arc::new(InMemoryFlowSource::new(flows));
        let scheduler = Arc::new(Scheduler::new(store.clone(), devkitd.clone(), config()));
        Self {
            store,
            scheduler,
            flows,
        }
    }

    async fn start(&self, req: StartFlowRequest) -> start_flow::StartFlowResponse {
        start_flow::start_flow(
            self.store.clone(),
            self.scheduler.clone(),
            self.flows.clone(),
            &config(),
            req,
        )
        .await
        .unwrap()
    }
}

fn start_req(flow_name: &str, idempotency_key: Option<&str>) -> StartFlowRequest {
    StartFlowRequest {
        flow_name: flow_name.into(),
        version_req: None,
        inputs: serde_json::json!({ "message": "add feature" }),
        trigger: Value::Null,
        idempotency_key: idempotency_key.map(|s| s.to_string()),
    }
}

#[tokio::test]
async fn start_flow_with_same_idempotency_key_replays_the_run() {
    let flows = vec![load_fixture("linear.yaml")];
    let scripts: HashMap<String, Script> = [
        (
            "plan".into(),
            Script::Succeed(Value::String("PLAN.md".into())),
        ),
        ("implement".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);

    let first = harness.start(start_req("linear", Some("key-1"))).await;
    assert!(!first.replayed);

    let second = harness.start(start_req("linear", Some("key-1"))).await;
    assert!(second.replayed);
    assert_eq!(second.run_id, first.run_id);

    // Only one RunStarted should have been submitted: the run has exactly
    // one row keyed under this idempotency key in the store.
    let runs = harness.store.list_runs(Default::default()).await.unwrap();
    assert_eq!(runs.len(), 1);
}

#[tokio::test]
async fn start_flow_with_different_keys_creates_distinct_runs() {
    let flows = vec![load_fixture("linear.yaml")];
    let scripts: HashMap<String, Script> = [
        (
            "plan".into(),
            Script::Succeed(Value::String("PLAN.md".into())),
        ),
        ("implement".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);

    let first = harness.start(start_req("linear", Some("key-1"))).await;
    let second = harness.start(start_req("linear", Some("key-2"))).await;
    assert_ne!(first.run_id, second.run_id);
    assert!(!second.replayed);
}

#[tokio::test]
async fn get_step_output_selects_latest_attempt_by_default_and_a_specific_one_when_asked() {
    let flows = vec![load_fixture("human-loop.yaml")];
    let scripts: HashMap<String, Script> = [
        (
            "plan".into(),
            Script::Succeed(Value::String("PLAN.md".into())),
        ),
        ("implement".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id: RunId = harness.start(start_req("human-loop", None)).await.run_id;

    sleep(Duration::from_millis(50)).await;

    // Reject once so "plan" gets a second attempt.
    flowspec_app::use_cases::approvals::reject_step(
        harness.store.clone(),
        harness.scheduler.clone(),
        flowspec_app::use_cases::approvals::RejectRequest {
            run_id: run_id.clone(),
            step_id: Some("plan".into()),
            feedback: "try again".into(),
        },
    )
    .await
    .unwrap();

    sleep(Duration::from_millis(100)).await;

    let latest = queries::get_step_output(
        harness.store.clone(),
        GetStepOutputRequest {
            run_id: run_id.clone(),
            step_id: "plan".into(),
            attempt: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(latest.attempt, 2);

    let first_attempt = queries::get_step_output(
        harness.store.clone(),
        GetStepOutputRequest {
            run_id: run_id.clone(),
            step_id: "plan".into(),
            attempt: Some(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(first_attempt.attempt, 1);
    assert_eq!(first_attempt.feedback.as_deref(), Some("try again"));

    let missing = queries::get_step_output(
        harness.store.clone(),
        GetStepOutputRequest {
            run_id: run_id.clone(),
            step_id: "plan".into(),
            attempt: Some(99),
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(missing, QueryError::AttemptNotFound(99, _)));

    let unknown_step = queries::get_step_output(
        harness.store.clone(),
        GetStepOutputRequest {
            run_id,
            step_id: "no-such-step".into(),
            attempt: None,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(unknown_step, QueryError::StepNotFound(_)));
}

#[tokio::test]
async fn pending_approvals_run_id_filter_scopes_to_one_run() {
    let flows = vec![load_fixture("human-loop.yaml")];
    let scripts: HashMap<String, Script> = [
        (
            "plan".into(),
            Script::Succeed(Value::String("PLAN.md".into())),
        ),
        ("implement".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_a = harness.start(start_req("human-loop", None)).await.run_id;
    let run_b = harness.start(start_req("human-loop", None)).await.run_id;
    sleep(Duration::from_millis(50)).await;

    let all = queries::pending_approvals(
        harness.store.clone(),
        PendingApprovalsRequest { run_id: None },
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 2);

    let scoped = queries::pending_approvals(
        harness.store.clone(),
        PendingApprovalsRequest {
            run_id: Some(run_a.clone()),
        },
    )
    .await
    .unwrap();
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].run_id, run_a);
    assert_ne!(scoped[0].run_id, run_b);
}

#[tokio::test]
async fn list_runs_rejects_an_invalid_phase_with_the_legal_values_in_the_message() {
    let harness = Harness::new(Vec::new(), HashMap::new());
    let err = queries::list_runs(
        harness.store.clone(),
        ListRunsRequest {
            flow_name: None,
            phase: Some("not-a-phase".into()),
            limit: None,
        },
    )
    .await
    .unwrap_err();
    let message = format!("{err}");
    let QueryError::InvalidPhase(msg) = err else {
        panic!("expected InvalidPhase, got {message}");
    };
    assert_eq!(msg, "not-a-phase");
    assert!(message.contains("running"));
}
