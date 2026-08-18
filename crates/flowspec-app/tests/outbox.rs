//! Tests for the platform run-event outbox (`Mutation::AppendEvent` +
//! `StateStore::{unpushed_events,mark_events_pushed}`) and the
//! `flow_run_snapshot` projection that rides alongside it. See
//! `PLAN-LIVERUN-CONNECTED.md` Steps 2-3.

use flowspec_app::ports::{RunEventType, RunId, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::{FakeDevkitd, InMemoryFlowSource, InMemoryStateStore, Script};
use flowspec_app::use_cases::{approvals, queries, start_flow};
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

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

    async fn start(
        &self,
        flow_name: &str,
        inputs: Value,
        idempotency_key: Option<String>,
    ) -> RunId {
        start_flow::start_flow(
            self.store.clone(),
            self.scheduler.clone(),
            self.flows.clone(),
            &config(),
            start_flow::StartFlowRequest {
                flow_name: flow_name.into(),
                version_req: None,
                inputs,
                trigger: Value::Null,
                idempotency_key,
            },
        )
        .await
        .unwrap()
        .run_id
    }

    async fn status(&self, run_id: &RunId) -> queries::RunStatus {
        queries::get_run_status(
            self.store.clone(),
            None,
            queries::GetRunStatusRequest {
                run_id: run_id.clone(),
            },
        )
        .await
        .unwrap()
    }

    async fn wait_terminal(&self, run_id: &RunId, max: Duration) -> queries::RunStatus {
        timeout(max, async {
            loop {
                let status = self.status(run_id).await;
                if status.phase != "running" {
                    return status;
                }
                if !status.active_steps.is_empty()
                    && status.active_steps.iter().all(|s| s.status != "running")
                {
                    return status;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("run did not reach terminal state in time")
    }
}

#[tokio::test]
async fn events_land_in_the_same_batch_and_sequence_is_contiguous() {
    let flows = vec![load_fixture("linear.yaml")];
    let scripts: HashMap<String, Script> = [
        ("plan".into(), Script::Succeed(Value::String("ok".into()))),
        (
            "implement".into(),
            Script::Succeed(Value::String("ok".into())),
        ),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start("linear", serde_json::json!({ "message": "go" }), None)
        .await;

    harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    // Give the terminal transition's own apply() a moment to land -- the
    // scheduler's mutation batch that flips the run terminal is a separate
    // apply() from the one that set the step terminal.
    sleep(Duration::from_millis(50)).await;

    let events = harness.store.unpushed_events(1000).await.unwrap();
    let mine: Vec<_> = events
        .into_iter()
        .filter(|(id, _)| id == &run_id)
        .map(|(_, e)| e)
        .collect();

    assert!(!mine.is_empty(), "expected at least one outbox event");

    // Sequence is contiguous starting at 1.
    let mut sequences: Vec<u64> = mine.iter().map(|e| e.sequence).collect();
    sequences.sort_unstable();
    let expected: Vec<u64> = (1..=sequences.len() as u64).collect();
    assert_eq!(sequences, expected, "sequence must be contiguous per run");

    // First event is run_started, last is run_completed -- both are
    // separate apply() calls from everything in between, proving ordering
    // survives across transaction boundaries.
    let by_seq: HashMap<u64, RunEventType> =
        mine.iter().map(|e| (e.sequence, e.event_type)).collect();
    assert_eq!(by_seq[&1], RunEventType::RunStarted);
    assert_eq!(
        by_seq[&(mine.len() as u64)],
        RunEventType::RunCompleted,
        "last event must be run_completed for a successful linear run"
    );

    // mark_events_pushed is idempotent and drains unpushed_events.
    harness
        .store
        .mark_events_pushed(&run_id, mine.len() as u64)
        .await
        .unwrap();
    let remaining = harness.store.unpushed_events(1000).await.unwrap();
    assert!(
        remaining.iter().all(|(id, _)| id != &run_id),
        "all events should be marked pushed"
    );

    // Re-marking is a no-op, not an error.
    harness
        .store
        .mark_events_pushed(&run_id, mine.len() as u64)
        .await
        .unwrap();
}

#[tokio::test]
async fn flow_run_snapshot_tracks_approve_and_reject_self_loop() {
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
    let run_id = harness
        .start(
            "human-loop",
            serde_json::json!({ "message": "add feature" }),
            Some("platform:run_abc123".into()),
        )
        .await;

    sleep(Duration::from_millis(50)).await;

    // Snapshot while waiting on the first attempt: run_id is the platform's
    // id (prefix stripped), the plan step is human_loop + waiting_approval.
    let snap = queries::flow_run_snapshot(harness.store.clone(), &run_id)
        .await
        .unwrap();
    assert_eq!(snap.run_id, "run_abc123");
    assert_eq!(snap.phase, "running");
    let plan = snap.steps.iter().find(|s| s.step_id == "plan").unwrap();
    assert!(plan.human_loop);
    assert_eq!(plan.status, "waiting_approval");
    assert_eq!(plan.attempt, 1);

    // Reject with feedback -> self-loop, attempt 2.
    approvals::reject_step(
        harness.store.clone(),
        harness.scheduler.clone(),
        approvals::RejectRequest {
            run_id: run_id.clone(),
            step_id: Some("plan".into()),
            feedback: "missing edge case".into(),
        },
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(100)).await;

    let snap = queries::flow_run_snapshot(harness.store.clone(), &run_id)
        .await
        .unwrap();
    let plan = snap.steps.iter().find(|s| s.step_id == "plan").unwrap();
    assert_eq!(
        plan.attempt, 2,
        "reject_input re-run must show up as the latest attempt, not a second entry"
    );
    assert_eq!(plan.status, "waiting_approval");
    assert!(
        plan.input_resolved
            .as_deref()
            .unwrap_or("")
            .contains("missing edge case"),
        "reject_input must fold the feedback into the re-run's input"
    );

    // Approve attempt 2 -> run completes.
    approvals::approve_step(
        harness.store.clone(),
        harness.scheduler.clone(),
        approvals::ApproveRequest {
            run_id: run_id.clone(),
            step_id: Some("plan".into()),
            comment: None,
        },
    )
    .await
    .unwrap();

    harness.wait_terminal(&run_id, Duration::from_secs(2)).await;

    let snap = queries::flow_run_snapshot(harness.store.clone(), &run_id)
        .await
        .unwrap();
    assert_eq!(snap.run_id, "run_abc123");
    assert_eq!(snap.phase, "completed");
    assert!(snap.completed_at.is_some());
    let implement = snap
        .steps
        .iter()
        .find(|s| s.step_id == "implement")
        .unwrap();
    assert_eq!(implement.status, "completed");
    assert!(!implement.human_loop);
}
