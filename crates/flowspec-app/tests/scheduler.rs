use flowspec_app::ports::{RunId, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::{FakeDevkitd, InMemoryFlowSource, InMemoryStateStore, Script};
use flowspec_app::use_cases::{
    approvals, cancel_run, queries, start_flow, start_flow::StartFlowRequest,
};
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use flowspec_domain::run::types::{Event, RunPhase, StepStatus};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
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

fn load_flows(fixtures: &[&str]) -> Vec<FlowDefinition> {
    fixtures.iter().map(|f| load_fixture(f)).collect()
}

struct Harness {
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    devkitd: Arc<FakeDevkitd>,
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
            devkitd,
            flows,
        }
    }

    async fn start(&self, flow_name: &str, inputs: Value) -> RunId {
        start_flow::start_flow(
            self.store.clone(),
            self.scheduler.clone(),
            self.flows.clone(),
            &config(),
            StartFlowRequest {
                flow_name: flow_name.into(),
                version_req: None,
                inputs,
                trigger: Value::Null,
                idempotency_key: None,
            },
        )
        .await
        .unwrap()
        .run_id
    }

    async fn status(&self, run_id: &RunId) -> queries::RunStatus {
        queries::get_run_status(
            self.store.clone(),
            Some(&|run_id: &RunId, step_id: &str| self.scheduler.liveness_ago(run_id, step_id)),
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
                // Allow a genuine pause (e.g. `waiting_approval`) to short-circuit
                // the wait: there are active steps but none of them is `running`.
                // The empty-active-steps case is *not* a pause -- it is the gating
                // window before `before_run` lets the first step activate, or a
                // future post-hook pre-activation gap, and must keep polling.
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
async fn linear_flow_completes() {
    let flows = load_flows(&["linear.yaml"]);
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
        .start("linear", serde_json::json!({ "message": "add feature" }))
        .await;
    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "completed");

    let record = harness.store.load_run(&run_id).await.unwrap();
    assert!(
        record
            .latest_steps()
            .iter()
            .all(|s| s.run.status == StepStatus::Completed)
    );
}

#[tokio::test]
async fn human_loop_approve_then_complete() {
    let flows = load_flows(&["human-loop.yaml"]);
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
        )
        .await;

    sleep(Duration::from_millis(50)).await;
    let status = harness.status(&run_id).await;
    // Should be paused at plan waiting approval.
    assert_eq!(status.phase, "running");
    assert_eq!(status.active_steps.len(), 1);
    assert_eq!(status.active_steps[0].step_id, "plan");

    let pending = queries::pending_approvals(
        harness.store.clone(),
        queries::PendingApprovalsRequest { run_id: None },
    )
    .await
    .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].run_id, run_id);

    approvals::approve_step(
        harness.store.clone(),
        harness.scheduler.clone(),
        approvals::ApproveRequest {
            run_id: run_id.clone(),
            step_id: Some("plan".into()),
            comment: Some("lgtm".into()),
        },
    )
    .await
    .unwrap();

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "completed");
}

#[tokio::test]
async fn human_loop_reject_loops_with_feedback() {
    let flows = load_flows(&["human-loop.yaml"]);
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
        )
        .await;
    sleep(Duration::from_millis(50)).await;

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

    // After rejection the plan step re-runs. Wait for it to pause again.
    sleep(Duration::from_millis(100)).await;
    let status = harness.status(&run_id).await;
    assert_eq!(status.phase, "running");

    let record = harness.store.load_run(&run_id).await.unwrap();
    let plan = record
        .latest_steps()
        .into_iter()
        .find(|s| s.run.step_id == "plan")
        .unwrap();
    assert_eq!(plan.run.attempt, 2);
    assert!(
        plan.run
            .input_resolved
            .as_ref()
            .unwrap()
            .contains("Feedback: missing edge case")
    );

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

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "completed");
}

#[tokio::test]
async fn fan_out_runs_branches_concurrently() {
    let flows = load_flows(&["fan-out.yaml"]);
    let scripts: HashMap<String, Script> = [
        ("build".into(), Script::Succeed(Value::String("ok".into()))),
        (
            "unit-tests".into(),
            Script::DelayThen(
                Duration::from_millis(100),
                Box::new(Script::Succeed(Value::String("unit-ok".into()))),
            ),
        ),
        (
            "integration-tests".into(),
            Script::DelayThen(
                Duration::from_millis(100),
                Box::new(Script::Succeed(Value::String("integ-ok".into()))),
            ),
        ),
        (
            "lint".into(),
            Script::DelayThen(
                Duration::from_millis(100),
                Box::new(Script::Succeed(Value::String("lint-ok".into()))),
            ),
        ),
        ("gate".into(), Script::Succeed(Value::Null)),
        ("report-failure".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness.start("fan-out", Value::Null).await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(3)).await;
    assert_eq!(status.phase, "completed");

    let invocations = harness.devkitd.invocations().await;
    let branch_starts: Vec<_> = invocations
        .iter()
        .filter(|(tool, args, _)| {
            tool == "agent-run"
                && ["unit-tests", "integration-tests", "lint"]
                    .iter()
                    .any(|s| args.get("step").and_then(|v| v.as_str()) == Some(s))
        })
        .map(|(_, _, t)| *t)
        .collect();
    assert_eq!(branch_starts.len(), 3);
    let span = branch_starts
        .iter()
        .max()
        .unwrap()
        .duration_since(*branch_starts.iter().min().unwrap());
    assert!(
        span < Duration::from_millis(50),
        "branches should start concurrently, span={span:?}"
    );
}

#[tokio::test]
async fn fan_out_fail_fast_cancels_siblings() {
    let flows = load_flows(&["fan-out.yaml"]);
    let scripts: HashMap<String, Script> = [
        ("build".into(), Script::Succeed(Value::String("ok".into()))),
        (
            "unit-tests".into(),
            Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                stdout: String::new(),
                stderr: "boom".into(),
                exit_code: 1,
            }),
        ),
        ("integration-tests".into(), Script::Hang),
        ("lint".into(), Script::Hang),
        ("report-failure".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness.start("fan-out", Value::Null).await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "failed");

    let record = harness.store.load_run(&run_id).await.unwrap();
    let statuses: HashMap<String, StepStatus> = record
        .latest_steps()
        .into_iter()
        .map(|s| (s.run.step_id.clone(), s.run.status))
        .collect();
    assert_eq!(statuses.get("unit-tests"), Some(&StepStatus::Failed));
    assert_eq!(
        statuses.get("integration-tests"),
        Some(&StepStatus::Cancelled)
    );
    assert_eq!(statuses.get("lint"), Some(&StepStatus::Cancelled));

    // The step's own job failed (no after: hook involved) -- failed_in must
    // say "step", and the structured detail (written by
    // Mutation::SetStepFailureDetail, straight from the live DevkitdError,
    // independently of the flattened failure_reason string) must be there.
    let unit_tests = status
        .steps
        .iter()
        .find(|s| s.step_id == "unit-tests")
        .expect("unit-tests step present");
    assert_eq!(unit_tests.failed_in.as_deref(), Some("step"));
    assert!(unit_tests.hooks.is_empty());
    let detail = unit_tests
        .failure
        .as_ref()
        .expect("failed step must carry structured detail");
    assert_eq!(detail.kind, "tool_error");
    assert_eq!(detail.exit_code, Some(1));
    assert_eq!(detail.stderr.as_deref(), Some("boom"));
}

#[tokio::test]
async fn retries_run_before_on_failure() {
    let flows = load_flows(&["failure-routing.yaml"]);
    let scripts: HashMap<String, Script> = [
        (
            "implement".into(),
            Script::Sequence(vec![
                Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                    stdout: String::new(),
                    stderr: "attempt 1 failed".into(),
                    exit_code: 1,
                }),
                Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                    stdout: String::new(),
                    stderr: "attempt 2 failed".into(),
                    exit_code: 1,
                }),
                Script::Succeed(Value::String("code".into())),
            ]),
        ),
        ("test".into(), Script::Succeed(Value::String("pass".into()))),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness.start("failure-routing", Value::Null).await;
    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "completed");

    let record = harness.store.load_run(&run_id).await.unwrap();
    let implement = record
        .latest_steps()
        .into_iter()
        .find(|s| s.run.step_id == "implement")
        .unwrap();
    assert_eq!(implement.run.attempt, 3);
    assert_eq!(implement.run.status, StepStatus::Completed);
}

#[tokio::test]
async fn on_failure_routes_back_to_implement() {
    let flows = load_flows(&["failure-routing.yaml"]);
    let scripts: HashMap<String, Script> = [
        (
            "implement".into(),
            Script::Sequence(vec![
                Script::Succeed(Value::String("code".into())),
                Script::Succeed(Value::String("code2".into())),
            ]),
        ),
        (
            "test".into(),
            Script::Sequence(vec![
                Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                    stdout: String::new(),
                    stderr: "tests failed".into(),
                    exit_code: 1,
                }),
                Script::Succeed(Value::String("pass".into())),
            ]),
        ),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness.start("failure-routing", Value::Null).await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "completed");

    let record = harness.store.load_run(&run_id).await.unwrap();
    let test = record
        .latest_steps()
        .into_iter()
        .find(|s| s.run.step_id == "test")
        .unwrap();
    assert_eq!(test.run.attempt, 2);
    assert_eq!(test.run.status, StepStatus::Completed);
}

#[tokio::test]
async fn run_timeout_cancels_active_step() {
    let flows = load_flows(&["run-timeout.yaml"]);
    let scripts: HashMap<String, Script> = [("hang".into(), Script::Hang)].into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness.start("run-timeout", Value::Null).await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(3)).await;
    assert_eq!(status.phase, "failed");

    let record = harness.store.load_run(&run_id).await.unwrap();
    let hang = record
        .latest_steps()
        .into_iter()
        .find(|s| s.run.step_id == "hang")
        .unwrap();
    assert_eq!(hang.run.status, StepStatus::Cancelled);
}

#[tokio::test]
async fn cancel_run_terminates() {
    let flows = load_flows(&["linear.yaml"]);
    let scripts: HashMap<String, Script> = [("plan".into(), Script::Hang)].into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start("linear", serde_json::json!({ "message": "add feature" }))
        .await;
    sleep(Duration::from_millis(50)).await;

    cancel_run::cancel_run(
        harness.store.clone(),
        harness.scheduler.clone(),
        cancel_run::CancelRunRequest {
            run_id: run_id.clone(),
        },
    )
    .await
    .unwrap();

    let status = harness.status(&run_id).await;
    assert_eq!(status.phase, "cancelled");
}

#[tokio::test]
async fn concurrent_submits_serialize() {
    let flows = load_flows(&["human-loop.yaml"]);
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
        )
        .await;
    harness.wait_terminal(&run_id, Duration::from_secs(2)).await;

    let mut handles = vec![];
    for i in 0..10 {
        let scheduler = harness.scheduler.clone();
        let run_id = run_id.clone();
        handles.push(tokio::spawn(async move {
            if i % 2 == 0 {
                scheduler
                    .submit(
                        run_id,
                        Event::StepApproved {
                            step_id: "plan".into(),
                            comment: None,
                        },
                    )
                    .await;
            } else {
                scheduler.submit(run_id, Event::CancelRequested).await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let record = harness.store.load_run(&run_id).await.unwrap();
    let phase = flowspec_app::scheduler::run_phase(&record.run_state());
    assert!(matches!(phase, RunPhase::Completed | RunPhase::Cancelled));
}

#[tokio::test]
async fn recovery_reattaches_to_running_step() {
    let flows = load_flows(&["linear.yaml"]);
    let scripts: HashMap<String, Script> = [(
        "plan".into(),
        Script::DelayThen(
            Duration::from_millis(200),
            Box::new(Script::Succeed(Value::String("PLAN.md".into()))),
        ),
    )]
    .into();
    let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
    let devkitd = Arc::new(FakeDevkitd::new(scripts).with_poll_interval(Duration::from_millis(10)));
    let flows = Arc::new(InMemoryFlowSource::new(flows));

    let scheduler1 = Arc::new(Scheduler::new(store.clone(), devkitd.clone(), config()));
    let run_id = start_flow::start_flow(
        store.clone(),
        scheduler1.clone(),
        flows.clone(),
        &config(),
        StartFlowRequest {
            flow_name: "linear".into(),
            version_req: None,
            inputs: serde_json::json!({ "message": "add feature" }),
            trigger: Value::Null,
            idempotency_key: None,
        },
    )
    .await
    .unwrap()
    .run_id;

    // Wait until plan is running, then "crash" the scheduler by shutting it
    // down (aborting in-flight wait tasks) and dropping it.
    loop {
        let record = store.load_run(&run_id).await.unwrap();
        if record.latest_steps().iter().any(|s| {
            s.run.step_id == "plan" && s.run.status == StepStatus::Running && s.job_id.is_some()
        }) {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    scheduler1.shutdown();
    drop(scheduler1);

    // Build a new scheduler over the same store and fake; recover should re-attach.
    let scheduler2 = Arc::new(Scheduler::new(store.clone(), devkitd.clone(), config()));
    scheduler2.recover().await;

    let status = timeout(Duration::from_secs(2), async {
        loop {
            let status = queries::get_run_status(
                store.clone(),
                Some(&|run_id: &RunId, step_id: &str| scheduler2.liveness_ago(run_id, step_id)),
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
    .expect("run did not complete after recovery");

    assert_eq!(status.phase, "completed");
}

// ---------------------------------------------------------------------------
// Hook subsystem coverage
// ---------------------------------------------------------------------------

/// Helper: collect (tool, step_id?, timestamp) for each devkitd start, in order.
async fn invocation_trace(devkitd: &FakeDevkitd) -> Vec<(String, Option<String>, Instant)> {
    devkitd
        .invocations()
        .await
        .into_iter()
        .map(|(tool, args, ts)| {
            let step_id = args.get("step").and_then(|v| v.as_str()).map(String::from);
            (tool, step_id, ts)
        })
        .collect()
}

fn hook_script_value() -> Value {
    // Used as the step output JSON for `build`/`validate` in the hooks fixture.
    Value::String("ok".into())
}

/// `before_run` completes before the first step activates (gating), and
/// `after_run` hooks gated by `when:` only fire for the matching terminal
/// phase. Drives the full `hooks.yaml` fixture end-to-end.
#[tokio::test]
async fn hooks_before_run_gates_entry_and_after_run_when_filters() {
    let flows = load_flows(&["hooks.yaml"]);
    let scripts: HashMap<String, Script> = [
        (
            "audit".into(),
            Script::DelayThen(
                Duration::from_millis(60),
                Box::new(Script::Succeed(Value::Null)),
            ),
        ),
        ("build".into(), Script::Succeed(hook_script_value())),
        ("validate".into(), Script::Succeed(Value::Null)),
        ("post-build".into(), Script::Succeed(Value::Null)),
        ("notify-success".into(), Script::Succeed(Value::Null)),
        ("notify-failed".into(), Script::Succeed(Value::Null)),
        ("notify-cancelled".into(), Script::Succeed(Value::Null)),
        ("notify-always".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start("hooks", serde_json::json!({ "message": "ship it" }))
        .await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(3)).await;
    assert_eq!(status.phase, "completed");

    let trace = invocation_trace(&harness.devkitd).await;
    let tools: Vec<&str> = trace.iter().map(|(t, _, _)| t.as_str()).collect();
    assert_eq!(
        tools,
        [
            "audit",
            "agent-run",
            "post-build",
            "agent-run",
            "notify-success",
            "notify-always"
        ],
        "before_run gates entry, after_step gates routing, after_run only fires matching when:"
    );

    // Validate returned at the step-key slot (second agent-run), proving
    // routing happened post after_step hook.
    assert_eq!(trace[1].1.as_deref(), Some("build"));
    assert_eq!(trace[3].1.as_deref(), Some("validate"));

    // Gating is observable: the entry step's devkitd `start` call records a
    // timestamp strictly after the before_run hook fired, with at least the
    // scripted delay between them.
    let audit_start = trace[0].2;
    let build_start = trace[1].2;
    assert!(
        build_start > audit_start,
        "build should start strictly after the audit hook"
    );
    assert!(
        build_start.duration_since(audit_start) >= Duration::from_millis(40),
        "gating delay visible: span={:?}",
        build_start.duration_since(audit_start)
    );
}

/// `before_run` hook fails (no `always_continue`) -> the run fails and the
/// entry step is never dispatched to devkitd.
#[tokio::test]
async fn hooks_before_run_failure_fails_run_without_starting_entry() {
    let flows = load_flows(&["hooks-gating-failure.yaml"]);
    let scripts: HashMap<String, Script> = [
        (
            "gate".into(),
            Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                stdout: String::new(),
                stderr: "gate rejected".into(),
                exit_code: 1,
            }),
        ),
        ("build".into(), Script::Hang),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start(
            "hooks-gating-failure",
            serde_json::json!({ "message": "go" }),
        )
        .await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "failed");
    assert!(
        status.steps.is_empty(),
        "no step ever activated -- this is the case that was previously \
         undiagnosable: phase failed, steps empty, and (before this fix) no \
         indication anywhere of why"
    );

    // The reason is not lost: it's on run_hooks, with full structured detail.
    assert_eq!(status.run_hooks.len(), 1);
    let gate = &status.run_hooks[0];
    assert_eq!(gate.hook, "gate");
    assert_eq!(gate.phase, "before_run");
    assert_eq!(gate.status, "failed");
    let failure = gate
        .failure
        .as_ref()
        .expect("failed hook must carry detail");
    assert_eq!(failure.kind, "tool_error");
    assert_eq!(failure.exit_code, Some(1));
    assert_eq!(failure.stderr.as_deref(), Some("gate rejected"));

    // And the same is true one layer down, straight off the store.
    let hook_runs = harness.store.list_hook_runs(&run_id).await.unwrap();
    assert_eq!(hook_runs.len(), 1);
    assert!(
        hook_runs[0]
            .failure_reason
            .as_deref()
            .unwrap()
            .contains("gate rejected")
    );

    // Only the gate hook was ever dispatched to devkitd; `build` never started.
    let trace = invocation_trace(&harness.devkitd).await;
    assert!(
        trace.iter().all(|(t, _, _)| t != "agent-run"),
        "entry step must not start when a gating hook fails: {trace:?}"
    );
    assert_eq!(harness.devkitd.start_count().await, 1);
}

/// `before_run` hook fails but `always_continue: true` -> the run proceeds
/// to the entry step and completes normally. The hook's failure is audit
/// (recorded in `hook_runs`), not a run failure.
#[tokio::test]
async fn hooks_before_run_failure_with_always_continue_continues() {
    let flows = load_flows(&["hooks-always-continue.yaml"]);
    let scripts: HashMap<String, Script> = [
        (
            "gate".into(),
            Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                stdout: String::new(),
                stderr: "non-fatal".into(),
                exit_code: 1,
            }),
        ),
        ("build".into(), Script::Succeed(hook_script_value())),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start(
            "hooks-always-continue",
            serde_json::json!({ "message": "go" }),
        )
        .await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(status.phase, "completed");

    let trace = invocation_trace(&harness.devkitd).await;
    let tools: Vec<&str> = trace.iter().map(|(t, _, _)| t.as_str()).collect();
    assert_eq!(tools, ["gate", "agent-run"]);
    assert_eq!(trace[1].1.as_deref(), Some("build"));
}

/// A per-step `after` hook failing routes the step as failed (gating).
/// Validate (the downstream `on_success` target) is never dispatched.
#[tokio::test]
async fn hooks_after_step_failure_routes_step_failed() {
    let flows = load_flows(&["hooks.yaml"]);
    let scripts: HashMap<String, Script> = [
        ("audit".into(), Script::Succeed(Value::Null)),
        ("build".into(), Script::Succeed(hook_script_value())),
        ("validate".into(), Script::Succeed(Value::Null)),
        (
            "post-build".into(),
            Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                stdout: String::new(),
                stderr: "post-build asserts".into(),
                exit_code: 1,
            }),
        ),
        ("notify-success".into(), Script::Succeed(Value::Null)),
        ("notify-failed".into(), Script::Succeed(Value::Null)),
        ("notify-cancelled".into(), Script::Succeed(Value::Null)),
        ("notify-always".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start("hooks", serde_json::json!({ "message": "ship it" }))
        .await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    // build's `on_failure: done` -> terminal, phase derived from a failed step
    // is `failed`; after_run when=failed fires notify-failed + notify-always.
    assert_eq!(status.phase, "failed");

    let trace = invocation_trace(&harness.devkitd).await;
    let tools: Vec<&str> = trace.iter().map(|(t, _, _)| t.as_str()).collect();
    assert_eq!(
        tools,
        [
            "audit",
            "agent-run",
            "post-build",
            "notify-failed",
            "notify-always"
        ],
        "after_step failure routes step failed and skips on_success targets"
    );
    // The validate step never started (gating held it back).
    assert!(
        !trace
            .iter()
            .any(|(_, step, _)| step.as_deref() == Some("validate")),
        "validate must not run when its predecessor's after hook failed"
    );

    // `build` itself succeeded (Script::Succeed above) -- it's `post-build`,
    // its `after:` hook, that failed. `failed_in` must say so, distinguishing
    // this from the agent itself failing.
    let build = status
        .steps
        .iter()
        .find(|s| s.step_id == "build")
        .expect("build step present");
    assert_eq!(build.status, "failed");
    assert_eq!(build.failed_in.as_deref(), Some("after_hook"));
    assert_eq!(build.hooks.len(), 1);
    assert_eq!(build.hooks[0].hook, "post-build");
    assert_eq!(build.hooks[0].status, "failed");
    assert_eq!(
        build.hooks[0].failure.as_ref().unwrap().stderr.as_deref(),
        Some("post-build asserts")
    );
}

/// An `after_run` hook failing is audit-only: the run still reaches its
/// engine-decided terminal phase (Completed here). Crucially, the
/// continuation that applies `MarkRunTerminal` still runs -- without that the
/// run would be stuck `Running`.
#[tokio::test]
async fn hooks_after_run_failure_is_audit_only_and_run_still_completes() {
    let flows = load_flows(&["hooks.yaml"]);
    let scripts: HashMap<String, Script> = [
        ("audit".into(), Script::Succeed(Value::Null)),
        ("build".into(), Script::Succeed(hook_script_value())),
        ("validate".into(), Script::Succeed(Value::Null)),
        ("post-build".into(), Script::Succeed(Value::Null)),
        (
            "notify-success".into(),
            Script::Fail(flowspec_app::ports::DevkitdError::ToolError {
                stdout: String::new(),
                stderr: "channel down".into(),
                exit_code: 1,
            }),
        ),
        ("notify-failed".into(), Script::Succeed(Value::Null)),
        ("notify-cancelled".into(), Script::Succeed(Value::Null)),
        ("notify-always".into(), Script::Succeed(Value::Null)),
    ]
    .into();
    let harness = Harness::new(flows, scripts);
    let run_id = harness
        .start("hooks", serde_json::json!({ "message": "ship it" }))
        .await;

    let status = harness.wait_terminal(&run_id, Duration::from_secs(2)).await;
    assert_eq!(
        status.phase, "completed",
        "an after_run hook failure must not flip the run phase away from the engine's decision"
    );

    let trace = invocation_trace(&harness.devkitd).await;
    let tools: Vec<&str> = trace.iter().map(|(t, _, _)| t.as_str()).collect();
    assert_eq!(
        tools,
        [
            "audit",
            "agent-run",
            "post-build",
            "agent-run",
            "notify-success",
            "notify-always"
        ]
    );
    // notify-failed never fires: at this terminal phase it doesn't pass `when:`.
    assert!(
        !tools.contains(&"notify-failed"),
        "when: failed should not fire on a completed run"
    );
}

/// Recovery of a run interrupted mid `before_run` gating. The run was in
/// `Running` phase with no step rows yet; without `ReissueRunStarted` it would
/// hang forever. Crash the scheduler with the audit hook in flight, rebuild,
/// `recover()`, and assert the run completes.
#[tokio::test]
async fn recovery_of_gating_hook_completes_run() {
    let flows = load_flows(&["hooks.yaml"]);
    let scripts: HashMap<String, Script> = [
        (
            "audit".into(),
            Script::DelayThen(
                Duration::from_millis(250),
                Box::new(Script::Succeed(Value::Null)),
            ),
        ),
        ("build".into(), Script::Succeed(hook_script_value())),
        ("validate".into(), Script::Succeed(Value::Null)),
        ("post-build".into(), Script::Succeed(Value::Null)),
        ("notify-success".into(), Script::Succeed(Value::Null)),
        ("notify-failed".into(), Script::Succeed(Value::Null)),
        ("notify-cancelled".into(), Script::Succeed(Value::Null)),
        ("notify-always".into(), Script::Succeed(Value::Null)),
    ]
    .into();

    let store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
    let devkitd = Arc::new(FakeDevkitd::new(scripts).with_poll_interval(Duration::from_millis(10)));
    let flows = Arc::new(InMemoryFlowSource::new(flows));

    let scheduler1 = Arc::new(Scheduler::new(store.clone(), devkitd.clone(), config()));
    let run_id = start_flow::start_flow(
        store.clone(),
        scheduler1.clone(),
        flows.clone(),
        &config(),
        StartFlowRequest {
            flow_name: "hooks".into(),
            version_req: None,
            inputs: serde_json::json!({ "message": "ship it" }),
            trigger: Value::Null,
            idempotency_key: None,
        },
    )
    .await
    .unwrap()
    .run_id;

    // Wait until the before_run hook has been dispatched AND the entry step
    // has not yet started -- that is the gating-mid window where a crash
    // leaves no step row behind.
    loop {
        let trace = invocation_trace(&devkitd).await;
        let audit_started = trace.iter().any(|(t, _, _)| t == "audit");
        let build_started = trace
            .iter()
            .any(|(_, step, _)| step.as_deref() == Some("build"));
        if audit_started && !build_started {
            // Drain a beat to ensure the hook's wait task is genuinely in
            // flight (the gate delays 250ms, so 30ms in we are mid-hook).
            sleep(Duration::from_millis(30)).await;
            break;
        }
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        devkitd.start_count().await,
        1,
        "only the audit hook should have started"
    );

    // Crash the scheduler by aborting in-flight tasks (without cancelling
    // devkitd jobs) and dropping it.
    scheduler1.shutdown();
    drop(scheduler1);

    // Build a fresh scheduler over the same store and fake; recovery should
    // re-issue RunStarted, re-run the gating hook, and let the flow complete.
    let scheduler2 = Arc::new(Scheduler::new(store.clone(), devkitd.clone(), config()));
    scheduler2.recover().await;

    let status = timeout(Duration::from_secs(3), async {
        loop {
            let status = queries::get_run_status(
                store.clone(),
                Some(&|run_id: &RunId, step_id: &str| scheduler2.liveness_ago(run_id, step_id)),
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
    .expect("recovered run did not complete -- stuck mid gating hook");

    assert_eq!(status.phase, "completed");

    // The re-issue re-ran the before_run hook (audit invoked twice) and then
    // the entry step finally started. Sanity check: at least one build call
    // happened post-recovery.
    let trace = invocation_trace(&devkitd).await;
    assert!(
        trace
            .iter()
            .any(|(_, step, _)| step.as_deref() == Some("build")),
        "entry step must run after recovery: {trace:?}"
    );
    assert_eq!(
        trace.iter().filter(|(t, _, _)| t == "audit").count(),
        2,
        "ReissueRunStarted re-runs the before_run gating hook exactly once"
    );
}
