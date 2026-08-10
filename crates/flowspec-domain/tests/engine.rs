#![allow(clippy::field_reassign_with_default)]
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use flowspec_domain::run::types::{Command, Event, RunPhase, RunState, StepRun, StepStatus};
use flowspec_domain::run::{decide, derive};
use std::path::PathBuf;
use std::time::SystemTime;

fn load_flow(rel: &str) -> FlowDefinition {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel);
    let text = std::fs::read_to_string(path).unwrap();
    let file: FlowFile = serde_yaml_ng::from_str(&text).unwrap();
    file.into_definitions().remove(0)
}

fn now() -> SystemTime {
    SystemTime::now()
}

fn activated_ids(commands: &[Command]) -> Vec<String> {
    commands
        .iter()
        .flat_map(|c| match c {
            Command::ActivateSteps(v) => v.iter().map(|a| a.step_id.clone()).collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect()
}

fn mark(state: &mut RunState, step_id: &str, status: StepStatus) {
    if let Some(s) = state.step_mut(step_id) {
        s.status = status;
    } else {
        let mut r = StepRun::pending(step_id);
        r.status = status;
        r.attempt = 1;
        state.steps.push(r);
    }
}

/// Like `mark`, but also sets the step's output -- needed whenever another
/// step's template references `{{ steps.<id>.output }}`.
fn mark_completed_with_output(state: &mut RunState, step_id: &str, output: serde_json::Value) {
    mark(state, step_id, StepStatus::Completed);
    if let Some(s) = state.step_mut(step_id) {
        s.output = Some(output);
    }
}

#[test]
fn run_started_activates_the_entry_step() {
    let flow = load_flow("linear.yaml");
    let mut state = RunState::default();
    state.inputs = serde_json::json!({ "message": "add feature" });
    let commands = decide(&flow, &state, Event::RunStarted, now()).unwrap();
    assert_eq!(activated_ids(&commands), vec!["plan"]);
}

#[test]
fn step_completed_routes_to_on_success() {
    let flow = load_flow("linear.yaml");
    let mut state = RunState::default();
    mark(&mut state, "plan", StepStatus::Running);

    let commands = decide(
        &flow,
        &state,
        Event::StepCompleted {
            step_id: "plan".into(),
            output: serde_json::json!("PLAN.md"),
        },
        now(),
    )
    .unwrap();

    assert_eq!(activated_ids(&commands), vec!["implement"]);
}

#[test]
fn fan_out_activates_all_branches_and_gate_waits_on_needs() {
    let flow = load_flow("fan-out.yaml");
    let mut state = RunState::default();
    mark(&mut state, "build", StepStatus::Running);

    let commands = decide(
        &flow,
        &state,
        Event::StepCompleted {
            step_id: "build".into(),
            output: serde_json::json!("ok"),
        },
        now(),
    )
    .unwrap();

    let mut activated = activated_ids(&commands);
    activated.sort();
    assert_eq!(activated, vec!["integration-tests", "lint", "unit-tests"]);

    // Completing just one branch must not open the `gate` fan-in yet.
    let mut state2 = RunState::default();
    mark(&mut state2, "build", StepStatus::Completed);
    mark(&mut state2, "unit-tests", StepStatus::Running);
    mark(&mut state2, "integration-tests", StepStatus::Running);
    mark(&mut state2, "lint", StepStatus::Running);
    let commands2 = decide(
        &flow,
        &state2,
        Event::StepCompleted {
            step_id: "unit-tests".into(),
            output: serde_json::json!("ok"),
        },
        now(),
    )
    .unwrap();
    assert!(
        activated_ids(&commands2).is_empty(),
        "gate must not open until all needs are satisfied"
    );
}

#[test]
fn fan_in_gate_opens_once_all_needs_are_completed() {
    let flow = load_flow("fan-out.yaml");
    let mut state = RunState::default();
    mark_completed_with_output(&mut state, "build", serde_json::json!("ok"));
    mark_completed_with_output(&mut state, "unit-tests", serde_json::json!("ok"));
    mark(&mut state, "integration-tests", StepStatus::Running);
    mark_completed_with_output(&mut state, "lint", serde_json::json!("ok"));

    let commands = decide(
        &flow,
        &state,
        Event::StepCompleted {
            step_id: "integration-tests".into(),
            output: serde_json::json!("ok"),
        },
        now(),
    )
    .unwrap();

    assert_eq!(activated_ids(&commands), vec!["gate"]);
}

#[test]
fn fail_fast_cancels_active_siblings() {
    let flow = load_flow("fan-out.yaml");
    let mut state = RunState::default();
    mark(&mut state, "build", StepStatus::Completed);
    mark(&mut state, "unit-tests", StepStatus::Running);
    mark(&mut state, "integration-tests", StepStatus::Running);
    mark(&mut state, "lint", StepStatus::Running);

    let commands = decide(
        &flow,
        &state,
        Event::StepFailed {
            step_id: "unit-tests".into(),
            reason: "boom".into(),
        },
        now(),
    )
    .unwrap();

    let cancelled: Vec<String> = commands
        .iter()
        .flat_map(|c| match c {
            Command::CancelSteps(ids) => ids.clone(),
            _ => vec![],
        })
        .collect();
    let mut cancelled = cancelled;
    cancelled.sort();
    assert_eq!(cancelled, vec!["integration-tests", "lint"]);
}

#[test]
fn retries_reactivate_before_on_failure_routes() {
    let flow = load_flow("failure-routing.yaml"); // implement has retries: 2
    let mut state = RunState::default();
    mark(&mut state, "implement", StepStatus::Running);

    let commands = decide(
        &flow,
        &state,
        Event::StepFailed {
            step_id: "implement".into(),
            reason: "boom".into(),
        },
        now(),
    )
    .unwrap();

    match &commands[..] {
        [Command::ActivateSteps(activations)] => {
            assert_eq!(activations.len(), 1);
            assert_eq!(activations[0].step_id, "implement");
            assert_eq!(activations[0].attempt, 2);
        }
        other => panic!("expected a single retry activation, got {other:?}"),
    }
}

#[test]
fn retries_exhausted_routes_via_on_failure() {
    let flow = load_flow("failure-routing.yaml");
    let mut state = RunState::default();
    let mut run = StepRun::pending("implement");
    run.status = StepStatus::Running;
    run.attempt = 3; // already used attempts 1,2,3 -- retries: 2 means attempt <= 2 retries
    state.steps.push(run);

    let commands = decide(
        &flow,
        &state,
        Event::StepFailed {
            step_id: "implement".into(),
            reason: "boom".into(),
        },
        now(),
    )
    .unwrap();

    // implement.on_failure: done -- no further activation, and the run terminates failed.
    assert!(activated_ids(&commands).is_empty());
    assert!(
        commands
            .iter()
            .any(|c| matches!(c, Command::MarkRunTerminal(RunPhase::Failed)))
    );
}

#[test]
fn approval_routes_on_approve_and_rejection_self_loops_with_feedback() {
    let flow = load_flow("human-loop.yaml");

    let mut state = RunState::default();
    state.inputs = serde_json::json!({ "message": "add feature" });
    mark(&mut state, "plan", StepStatus::WaitingApproval);
    if let Some(s) = state.step_mut("plan") {
        s.output = Some(serde_json::json!("PLAN.md"));
    }
    let approved = decide(
        &flow,
        &state,
        Event::StepApproved {
            step_id: "plan".into(),
            comment: Some("lgtm".into()),
        },
        now(),
    )
    .unwrap();
    assert_eq!(activated_ids(&approved), vec!["implement"]);

    let mut state2 = RunState::default();
    state2.inputs = serde_json::json!({ "message": "add feature" });
    mark(&mut state2, "plan", StepStatus::WaitingApproval);
    if let Some(s) = state2.step_mut("plan") {
        s.attempt = 1;
    }
    let rejected = decide(
        &flow,
        &state2,
        Event::StepRejected {
            step_id: "plan".into(),
            feedback: "missing edge case".into(),
        },
        now(),
    )
    .unwrap();
    match &rejected[..] {
        [
            Command::MarkStepStatus { .. },
            Command::ActivateSteps(activations),
        ] => {
            assert_eq!(activations[0].step_id, "plan");
            assert_eq!(activations[0].attempt, 2);
            assert!(activations[0].input.contains("Feedback: missing edge case"));
        }
        other => panic!("expected mark + self-loop reactivation, got {other:?}"),
    }
}

#[test]
fn run_deadline_exceeded_cancels_active_steps_and_terminates_failed() {
    let mut state = RunState::default();
    mark(&mut state, "a", StepStatus::Running);
    mark(&mut state, "b", StepStatus::WaitingApproval);

    let commands = decide(
        &FlowDefinition {
            name: "x".into(),
            version: "1.0.0".into(),
            description: None,
            inputs: Default::default(),
            outputs: Default::default(),
            defaults: Default::default(),
            timeout: None,
            lifecycle: Default::default(),
            metadata: Default::default(),
            flowspec_version: None,
            steps: vec![],
        },
        &state,
        Event::RunDeadlineExceeded,
        now(),
    )
    .unwrap();

    let cancelled: Vec<String> = commands
        .iter()
        .flat_map(|c| match c {
            Command::CancelSteps(ids) => ids.clone(),
            _ => vec![],
        })
        .collect();
    let mut cancelled = cancelled;
    cancelled.sort();
    assert_eq!(cancelled, vec!["a", "b"]);
    assert!(
        commands
            .iter()
            .any(|c| matches!(c, Command::MarkRunTerminal(RunPhase::Failed)))
    );
}

#[test]
fn terminal_transition_marks_never_activated_steps_skipped() {
    let flow = load_flow("fan-out.yaml");
    let mut state = RunState::default();
    mark(&mut state, "build", StepStatus::Completed);
    mark(&mut state, "unit-tests", StepStatus::Running);
    mark(&mut state, "integration-tests", StepStatus::Failed);
    mark(&mut state, "lint", StepStatus::Cancelled);
    // gate and report-failure never activated in this state.

    let commands = decide(
        &flow,
        &state,
        Event::StepFailed {
            step_id: "unit-tests".into(),
            reason: "boom".into(),
        },
        now(),
    )
    .unwrap();

    // report-failure activates via on_failure (needs: build, unit-tests, integration-tests, lint
    // -- not all satisfied since integration-tests/lint are cancelled, not completed -- so it
    // does NOT activate; the run has no more work and terminates failed with gate/report-failure skipped).
    let skipped: Vec<String> = commands
        .iter()
        .flat_map(|c| match c {
            Command::MarkStepStatus {
                step_id,
                status: StepStatus::Skipped,
            } => vec![step_id.clone()],
            _ => vec![],
        })
        .collect();
    let mut skipped = skipped;
    skipped.sort();
    assert_eq!(skipped, vec!["gate", "report-failure"]);
    assert!(
        commands
            .iter()
            .any(|c| matches!(c, Command::MarkRunTerminal(RunPhase::Failed)))
    );
}

#[test]
fn phase_and_active_steps_are_derived_consistently() {
    let mut state = RunState::default();
    mark(&mut state, "a", StepStatus::Running);
    assert_eq!(derive::phase(&state), RunPhase::Running);
    assert_eq!(derive::active_steps(&state), vec!["a".to_string()]);

    mark(&mut state, "a", StepStatus::Completed);
    assert_eq!(derive::phase(&state), RunPhase::Completed);
    assert!(derive::active_steps(&state).is_empty());
}
