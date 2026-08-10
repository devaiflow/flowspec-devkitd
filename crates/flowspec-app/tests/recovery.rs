use flowspec_app::ports::{RunRecord, StepRecord};
use flowspec_app::recovery::{RecoveryAction, RecoveryReason, recovery_plan};
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_domain::run::types::{RunPhase, StepRun, StepStatus};
use serde_json::json;
use std::time::SystemTime;

fn empty_flow() -> FlowDefinition {
    FlowDefinition {
        name: "empty".into(),
        version: "1.0.0".into(),
        description: None,
        inputs: Default::default(),
        outputs: Default::default(),
        defaults: Default::default(),
        timeout: None,
        lifecycle: Default::default(),
        metadata: Default::default(),
        flowspec_version: None,
        steps: Vec::new(),
    }
}

fn run_with_steps(run_id: &str, steps: Vec<StepRecord>) -> RunRecord {
    RunRecord {
        run_id: run_id.into(),
        flow_name: "empty".into(),
        flow_version: "1.0.0".into(),
        definition: empty_flow(),
        inputs: json!({}),
        trigger: json!({}),
        phase: RunPhase::Running,
        cancelled: false,
        created_at: SystemTime::now(),
        updated_at: SystemTime::now(),
        idempotency_key: None,
        steps,
    }
}

fn step(status: StepStatus, attempt: u32) -> StepRun {
    StepRun {
        status,
        attempt,
        ..StepRun::pending("plan")
    }
}

#[test]
fn running_step_with_job_id_reattaches() {
    let mut s = step(StepStatus::Running, 1);
    s.step_id = "plan".into();
    let mut rec = StepRecord::pending("plan");
    rec.run = s;
    rec.job_id = Some("job_123".into());

    let run = run_with_steps("run_1", vec![rec]);
    let actions = recovery_plan(vec![run]);

    assert_eq!(
        actions,
        vec![RecoveryAction::ReattachStep {
            run_id: "run_1".into(),
            step_id: "plan".into(),
            attempt: 1,
            job_id: "job_123".into(),
        }]
    );
}

#[test]
fn running_step_without_job_id_fails_as_interrupted() {
    let mut s = step(StepStatus::Running, 1);
    s.step_id = "plan".into();
    let rec = StepRecord {
        run: s,
        job_id: None,
        with_resolved: None,
        failure_detail: None,
    };

    let run = run_with_steps("run_1", vec![rec]);
    let actions = recovery_plan(vec![run]);

    assert_eq!(
        actions,
        vec![RecoveryAction::FailStep {
            run_id: "run_1".into(),
            step_id: "plan".into(),
            attempt: 1,
            reason: RecoveryReason::Interrupted,
        }]
    );
}

#[test]
fn waiting_approval_is_left_alone() {
    let mut s = step(StepStatus::WaitingApproval, 1);
    s.step_id = "plan".into();
    let rec = StepRecord {
        run: s,
        job_id: None,
        with_resolved: None,
        failure_detail: None,
    };

    let run = run_with_steps("run_1", vec![rec]);
    let actions = recovery_plan(vec![run]);

    assert_eq!(
        actions,
        vec![RecoveryAction::LeaveWaitingApproval {
            run_id: "run_1".into(),
            step_id: "plan".into(),
            attempt: 1,
        }]
    );
}

#[test]
fn waiting_on_subflow_rechecks_child() {
    let mut s = step(StepStatus::WaitingOnSubflow, 1);
    s.step_id = "deploy".into();
    s.child_run_id = Some("child_1".into());
    let rec = StepRecord {
        run: s,
        job_id: None,
        with_resolved: None,
        failure_detail: None,
    };

    let run = run_with_steps("run_1", vec![rec]);
    let actions = recovery_plan(vec![run]);

    assert_eq!(
        actions,
        vec![RecoveryAction::RecheckSubflow {
            run_id: "run_1".into(),
            step_id: "deploy".into(),
            attempt: 1,
            child_run_id: "child_1".into(),
        }]
    );
}

#[test]
fn terminal_steps_produce_no_actions() {
    let mut s = step(StepStatus::Completed, 1);
    s.step_id = "plan".into();
    let rec = StepRecord {
        run: s,
        job_id: None,
        with_resolved: None,
        failure_detail: None,
    };

    let run = run_with_steps("run_1", vec![rec]);
    let actions = recovery_plan(vec![run]);
    assert!(actions.is_empty());
}

#[test]
fn running_run_with_no_step_rows_reissues_run_started() {
    // A `Running` run that never advanced past gating — interrupted mid
    // `before_run` hook before the first step row was ever inserted. Without
    // a `ReissueRunStarted` action this run would hang forever.
    let run = run_with_steps("run_1", vec![]);
    let actions = recovery_plan(vec![run]);
    assert_eq!(
        actions,
        vec![RecoveryAction::ReissueRunStarted {
            run_id: "run_1".into(),
        }]
    );
}

#[test]
fn only_latest_attempt_is_considered() {
    let mut old = step(StepStatus::Running, 1);
    old.step_id = "plan".into();
    let mut latest = step(StepStatus::WaitingApproval, 2);
    latest.step_id = "plan".into();

    let run = run_with_steps(
        "run_1",
        vec![
            StepRecord {
                run: old,
                job_id: Some("job_old".into()),
                with_resolved: None,
                failure_detail: None,
            },
            StepRecord {
                run: latest,
                job_id: None,
                with_resolved: None,
                failure_detail: None,
            },
        ],
    );
    let actions = recovery_plan(vec![run]);

    assert_eq!(
        actions,
        vec![RecoveryAction::LeaveWaitingApproval {
            run_id: "run_1".into(),
            step_id: "plan".into(),
            attempt: 2,
        }]
    );
}
