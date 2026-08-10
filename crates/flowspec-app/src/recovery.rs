use crate::ports::RunRecord;
use flowspec_domain::run::types::StepStatus;

/// A pure decision about what to do with a run that was in-flight when the
/// runtime stopped. The scheduler (Phase 3) turns these into ordinary engine
/// events; no IO happens here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    ReattachStep {
        run_id: String,
        step_id: String,
        attempt: u32,
        job_id: String,
    },
    FailStep {
        run_id: String,
        step_id: String,
        attempt: u32,
        reason: RecoveryReason,
    },
    LeaveWaitingApproval {
        run_id: String,
        step_id: String,
        attempt: u32,
    },
    RecheckSubflow {
        run_id: String,
        step_id: String,
        attempt: u32,
        child_run_id: String,
    },
    /// A run stuck in `Running` with no step rows at all — meaning it was
    /// interrupted *during a gating hook* (e.g. `before_run`) before the first
    /// step ever activated. Recovery re-issues `RunStarted` so the engine
    /// re-emits the gating hooks and entry activations. Gating hooks may
    /// re-execute on recovery; that's a recovery-driven re-run, not a silent
    /// hang. Without this action the run would be stuck `Running` forever,
    /// since nothing else produces an event for it.
    ReissueRunStarted { run_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryReason {
    Interrupted,
}

/// Decide what recovery action each active step in a running run needs.
/// Only the latest attempt of each step is considered -- older attempts are
/// historical audit rows.
pub fn recovery_plan(runs: Vec<RunRecord>) -> Vec<RecoveryAction> {
    let mut actions = Vec::new();
    for run in runs {
        let mut produced_for_this_run = false;
        for step in run.latest_steps() {
            let status = step.run.status;
            if !status.is_active() {
                continue;
            }
            match status {
                StepStatus::Running => {
                    if let Some(job_id) = &step.job_id {
                        actions.push(RecoveryAction::ReattachStep {
                            run_id: run.run_id.clone(),
                            step_id: step.run.step_id.clone(),
                            attempt: step.run.attempt,
                            job_id: job_id.clone(),
                        });
                    } else {
                        actions.push(RecoveryAction::FailStep {
                            run_id: run.run_id.clone(),
                            step_id: step.run.step_id.clone(),
                            attempt: step.run.attempt,
                            reason: RecoveryReason::Interrupted,
                        });
                    }
                    produced_for_this_run = true;
                }
                StepStatus::WaitingApproval => {
                    actions.push(RecoveryAction::LeaveWaitingApproval {
                        run_id: run.run_id.clone(),
                        step_id: step.run.step_id.clone(),
                        attempt: step.run.attempt,
                    });
                    produced_for_this_run = true;
                }
                StepStatus::WaitingOnSubflow => {
                    if let Some(child_run_id) = &step.run.child_run_id {
                        actions.push(RecoveryAction::RecheckSubflow {
                            run_id: run.run_id.clone(),
                            step_id: step.run.step_id.clone(),
                            attempt: step.run.attempt,
                            child_run_id: child_run_id.clone(),
                        });
                    } else {
                        actions.push(RecoveryAction::FailStep {
                            run_id: run.run_id.clone(),
                            step_id: step.run.step_id.clone(),
                            attempt: step.run.attempt,
                            reason: RecoveryReason::Interrupted,
                        });
                    }
                    produced_for_this_run = true;
                }
                _ => {}
            }
        }

        // A `Running` run that produced no per-step action is one interrupted
        // *before any step row existed* — the only path that fits is a gating
        // hook (`before_run`) mid-flight when the runtime stopped. Its hook
        // task isn't recoverable (we don't persist in-flight hook handles at
        // this stage), so re-issue `RunStarted` and let the run make forward
        // progress from scratch. Gating hooks may re-execute; that is the
        // explicit recovery contract for them in this phase. The alternative —
        // leaving the run `Running` with no event source — is a permanent hang.
        if !produced_for_this_run && run.steps.is_empty() {
            actions.push(RecoveryAction::ReissueRunStarted {
                run_id: run.run_id.clone(),
            });
        }
    }
    actions
}
