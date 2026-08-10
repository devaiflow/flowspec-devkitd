use super::types::{RunPhase, RunState, StepStatus};

/// The run's overall phase, derived from step statuses. A run is `running` as
/// long as at least one step is not yet in a terminal state; otherwise it is
/// terminal, and `failed` wins over `completed` if any step ended badly.
pub fn phase(state: &RunState) -> RunPhase {
    if state.cancelled
        && state.steps.iter().all(|s| s.status.is_terminal())
        && state
            .steps
            .iter()
            .any(|s| s.status == StepStatus::Cancelled)
    {
        return RunPhase::Cancelled;
    }

    let all_terminal = state.steps.iter().all(|s| s.status.is_terminal());
    if !all_terminal {
        return RunPhase::Running;
    }

    if state
        .steps
        .iter()
        .any(|s| matches!(s.status, StepStatus::Failed | StepStatus::FailedRejected))
    {
        return RunPhase::Failed;
    }

    if state
        .steps
        .iter()
        .any(|s| s.status == StepStatus::Cancelled)
    {
        return RunPhase::Cancelled;
    }

    RunPhase::Completed
}

/// Step ids currently non-terminal and non-pending: `running`,
/// `waiting_approval`, or `waiting_on_subflow`.
pub fn active_steps(state: &RunState) -> Vec<String> {
    state
        .steps
        .iter()
        .filter(|s| s.status.is_active())
        .map(|s| s.step_id.clone())
        .collect()
}
