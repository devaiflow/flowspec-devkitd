use super::types::{
    ApprovalStatus, Command, EngineError, Event, HookPhase, RunPhase, RunState, StepActivation,
    StepRun, StepStatus, Timestamp,
};
use crate::flow::dag;
use crate::flow::types::{DONE, FlowDefinition, HookCondition, RouteTarget, Step, StepWith};
use crate::template::TemplateContext;

/// `decide(flow, state, event, now) -> commands` is the engine's single entry
/// point. Pure: no IO, no clock reads beyond the `now` argument.
pub fn decide(
    flow: &FlowDefinition,
    state: &RunState,
    event: Event,
    now: Timestamp,
) -> Result<Vec<Command>, EngineError> {
    let _ = now;
    match event {
        Event::RunStarted => on_run_started(flow, state),
        Event::StepCompleted { step_id, output } => {
            on_step_completed(flow, state, &step_id, output)
        }
        Event::StepFailed { step_id, reason } => on_step_failed(flow, state, &step_id, reason),
        Event::StepApproved { step_id, comment } => {
            on_step_approved(flow, state, &step_id, comment)
        }
        Event::StepRejected { step_id, feedback } => {
            on_step_rejected(flow, state, &step_id, feedback)
        }
        Event::SubflowFinished {
            step_id,
            phase,
            outputs,
        } => on_subflow_finished(flow, state, &step_id, phase, outputs),
        Event::RunDeadlineExceeded => on_deadline_exceeded(state),
        Event::CancelRequested => on_cancel_requested(state),
    }
}

fn build_context(flow: &FlowDefinition, state: &RunState) -> TemplateContext {
    crate::template::build_context(flow, state, serde_json::Value::Null)
}

fn resolve_input(step: &Step, ctx: &TemplateContext) -> Result<String, EngineError> {
    let template = match step.kind() {
        Some(StepWith::Cli { with }) => with.input,
        _ => String::new(),
    };
    crate::template::resolve(&template, ctx, &step.id)
        .map_err(|_| EngineError::UnknownStep(step.id.clone()))
}

fn resolve_reject_input(
    step: &Step,
    ctx: &TemplateContext,
    feedback: &str,
) -> Result<String, EngineError> {
    if let Some(template) = &step.reject_input {
        return crate::template::resolve(template, ctx, &step.id)
            .map_err(|_| EngineError::UnknownStep(step.id.clone()));
    }
    let original = resolve_input(step, ctx)?;
    Ok(format!("{original}\n\nFeedback: {feedback}"))
}

fn needs_satisfied(flow: &FlowDefinition, state: &RunState, step: &Step) -> bool {
    if step.needs.is_empty() {
        return true;
    }
    step.needs.iter().all(|needed| {
        flow.step(needed).is_some()
            && state
                .step(needed)
                .map(|s| s.status == StepStatus::Completed)
                .unwrap_or(false)
    })
}

/// Builds activation commands for a set of routing targets, respecting `needs:`
/// gates. CLI steps become `ActivateSteps`; subflow steps become `StartChildRun`.
/// Steps whose `needs:` are not yet satisfied are skipped (they'll activate later
/// when their last predecessor completes and re-checks them).
fn activate_targets(
    flow: &FlowDefinition,
    state: &RunState,
    ctx: &TemplateContext,
    targets: &[&str],
) -> Result<Vec<Command>, EngineError> {
    let mut commands = Vec::new();
    for &target in targets {
        if target == DONE {
            continue;
        }
        let Some(step) = flow.step(target) else {
            return Err(EngineError::UnknownStep(target.to_string()));
        };
        if !needs_satisfied(flow, state, step) {
            continue;
        }
        match step.kind() {
            Some(StepWith::Subflow { .. }) => {
                commands.push(Command::StartChildRun {
                    step_id: step.id.clone(),
                });
            }
            _ => {
                let attempt = state.step(target).map(|s| s.attempt + 1).unwrap_or(1);
                let input = resolve_input(step, ctx)?;
                commands.push(Command::ActivateSteps(vec![StepActivation {
                    step_id: step.id.clone(),
                    attempt,
                    input,
                }]));
            }
        }
    }
    Ok(commands)
}

fn route_targets(route: &Option<RouteTarget>) -> Vec<&str> {
    route.as_ref().map(|r| r.as_slice()).unwrap_or_default()
}

fn on_run_started(flow: &FlowDefinition, state: &RunState) -> Result<Vec<Command>, EngineError> {
    let entries = dag::entry_steps(flow);
    let ctx = build_context(flow, state);
    let mut commands = Vec::new();

    if !flow.lifecycle.before_run.is_empty() {
        commands.push(Command::RunHooks {
            phase: HookPhase::BeforeRun,
            hooks: flow.lifecycle.before_run.clone(),
        });
    }

    commands.extend(activate_targets(flow, state, &ctx, &entries)?);
    Ok(commands)
}

fn active_step_ids_excluding(state: &RunState, excluding: &str) -> Vec<String> {
    state
        .steps
        .iter()
        .filter(|s| s.step_id != excluding && s.status.is_active())
        .map(|s| s.step_id.clone())
        .collect()
}

fn all_activated_terminal(state: &RunState) -> bool {
    state.steps.iter().all(|s| s.status.is_terminal())
}

fn terminal_commands_if_done(flow: &FlowDefinition, state: &RunState) -> Vec<Command> {
    let mut commands = Vec::new();
    if !all_activated_terminal(state) {
        return commands;
    }

    let skip: Vec<String> = flow
        .steps
        .iter()
        .filter(|s| state.step(&s.id).is_none())
        .map(|s| s.id.clone())
        .collect();
    for step_id in skip {
        commands.push(Command::MarkStepStatus {
            step_id,
            status: StepStatus::Skipped,
        });
    }

    let phase = super::derive::phase(state);
    if phase != RunPhase::Running {
        let due: Vec<_> = flow
            .lifecycle
            .after_run
            .iter()
            .filter(|h| hook_condition_met(h.when.unwrap_or_default(), phase))
            .cloned()
            .collect();
        if !due.is_empty() {
            commands.push(Command::RunHooks {
                phase: HookPhase::AfterRun,
                hooks: due,
            });
        }
        commands.push(Command::MarkRunTerminal(phase));
    }
    commands
}

fn hook_condition_met(when: HookCondition, phase: RunPhase) -> bool {
    match when {
        HookCondition::Always => true,
        HookCondition::Succeeded => phase == RunPhase::Completed,
        HookCondition::Failed => phase == RunPhase::Failed,
        HookCondition::Cancelled => phase == RunPhase::Cancelled,
    }
}

fn on_step_completed(
    flow: &FlowDefinition,
    state: &RunState,
    step_id: &str,
    output: serde_json::Value,
) -> Result<Vec<Command>, EngineError> {
    let step = flow
        .step(step_id)
        .ok_or_else(|| EngineError::UnknownStep(step_id.to_string()))?;
    let mut commands = Vec::new();

    if !step.after.is_empty() {
        commands.push(Command::RunHooks {
            phase: HookPhase::AfterStep {
                step_id: step_id.to_string(),
            },
            hooks: step.after.clone(),
        });
    }

    if step.human_loop {
        commands.push(Command::MarkStepStatus {
            step_id: step_id.to_string(),
            status: StepStatus::WaitingApproval,
        });
        commands.push(Command::WaitApproval {
            step_id: step_id.to_string(),
        });
        return Ok(commands);
    }

    commands.push(Command::MarkStepStatus {
        step_id: step_id.to_string(),
        status: StepStatus::Completed,
    });

    let mut next_state = state.clone();
    upsert_step(
        &mut next_state,
        step_id,
        StepStatus::Completed,
        Some(output),
    );

    let ctx = build_context(flow, &next_state);
    let targets = route_targets(&step.on_success);
    let activations = activate_targets(flow, &next_state, &ctx, &targets)?;
    if activations.is_empty() {
        commands.extend(terminal_commands_if_done(flow, &next_state));
    } else {
        commands.extend(activations);
    }
    Ok(commands)
}

fn upsert_step(
    state: &mut RunState,
    step_id: &str,
    status: StepStatus,
    output: Option<serde_json::Value>,
) {
    if let Some(existing) = state.step_mut(step_id) {
        existing.status = status;
        if output.is_some() {
            existing.output = output;
        }
    } else {
        let mut run = StepRun::pending(step_id);
        run.status = status;
        run.output = output;
        run.attempt = 1;
        state.steps.push(run);
    }
}

fn on_step_failed(
    flow: &FlowDefinition,
    state: &RunState,
    step_id: &str,
    reason: String,
) -> Result<Vec<Command>, EngineError> {
    let step = flow
        .step(step_id)
        .ok_or_else(|| EngineError::UnknownStep(step_id.to_string()))?;
    let attempt = state.step(step_id).map(|s| s.attempt).unwrap_or(1);
    let mut commands = Vec::new();

    if attempt <= step.retries {
        let ctx = build_context(flow, state);
        let input = resolve_input(step, &ctx)?;
        match step.kind() {
            Some(StepWith::Subflow { .. }) => {
                commands.push(Command::StartChildRun {
                    step_id: step_id.to_string(),
                });
            }
            _ => {
                commands.push(Command::ActivateSteps(vec![StepActivation {
                    step_id: step_id.to_string(),
                    attempt: attempt + 1,
                    input,
                }]));
            }
        }
        return Ok(commands);
    }

    commands.push(Command::MarkStepStatus {
        step_id: step_id.to_string(),
        status: StepStatus::Failed,
    });

    // Fail-fast: v0.3 cancels every other currently-active step when one branch fails.
    let siblings = active_step_ids_excluding(state, step_id);
    if !siblings.is_empty() {
        commands.push(Command::CancelSteps(siblings));
    }

    let mut next_state = state.clone();
    upsert_step(&mut next_state, step_id, StepStatus::Failed, None);
    next_state.steps.iter_mut().for_each(|s| {
        if s.status.is_active() {
            s.status = StepStatus::Cancelled;
        }
    });

    let failure_reason = reason.to_string();
    if let Some(existing) = next_state.step_mut(step_id) {
        existing.failure_reason = Some(failure_reason);
    }

    let ctx = build_context(flow, &next_state);
    let targets = route_targets(&step.on_failure);
    let activations = activate_targets(flow, &next_state, &ctx, &targets)?;
    if activations.is_empty() {
        commands.extend(terminal_commands_if_done(flow, &next_state));
    } else {
        commands.extend(activations);
    }
    Ok(commands)
}

fn on_step_approved(
    flow: &FlowDefinition,
    state: &RunState,
    step_id: &str,
    comment: Option<String>,
) -> Result<Vec<Command>, EngineError> {
    let step = flow
        .step(step_id)
        .ok_or_else(|| EngineError::UnknownStep(step_id.to_string()))?;
    let mut commands = vec![Command::MarkStepStatus {
        step_id: step_id.to_string(),
        status: StepStatus::Completed,
    }];

    let mut next_state = state.clone();
    upsert_step(&mut next_state, step_id, StepStatus::Completed, None);
    if let Some(existing) = next_state.step_mut(step_id) {
        existing.approval_status = Some(ApprovalStatus::Approved);
        existing.approval_comment = comment;
    }

    let ctx = build_context(flow, &next_state);
    let targets = route_targets(&step.on_approve);
    let activations = activate_targets(flow, &next_state, &ctx, &targets)?;
    if activations.is_empty() {
        commands.extend(terminal_commands_if_done(flow, &next_state));
    } else {
        commands.extend(activations);
    }
    Ok(commands)
}

fn on_step_rejected(
    flow: &FlowDefinition,
    state: &RunState,
    step_id: &str,
    feedback: String,
) -> Result<Vec<Command>, EngineError> {
    let step = flow
        .step(step_id)
        .ok_or_else(|| EngineError::UnknownStep(step_id.to_string()))?;
    let mut commands = vec![Command::MarkStepStatus {
        step_id: step_id.to_string(),
        status: StepStatus::FailedRejected,
    }];

    let mut next_state = state.clone();
    upsert_step(&mut next_state, step_id, StepStatus::FailedRejected, None);
    if let Some(existing) = next_state.step_mut(step_id) {
        existing.approval_status = Some(ApprovalStatus::Rejected);
        existing.feedback = Some(feedback.clone());
    }

    let ctx = build_context(flow, &next_state);
    let targets = route_targets(&step.on_reject);

    if dag::is_self_reject_loop(step) {
        let attempt = state.step(step_id).map(|s| s.attempt + 1).unwrap_or(1);
        let input = resolve_reject_input(step, &ctx, &feedback)?;
        match step.kind() {
            Some(StepWith::Subflow { .. }) => {
                commands.push(Command::StartChildRun {
                    step_id: step_id.to_string(),
                });
            }
            _ => {
                commands.push(Command::ActivateSteps(vec![StepActivation {
                    step_id: step_id.to_string(),
                    attempt,
                    input,
                }]));
            }
        }
        // The step is reactivating, not terminal -- reflect that before the
        // terminal check below, or a self-loop reject would look like the run
        // is done (the same reason `on_step_failed`'s retry path returns early).
        if let Some(existing) = next_state.step_mut(step_id) {
            existing.status = StepStatus::Running;
        }
    } else {
        let activations = activate_targets(flow, &next_state, &ctx, &targets)?;
        if activations.is_empty() {
            commands.extend(terminal_commands_if_done(flow, &next_state));
        } else {
            commands.extend(activations);
        }
    }

    Ok(commands)
}

fn on_subflow_finished(
    flow: &FlowDefinition,
    state: &RunState,
    step_id: &str,
    phase: RunPhase,
    outputs: Option<serde_json::Value>,
) -> Result<Vec<Command>, EngineError> {
    match phase {
        RunPhase::Completed => on_step_completed(
            flow,
            state,
            step_id,
            outputs.unwrap_or(serde_json::Value::Null),
        ),
        _ => on_step_failed(
            flow,
            state,
            step_id,
            format!("subflow ended in phase {phase:?}"),
        ),
    }
}

fn on_deadline_exceeded(state: &RunState) -> Result<Vec<Command>, EngineError> {
    let active = active_step_ids_excluding(state, "");
    let mut commands = Vec::new();
    if !active.is_empty() {
        commands.push(Command::CancelSteps(active));
    }
    commands.push(Command::MarkRunTerminal(RunPhase::Failed));
    Ok(commands)
}

fn on_cancel_requested(state: &RunState) -> Result<Vec<Command>, EngineError> {
    let active = active_step_ids_excluding(state, "");
    let mut commands = Vec::new();
    if !active.is_empty() {
        commands.push(Command::CancelSteps(active));
    }
    commands.push(Command::MarkRunTerminal(RunPhase::Cancelled));
    Ok(commands)
}
