//! The action-queue poll loop: `GET /api/agent/v1/actions`, drive each one
//! through the same use cases the MCP surface uses, and always either ack,
//! delete, or -- for approve/reject that arrived before the step is
//! actually waiting -- deliberately leave it pending for the next tick
//! (matching `devaiflow-platform/scripts/mock-runtime.mjs`, the contract's
//! reference implementation). Never DELETE an action already acked.

use super::client::{Action, PlatformClient};
use flowspec_app::ports::StateStore;
use flowspec_app::scheduler::Scheduler;
use flowspec_app::use_cases::approvals::{self, ApprovalError, ApproveRequest, RejectRequest};
use flowspec_app::use_cases::cancel_run::{self, CancelRunRequest};
use flowspec_app::use_cases::start_flow::{
    StartFlowError, StartFlowRequestBase, start_flow_with_definition,
};
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use flowspec_domain::flow::validate;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Prefix flowspec puts on `idempotency_key` for platform-originated runs.
/// Re-exported from `flowspec-app` so poller and pump agree on one constant.
pub use flowspec_app::use_cases::queries::PLATFORM_IDEMPOTENCY_PREFIX;

#[derive(Debug, Deserialize)]
struct TriggerRunPayload {
    run_id: String,
    flow_doc: Value,
    #[serde(default)]
    inputs: Value,
}

#[derive(Debug, Deserialize, Default)]
struct ApproveRejectPayload {
    step_id: Option<String>,
    comment: Option<String>,
    feedback: Option<String>,
}

pub struct PollerConfig {
    pub poll_interval: Duration,
}

/// Runs until `shutdown` is cancelled. Transport failures are logged and
/// retried next tick -- the platform being down must never stall run
/// execution.
pub async fn run(
    client: Arc<PlatformClient>,
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    config: PollerConfig,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(config.poll_interval) => {}
        }

        let actions = match client.get_actions().await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!("platform poller: failed to fetch actions: {e}");
                continue;
            }
        };

        for action in actions {
            if let Err(e) = handle_action(&client, &store, &scheduler, &action).await {
                tracing::warn!(action_id = %action.id, "platform poller: action handling failed: {e}");
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum PollerError {
    #[error("platform transport error: {0}")]
    Platform(#[from] super::client::PlatformError),
}

async fn handle_action(
    client: &PlatformClient,
    store: &Arc<dyn StateStore>,
    scheduler: &Arc<Scheduler>,
    action: &Action,
) -> Result<(), PollerError> {
    match action.kind.as_str() {
        "trigger_run" => handle_trigger_run(client, store, scheduler, action).await,
        "approve" => handle_approve_reject(client, store, scheduler, action, true).await,
        "reject" => handle_approve_reject(client, store, scheduler, action, false).await,
        "cancel" => handle_cancel(client, store, scheduler, action).await,
        other => {
            tracing::warn!(action_id = %action.id, kind = %other, "unknown action kind, deleting");
            client.delete_action(&action.id).await?;
            Ok(())
        }
    }
}

async fn handle_trigger_run(
    client: &PlatformClient,
    store: &Arc<dyn StateStore>,
    scheduler: &Arc<Scheduler>,
    action: &Action,
) -> Result<(), PollerError> {
    let payload: TriggerRunPayload = match serde_json::from_value(action.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(action_id = %action.id, "malformed trigger_run payload: {e}");
            client.delete_action(&action.id).await?;
            return Ok(());
        }
    };

    let idempotency_key = format!("{PLATFORM_IDEMPOTENCY_PREFIX}{}", payload.run_id);

    // Redelivered trigger_run (we acked, but the ack never landed, or this
    // is a duplicate delivery): resolve to the same run instead of starting
    // a second one, then just re-ack it.
    if let Ok(Some(existing_run_id)) = store.find_by_idempotency_key(&idempotency_key).await {
        client.ack_action(&action.id, Some(existing_run_id)).await?;
        return Ok(());
    }

    let definition = match parse_and_validate_flow_doc(&payload.flow_doc) {
        Ok(d) => d,
        Err(reason) => {
            tracing::warn!(action_id = %action.id, "invalid flow_doc: {reason}");
            client.delete_action(&action.id).await?;
            return Ok(());
        }
    };

    let result = start_flow_with_definition(
        store.clone(),
        scheduler.clone(),
        definition,
        StartFlowRequestBase {
            inputs: payload.inputs,
            trigger: serde_json::json!({ "source": "platform", "platform_run_id": payload.run_id }),
            idempotency_key: Some(idempotency_key),
        },
    )
    .await;

    match result {
        Ok(resp) => {
            client
                .ack_action(&action.id, Some(resp.run_id.clone()))
                .await?;
        }
        Err(StartFlowError::MissingInputs(missing)) => {
            tracing::warn!(action_id = %action.id, "trigger_run missing inputs: {missing}");
            client.delete_action(&action.id).await?;
        }
        Err(e) => {
            tracing::error!(action_id = %action.id, "trigger_run failed to start: {e}");
            client.delete_action(&action.id).await?;
        }
    }
    Ok(())
}

/// Parses `flow_doc` (either `{ flow: {...} }` or `{ flows: [...] }`,
/// per `schemas.ts`) into a `FlowDefinition` and runs the same validation
/// gate `FsFlowSource::load` uses. `flow.metadata` (incl. the platform's
/// `metadata.ui`) needs no special handling -- `FlowDefinition::metadata` is
/// a free-form map with no `deny_unknown_fields`.
fn parse_and_validate_flow_doc(flow_doc: &Value) -> Result<FlowDefinition, String> {
    let file: FlowFile =
        serde_json::from_value(flow_doc.clone()).map_err(|e| format!("parse error: {e}"))?;
    let definition = file
        .into_definitions()
        .into_iter()
        .next()
        .ok_or_else(|| "flow_doc contains no flow definitions".to_string())?;
    let violations = validate::validate(&definition);
    if !violations.is_empty() {
        let detail = violations
            .iter()
            .map(|v| format!("[{}] {}", v.rule, v.message))
            .collect::<Vec<_>>()
            .join("; ");
        return Err(detail);
    }
    Ok(definition)
}

async fn handle_approve_reject(
    client: &PlatformClient,
    store: &Arc<dyn StateStore>,
    scheduler: &Arc<Scheduler>,
    action: &Action,
    is_approve: bool,
) -> Result<(), PollerError> {
    let platform_run_id = &action.run_id;
    let idempotency_key = format!("{PLATFORM_IDEMPOTENCY_PREFIX}{platform_run_id}");

    let run_id = match store.find_by_idempotency_key(&idempotency_key).await {
        Ok(Some(id)) => id,
        _ => {
            // We have no record of this run at all -- nothing will ever make
            // this actionable.
            tracing::warn!(action_id = %action.id, %platform_run_id, "approve/reject for unknown run, deleting");
            client.delete_action(&action.id).await?;
            return Ok(());
        }
    };

    let payload: ApproveRejectPayload =
        serde_json::from_value(action.payload.clone()).unwrap_or_default();

    let outcome = if is_approve {
        approvals::approve_step(
            store.clone(),
            scheduler.clone(),
            ApproveRequest {
                run_id: run_id.clone(),
                step_id: payload.step_id.clone(),
                comment: payload.comment.clone(),
            },
        )
        .await
        .map(|_| ())
    } else {
        approvals::reject_step(
            store.clone(),
            scheduler.clone(),
            RejectRequest {
                run_id: run_id.clone(),
                step_id: payload.step_id.clone(),
                feedback: payload.feedback.clone().unwrap_or_default(),
            },
        )
        .await
        .map(|_| ())
    };

    match outcome {
        Ok(()) => {
            client.ack_action(&action.id, None).await?;
        }
        // The step isn't waiting yet (race: the platform enqueued the
        // action before the run reached that point) -- leave it pending,
        // exactly like the reference mock-runtime.mjs does, and pick it up
        // on a later tick.
        Err(ApprovalError::NoWaitingStep) | Err(ApprovalError::NotWaiting(_)) => {
            tracing::debug!(action_id = %action.id, "approve/reject not yet actionable, leaving pending");
        }
        // Ambiguous target (more than one step waiting and no step_id, or a
        // step_id that doesn't resolve) has no way to become actionable on
        // its own -- delete rather than redeliver forever.
        Err(ApprovalError::AmbiguousStep) => {
            tracing::warn!(action_id = %action.id, "ambiguous approval target, deleting");
            client.delete_action(&action.id).await?;
        }
        Err(ApprovalError::Store(e)) => {
            tracing::error!(action_id = %action.id, "store error resolving approval: {e}");
        }
    }
    Ok(())
}

async fn handle_cancel(
    client: &PlatformClient,
    store: &Arc<dyn StateStore>,
    scheduler: &Arc<Scheduler>,
    action: &Action,
) -> Result<(), PollerError> {
    let idempotency_key = format!("{PLATFORM_IDEMPOTENCY_PREFIX}{}", action.run_id);
    let run_id = match store.find_by_idempotency_key(&idempotency_key).await {
        Ok(Some(id)) => id,
        _ => {
            tracing::warn!(action_id = %action.id, "cancel for unknown run, acking anyway");
            client.ack_action(&action.id, None).await?;
            return Ok(());
        }
    };
    let _ = cancel_run::cancel_run(
        store.clone(),
        scheduler.clone(),
        CancelRunRequest { run_id },
    )
    .await;
    client.ack_action(&action.id, None).await?;
    Ok(())
}
