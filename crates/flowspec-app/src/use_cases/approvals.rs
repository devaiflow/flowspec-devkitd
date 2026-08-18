use crate::ports::{Mutation, RunEventRecord, RunEventType, RunId, StateStore, StoreError};
use crate::scheduler::Scheduler;
use flowspec_domain::run::types::{Event, StepStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

#[cfg(feature = "schema")]
use schemars::JsonSchema;

#[derive(Debug, thiserror::Error)]
pub enum ApprovalError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("no step is waiting approval")]
    NoWaitingStep,
    #[error("multiple steps waiting approval; step_id is required")]
    AmbiguousStep,
    #[error("step '{0}' is not waiting approval")]
    NotWaiting(String),
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct ApproveRequest {
    /// Id of the run whose step should be approved.
    pub run_id: RunId,
    /// Which step to approve. Optional when exactly one step on the run is
    /// waiting_approval; required (and validated against the actual waiting
    /// step) when more than one is waiting. Find candidates with
    /// pending_approvals.
    pub step_id: Option<String>,
    /// Optional note recorded alongside the approval.
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct RejectRequest {
    /// Id of the run whose step should be rejected.
    pub run_id: RunId,
    /// Which step to reject. Optional when exactly one step on the run is
    /// waiting_approval; required when more than one is waiting.
    pub step_id: Option<String>,
    /// Actionable feedback explaining why the step was rejected. This text
    /// is handed back to the agent verbatim, so be specific about what to
    /// change.
    pub feedback: String,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct ApprovalResponse {
    pub run_id: RunId,
    pub step_id: String,
}

pub async fn approve_step(
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    req: ApproveRequest,
) -> Result<ApprovalResponse, ApprovalError> {
    let run_id = req.run_id;
    let step_id = resolve_step_id(store.clone(), &run_id, req.step_id).await?;
    let _ = store
        .apply(
            &run_id,
            vec![Mutation::AppendEvent(RunEventRecord {
                event_type: RunEventType::ApprovalResolved,
                step_id: Some(step_id.clone()),
                timestamp: SystemTime::now(),
                payload: serde_json::json!({ "decision": "approved", "comment": req.comment.clone() }),
            })],
        )
        .await;
    scheduler
        .submit(
            run_id.clone(),
            Event::StepApproved {
                step_id: step_id.clone(),
                comment: req.comment,
            },
        )
        .await;
    Ok(ApprovalResponse { run_id, step_id })
}

pub async fn reject_step(
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    req: RejectRequest,
) -> Result<ApprovalResponse, ApprovalError> {
    let run_id = req.run_id;
    let step_id = resolve_step_id(store.clone(), &run_id, req.step_id).await?;
    let _ = store
        .apply(
            &run_id,
            vec![Mutation::AppendEvent(RunEventRecord {
                event_type: RunEventType::ApprovalResolved,
                step_id: Some(step_id.clone()),
                timestamp: SystemTime::now(),
                payload: serde_json::json!({ "decision": "rejected", "feedback": req.feedback.clone() }),
            })],
        )
        .await;
    scheduler
        .submit(
            run_id.clone(),
            Event::StepRejected {
                step_id: step_id.clone(),
                feedback: req.feedback,
            },
        )
        .await;
    Ok(ApprovalResponse { run_id, step_id })
}

async fn resolve_step_id(
    store: Arc<dyn StateStore>,
    run_id: &RunId,
    explicit: Option<String>,
) -> Result<String, ApprovalError> {
    let record = store.load_run(run_id).await?;
    let waiting: Vec<String> = record
        .latest_steps()
        .into_iter()
        .filter(|s| s.run.status == StepStatus::WaitingApproval)
        .map(|s| s.run.step_id.clone())
        .collect();

    if let Some(id) = explicit {
        if waiting.contains(&id) {
            Ok(id)
        } else {
            Err(ApprovalError::NotWaiting(id))
        }
    } else if waiting.is_empty() {
        Err(ApprovalError::NoWaitingStep)
    } else if waiting.len() > 1 {
        Err(ApprovalError::AmbiguousStep)
    } else {
        Ok(waiting.into_iter().next().unwrap())
    }
}
