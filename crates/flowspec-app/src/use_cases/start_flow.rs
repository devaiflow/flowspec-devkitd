use crate::ports::{FlowSource, NewRun, RunId, SchedulerConfig, StateStore, StoreError};
use crate::scheduler::Scheduler;
use crate::wire::Timestamp;
use flowspec_domain::run::types::Event;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "schema")]
use schemars::JsonSchema;

#[derive(Debug, thiserror::Error)]
pub enum StartFlowError {
    #[error("flow not found: {0}")]
    FlowNotFound(String),
    #[error("missing required input(s): {0}")]
    MissingInputs(String),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct StartFlowRequest {
    /// Name of the flow to start, as returned by list_flows.
    pub flow_name: String,
    /// Optional semver requirement (e.g. ">=1.2.0"). Defaults to the highest
    /// available version.
    pub version_req: Option<String>,
    /// Values for the flow's declared inputs. Required inputs (see
    /// list_flows) must be present and non-null.
    pub inputs: Value,
    /// Free-form context about what triggered this run (e.g. who asked, or
    /// which chat message). Stored with the run, not interpreted.
    pub trigger: Value,
    /// A key unique to this logical request. If a run with this key already
    /// exists, start_flow returns that run instead of creating a new one.
    /// Hosts that may retry or double-fire tool calls should always send
    /// one.
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct StartFlowResponse {
    pub run_id: RunId,
    pub phase: String,
    pub active_steps: Vec<String>,
    pub started_at: Timestamp,
    /// True if idempotency_key matched an existing run and no new run was
    /// created.
    pub replayed: bool,
}

pub async fn start_flow(
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    flows: Arc<dyn FlowSource>,
    _config: &SchedulerConfig,
    req: StartFlowRequest,
) -> Result<StartFlowResponse, StartFlowError> {
    let version_req = req.version_req.as_deref();
    let definition = flows
        .get(&req.flow_name, version_req)
        .await
        .ok_or_else(|| StartFlowError::FlowNotFound(req.flow_name.clone()))?;

    let missing: Vec<String> = definition
        .inputs
        .iter()
        .filter(|(_, spec)| spec.required)
        .filter(|(name, _)| req.inputs.get(name).map(|v| v.is_null()).unwrap_or(true))
        .map(|(name, _)| name.clone())
        .collect();
    if !missing.is_empty() {
        return Err(StartFlowError::MissingInputs(missing.join(", ")));
    }

    if let Some(run_id) = match &req.idempotency_key {
        Some(key) => store.find_by_idempotency_key(key).await?,
        None => None,
    } {
        return replayed_response(store, run_id).await;
    }

    let new_run = NewRun {
        flow_name: definition.name.clone(),
        flow_version: definition.version.clone(),
        definition: definition.clone(),
        inputs: req.inputs,
        trigger: req.trigger,
        idempotency_key: req.idempotency_key.clone(),
    };

    let run_id = match store.create_run(new_run).await {
        Ok(run_id) => run_id,
        // Lost a race against a concurrent start_flow with the same key:
        // the other caller's run now owns the key, so replay it instead of
        // erroring.
        Err(StoreError::Duplicate(_)) if req.idempotency_key.is_some() => {
            let key = req.idempotency_key.as_deref().unwrap();
            let run_id = store
                .find_by_idempotency_key(key)
                .await?
                .ok_or(StoreError::Duplicate(key.to_string()))?;
            return replayed_response(store, run_id).await;
        }
        Err(e) => return Err(e.into()),
    };
    scheduler.start_deadline_for_run(&run_id).await;
    scheduler.submit(run_id.clone(), Event::RunStarted).await;

    let record = store.load_run(&run_id).await?;
    Ok(StartFlowResponse {
        run_id: run_id.clone(),
        phase: serde_json::to_value(record.phase)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        active_steps: crate::scheduler::run_active_steps(&record.run_state()),
        started_at: record.created_at.into(),
        replayed: false,
    })
}

async fn replayed_response(
    store: Arc<dyn StateStore>,
    run_id: RunId,
) -> Result<StartFlowResponse, StartFlowError> {
    let record = store.load_run(&run_id).await?;
    Ok(StartFlowResponse {
        run_id: run_id.clone(),
        phase: serde_json::to_value(record.phase)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        active_steps: crate::scheduler::run_active_steps(&record.run_state()),
        started_at: record.created_at.into(),
        replayed: true,
    })
}
