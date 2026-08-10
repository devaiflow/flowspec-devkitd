use crate::ports::{RunId, StateStore, StoreError};
use crate::scheduler::Scheduler;
use crate::wire::Timestamp;
use flowspec_domain::run::types::Event;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(feature = "schema")]
use schemars::JsonSchema;

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct CancelRunRequest {
    /// Id of the run to cancel, as returned by start_flow or list_runs.
    pub run_id: RunId,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct CancelRunResponse {
    pub run_id: RunId,
    pub phase: String,
    pub active_steps: Vec<String>,
    pub updated_at: Timestamp,
}

pub async fn cancel_run(
    store: Arc<dyn StateStore>,
    scheduler: Arc<Scheduler>,
    req: CancelRunRequest,
) -> Result<CancelRunResponse, StoreError> {
    let run_id = req.run_id;
    scheduler
        .submit(run_id.clone(), Event::CancelRequested)
        .await;
    let record = store.load_run(&run_id).await?;
    Ok(CancelRunResponse {
        run_id: run_id.clone(),
        phase: serde_json::to_value(record.phase)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        active_steps: crate::scheduler::run_active_steps(&record.run_state()),
        updated_at: record.updated_at.into(),
    })
}
