use crate::ports::RunId;
use crate::scheduler::Scheduler;
use flowspec_domain::run::types::Event;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct AdvanceRunRequest {
    pub event: Event,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdvanceRunResponse {
    pub run_id: RunId,
}

pub async fn advance_run(
    scheduler: Arc<Scheduler>,
    run_id: RunId,
    req: AdvanceRunRequest,
) -> AdvanceRunResponse {
    scheduler.submit(run_id.clone(), req.event).await;
    AdvanceRunResponse { run_id }
}
