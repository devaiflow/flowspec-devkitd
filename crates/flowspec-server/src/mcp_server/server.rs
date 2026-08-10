use crate::container::Container;
use crate::mcp_server::error::ToolFailure;
use flowspec_app::ports::RunId;
use flowspec_app::use_cases::approvals::{self, ApprovalResponse, ApproveRequest, RejectRequest};
use flowspec_app::use_cases::cancel_run::{self, CancelRunRequest, CancelRunResponse};
use flowspec_app::use_cases::queries::{
    self, FlowSummary, GetRunStatusRequest, GetStepOutputRequest, ListRunsRequest, PendingApproval,
    PendingApprovalsRequest, RunStatus, RunSummaryView, RunTreeNode, StepOutputResponse,
};
use flowspec_app::use_cases::start_flow::{self, StartFlowRequest, StartFlowResponse};
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::{
    ServerHandler,
    handler::server::router::tool::ToolRouter,
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use std::sync::Arc;

#[derive(Debug, Clone, schemars::JsonSchema, serde::Deserialize)]
pub struct GetRunTreeRequest {
    /// Id of the run to inspect. Returns the run and every subflow it
    /// dispatched, recursively.
    pub run_id: RunId,
}

// A named field reads better to an LLM host than a bare array in
// structured_content, so these wrap the list-shaped use-case responses --
// a choice, not a constraint: rmcp no longer requires outputSchema to be a
// JSON object at the root (only inputSchema still is).

#[derive(Debug, Clone, schemars::JsonSchema, serde::Serialize)]
pub struct FlowsResponse {
    pub flows: Vec<FlowSummary>,
}

#[derive(Debug, Clone, schemars::JsonSchema, serde::Serialize)]
pub struct PendingApprovalsResponse {
    pub pending: Vec<PendingApproval>,
}

#[derive(Debug, Clone, schemars::JsonSchema, serde::Serialize)]
pub struct RunsResponse {
    pub runs: Vec<RunSummaryView>,
}

#[derive(Clone)]
pub struct FlowspecServer {
    container: Arc<Container>,
    tool_router: ToolRouter<Self>,
}

impl FlowspecServer {
    pub fn new(container: Arc<Container>) -> Self {
        FlowspecServer {
            container,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl FlowspecServer {
    #[tool(description = "Liveness check. Always replies 'pong'.")]
    async fn ping(&self) -> String {
        "pong".to_string()
    }

    #[tool(
        description = "List every available flow, with the version that will run by default and its declared inputs (name, type, required, default, description). Call this before start_flow to see what inputs a flow needs."
    )]
    async fn list_flows(&self) -> Json<FlowsResponse> {
        let flows = queries::list_flows(self.container.flow_source.clone()).await;
        Json(FlowsResponse { flows })
    }

    #[tool(
        description = "Start a run of a flow and return immediately with its run_id; the flow keeps executing in the background. Always send an idempotency_key: if this exact key was used before, the existing run is returned instead of starting a duplicate. Poll get_run_status or pending_approvals to follow progress."
    )]
    async fn start_flow(
        &self,
        Parameters(req): Parameters<StartFlowRequest>,
    ) -> Result<Json<StartFlowResponse>, ToolFailure> {
        let response = start_flow::start_flow(
            self.container.state_store.clone(),
            self.container.scheduler.clone(),
            self.container.flow_source.clone(),
            &self.container.scheduler_config,
            req,
        )
        .await?;
        Ok(Json(response))
    }

    #[tool(
        description = "Get the current status of a run: its phase, which steps are active right now, and a bounded summary of every step (including terminal ones). Output in the summary is truncated to ~500 characters; call get_step_output for the full value."
    )]
    async fn get_run_status(
        &self,
        Parameters(req): Parameters<GetRunStatusRequest>,
    ) -> Result<Json<RunStatus>, ToolFailure> {
        let container = self.container.clone();
        let probe =
            move |run_id: &RunId, step_id: &str| container.scheduler.liveness_ago(run_id, step_id);
        let status =
            queries::get_run_status(self.container.state_store.clone(), Some(&probe), req).await?;
        Ok(Json(status))
    }

    #[tool(
        description = "List steps currently waiting for human approval, across all runs or (with run_id) a single run. Each entry includes the step's output for review. Use approve_step or reject_step to act on one."
    )]
    async fn pending_approvals(
        &self,
        Parameters(req): Parameters<PendingApprovalsRequest>,
    ) -> Result<Json<PendingApprovalsResponse>, ToolFailure> {
        let pending = queries::pending_approvals(self.container.state_store.clone(), req).await?;
        Ok(Json(PendingApprovalsResponse { pending }))
    }

    #[tool(
        description = "Approve a step that is waiting for human review, letting the run continue. Find waiting steps with pending_approvals. Only steps in waiting_approval status can be approved; step_id is optional when exactly one step on the run is waiting."
    )]
    async fn approve_step(
        &self,
        Parameters(req): Parameters<ApproveRequest>,
    ) -> Result<Json<ApprovalResponse>, ToolFailure> {
        let response = approvals::approve_step(
            self.container.state_store.clone(),
            self.container.scheduler.clone(),
            req,
        )
        .await?;
        Ok(Json(response))
    }

    #[tool(
        description = "Reject a step that is waiting for human review, sending it back with feedback for another attempt. Find waiting steps with pending_approvals. The feedback text is handed to the agent verbatim, so be specific about what to change."
    )]
    async fn reject_step(
        &self,
        Parameters(req): Parameters<RejectRequest>,
    ) -> Result<Json<ApprovalResponse>, ToolFailure> {
        let response = approvals::reject_step(
            self.container.state_store.clone(),
            self.container.scheduler.clone(),
            req,
        )
        .await?;
        Ok(Json(response))
    }

    #[tool(
        description = "Cancel a run. Any in-flight step is stopped and the run moves to a terminal cancelled phase; already-completed steps are unaffected."
    )]
    async fn cancel_run(
        &self,
        Parameters(req): Parameters<CancelRunRequest>,
    ) -> Result<Json<CancelRunResponse>, ToolFailure> {
        let response = cancel_run::cancel_run(
            self.container.state_store.clone(),
            self.container.scheduler.clone(),
            req,
        )
        .await?;
        Ok(Json(response))
    }

    #[tool(
        description = "Get the full, untruncated output of one step attempt. Use this after get_run_status shows a truncated output_summary, or to inspect an older attempt after a rejection. Omit attempt for the latest one."
    )]
    async fn get_step_output(
        &self,
        Parameters(req): Parameters<GetStepOutputRequest>,
    ) -> Result<Json<StepOutputResponse>, ToolFailure> {
        let response = queries::get_step_output(self.container.state_store.clone(), req).await?;
        Ok(Json(response))
    }

    #[tool(
        description = "List runs, newest first, optionally filtered by flow_name and/or phase (one of: running, completed, failed, cancelled). Use limit to bound the result."
    )]
    async fn list_runs(
        &self,
        Parameters(req): Parameters<ListRunsRequest>,
    ) -> Result<Json<RunsResponse>, ToolFailure> {
        let runs = queries::list_runs(self.container.state_store.clone(), req).await?;
        Ok(Json(RunsResponse { runs }))
    }

    #[tool(
        description = "Get the parent/child hierarchy of subflow runs dispatched from a run, recursively. Useful for tracing a run that fans out into subflows."
    )]
    async fn get_run_tree(
        &self,
        Parameters(req): Parameters<GetRunTreeRequest>,
    ) -> Result<Json<RunTreeNode>, ToolFailure> {
        let tree = queries::get_run_tree(self.container.state_store.clone(), &req.run_id).await?;
        Ok(Json(tree))
    }
}

// rmcp 3.x's #[tool_handler] default router is `Self::tool_router()`, which
// rebuilds the router from scratch on every call -- unlike 0.16, which
// defaulted to the cached `self.tool_router` field built once in `new()`.
// Pin it back to the cached field explicitly.
#[tool_handler(router = self.tool_router.clone())]
impl ServerHandler for FlowspecServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "flowspec-devkitd: the FlowSpec runtime. Start flows with start_flow, always \
             sending an idempotency_key (a repeat call with the same key returns the \
             original run instead of starting a duplicate). Flows pause at approval steps: \
             poll pending_approvals to see what is waiting, then approve_step or \
             reject_step (feedback is handed to the agent verbatim). get_run_status returns \
             output *summaries* only (~500 chars); call get_step_output for full content. \
             If a run is failed with no failed step, check get_run_status's run_hooks: a \
             before_run/after_run lifecycle hook aborted it before any step started -- its \
             failure entry has the exit code, stdout, and stderr. A failed step's own hooks \
             (failed_in: \"after_hook\") are listed on that step's hooks field. \
             All tools return in milliseconds -- supervise long-running flows by polling, \
             never by waiting on a single call.",
        )
    }
}
