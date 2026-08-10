//! Domain-outcome error contract for MCP tools.
//!
//! Two channels, used deliberately: *protocol errors* (malformed input,
//! unknown tool) surface as MCP `ErrorData` for free, via rmcp's
//! `Parameters<T>` extractor. *Domain outcomes* (run not found, step not
//! approvable, flow validation failed) are a successful tool call whose
//! result has `is_error: true` and a structured body — hosts recover from
//! tool-result errors conversationally (they read the message and correct
//! course), while transport-level errors often abort the host's plan.
//!
//! `message` must be self-sufficient: assume it is the only thing the LLM
//! reads.

use flowspec_app::ports::StoreError;
use flowspec_app::use_cases::approvals::ApprovalError;
use flowspec_app::use_cases::queries::QueryError;
use flowspec_app::use_cases::start_flow::StartFlowError;
use rmcp::model::{ContentBlock, IntoContents};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    RunNotFound,
    StepNotFound,
    FlowNotFound,
    MissingInputs,
    NotApprovable,
    NoWaitingStep,
    AmbiguousStep,
    InvalidPhase,
    ValidationFailed,
    StoreError,
    Internal,
}

/// A domain-outcome failure. Serializes to
/// `{ "error_kind": "...", "message": "...", "detail": {...} }` and is
/// carried on `CallToolResult` with `is_error: true`.
#[derive(Debug, Clone, Serialize)]
pub struct ToolFailure {
    pub error_kind: ErrorKind,
    pub message: String,
    #[serde(skip_serializing_if = "Value::is_null")]
    pub detail: Value,
}

impl ToolFailure {
    pub fn new(error_kind: ErrorKind, message: impl Into<String>) -> Self {
        ToolFailure {
            error_kind,
            message: message.into(),
            detail: Value::Null,
        }
    }
}

impl IntoContents for ToolFailure {
    fn into_contents(self) -> Vec<ContentBlock> {
        let text = serde_json::to_string_pretty(&self).unwrap_or_else(|_| {
            format!(
                "{{\"error_kind\":\"internal\",\"message\":{:?}}}",
                self.message
            )
        });
        vec![ContentBlock::text(text)]
    }
}

impl From<StoreError> for ToolFailure {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::NotFound(id) => ToolFailure::new(
                ErrorKind::RunNotFound,
                format!(
                    "Run '{id}' was not found. Check the run_id, or call list_runs to see known runs."
                ),
            ),
            StoreError::Duplicate(key) => ToolFailure::new(
                ErrorKind::ValidationFailed,
                format!("idempotency_key '{key}' is already in use by another run."),
            ),
            other => ToolFailure::new(ErrorKind::StoreError, other.to_string()),
        }
    }
}

impl From<StartFlowError> for ToolFailure {
    fn from(e: StartFlowError) -> Self {
        match e {
            StartFlowError::FlowNotFound(name) => ToolFailure::new(
                ErrorKind::FlowNotFound,
                format!(
                    "Flow '{name}' was not found. Call list_flows to see available flows and their versions."
                ),
            ),
            StartFlowError::MissingInputs(names) => ToolFailure::new(
                ErrorKind::MissingInputs,
                format!(
                    "Missing required input(s): {names}. Call list_flows to see the flow's declared inputs."
                ),
            ),
            StartFlowError::Store(e) => e.into(),
        }
    }
}

impl From<ApprovalError> for ToolFailure {
    fn from(e: ApprovalError) -> Self {
        match e {
            ApprovalError::Store(e) => e.into(),
            ApprovalError::NoWaitingStep => ToolFailure::new(
                ErrorKind::NoWaitingStep,
                "No step on this run is waiting for approval. Call pending_approvals to check current status.",
            ),
            ApprovalError::AmbiguousStep => ToolFailure::new(
                ErrorKind::AmbiguousStep,
                "More than one step on this run is waiting for approval; step_id is required. Call pending_approvals to see which steps are waiting.",
            ),
            ApprovalError::NotWaiting(step_id) => ToolFailure::new(
                ErrorKind::NotApprovable,
                format!(
                    "Step '{step_id}' is not waiting for approval. Call pending_approvals to see which steps are waiting."
                ),
            ),
        }
    }
}

impl From<QueryError> for ToolFailure {
    fn from(e: QueryError) -> Self {
        let message = e.to_string();
        match e {
            QueryError::Store(e) => e.into(),
            QueryError::StepNotFound(step_id) => ToolFailure::new(
                ErrorKind::StepNotFound,
                format!(
                    "Step '{step_id}' was not found on this run. Call get_run_status to see the run's steps."
                ),
            ),
            QueryError::AttemptNotFound(attempt, step_id) => ToolFailure::new(
                ErrorKind::StepNotFound,
                format!(
                    "Attempt {attempt} of step '{step_id}' was not found. Omit attempt to fetch the latest one, or call get_run_status to see valid attempts."
                ),
            ),
            QueryError::InvalidPhase(_) => ToolFailure::new(ErrorKind::InvalidPhase, message),
        }
    }
}
