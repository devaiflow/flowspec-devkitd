use crate::ports::{
    FlowSource, HookRunRecord, HookStatus, RunFilter, RunId, RunSummary, StateStore, StoreError,
};
use crate::wire::{JobFailure, OutputSummary, Timestamp, summarize_output};
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_domain::run::types::{RunPhase, StepStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "schema")]
use schemars::JsonSchema;

/// Output summaries in status responses are capped at this many characters.
/// Full content is always available via `get_step_output`.
pub const OUTPUT_SUMMARY_MAX_CHARS: usize = 500;

/// Caller-supplied liveness probe: "how long ago did we last poll this step?"
pub type LivenessProbe<'a> = &'a (dyn Fn(&RunId, &str) -> Option<Duration> + Send + Sync);

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct GetRunStatusRequest {
    /// Id of the run to inspect, as returned by start_flow or list_runs.
    pub run_id: RunId,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct RunStatus {
    pub run_id: RunId,
    pub flow_name: String,
    pub flow_version: String,
    pub phase: String,
    /// Steps currently pending, running, or waiting on approval/subflow.
    pub active_steps: Vec<ActiveStepInfo>,
    /// Every step in the run's latest attempt, including terminal ones.
    /// Output is a size-bounded summary; call get_step_output for the full
    /// value.
    pub steps: Vec<StepInfo>,
    /// This run's `before_run`/`after_run` lifecycle hooks. Check this when
    /// `phase` is `failed` but `steps` is empty or shows no failure -- a
    /// gating hook aborted the run before (or after) any step ran.
    pub run_hooks: Vec<HookInfo>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// One executed hook (`before_run`, `after_run`, or a step's `after_step`).
/// Steps and hooks run through the same devkitd job interface, so a failure
/// here carries the same shape as a step's: `failure_summary` truncated like
/// every other status-response field, `failure` structured with exit code,
/// stdout, and stderr.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct HookInfo {
    pub hook: String,
    /// before_run | after_run | after_step
    pub phase: String,
    /// completed | failed | skipped
    pub status: String,
    pub job_id: Option<String>,
    pub failure_summary: Option<OutputSummary>,
    pub failure: Option<JobFailure>,
    pub started_at: Timestamp,
    pub completed_at: Option<Timestamp>,
}

fn hook_info(h: &HookRunRecord) -> HookInfo {
    HookInfo {
        hook: h.hook.clone(),
        phase: status_str_hook_phase(h.phase),
        status: status_str_hook_status(h.status),
        job_id: h.job_id.clone(),
        failure_summary: h
            .failure_reason
            .as_ref()
            .map(|r| summarize_output(&Value::String(r.clone()), OUTPUT_SUMMARY_MAX_CHARS)),
        failure: h.failure_detail.clone(),
        started_at: h.started_at.into(),
        completed_at: h.completed_at.map(Timestamp::from),
    }
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct ActiveStepInfo {
    pub step_id: String,
    pub status: String,
    /// Seconds since the last confirmed liveness poll from devkitd, if the
    /// step is currently executing.
    pub last_poll_ago_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct StepInfo {
    pub step_id: String,
    pub status: String,
    pub attempt: u32,
    pub output_summary: Option<OutputSummary>,
    pub failure_reason: Option<String>,
    /// Structured sibling of `failure_reason`: exit code, stdout, stderr.
    pub failure: Option<JobFailure>,
    /// Set only when `status` is `failed`: "step" when the step's own job
    /// failed, "after_hook" when one of its `after:` hooks failed instead
    /// (`hooks` below names which one). Distinguishes "the agent failed"
    /// from "the agent succeeded but its follow-up hook didn't".
    pub failed_in: Option<String>,
    /// This step's `after:` hook executions.
    pub hooks: Vec<HookInfo>,
    pub feedback: Option<String>,
    pub last_poll_ago_secs: Option<u64>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct PendingApprovalsRequest {
    /// Restrict results to a single run. Omit to see waiting steps across
    /// all runs.
    pub run_id: Option<RunId>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct PendingApproval {
    pub run_id: RunId,
    pub step_id: String,
    /// The step's output, for review before approving or rejecting.
    pub output: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct FlowInputSummary {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub required: bool,
    pub default: Option<Value>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct FlowSummary {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub inputs: Vec<FlowInputSummary>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct GetStepOutputRequest {
    /// Id of the run the step belongs to.
    pub run_id: RunId,
    /// Id of the step, as seen in get_run_status's steps list.
    pub step_id: String,
    /// Which attempt to fetch. Omit for the latest attempt.
    pub attempt: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct StepOutputResponse {
    pub run_id: RunId,
    pub step_id: String,
    pub attempt: u32,
    pub status: String,
    /// Full, untruncated output for this attempt.
    pub output: Option<Value>,
    pub failure_reason: Option<String>,
    /// Structured sibling of `failure_reason`: exit code, stdout, stderr.
    pub failure: Option<JobFailure>,
    pub feedback: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct ListRunsRequest {
    pub flow_name: Option<String>,
    /// One of: running, completed, failed, cancelled.
    pub phase: Option<String>,
    /// Maximum number of runs to return, newest first.
    pub limit: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("step '{0}' not found")]
    StepNotFound(String),
    #[error("attempt {0} of step '{1}' not found")]
    AttemptNotFound(u32, String),
    #[error("invalid phase '{0}'; expected one of: running, completed, failed, cancelled")]
    InvalidPhase(String),
}

pub fn parse_phase(raw: &str) -> Result<RunPhase, QueryError> {
    serde_json::from_value(Value::String(raw.to_string()))
        .map_err(|_| QueryError::InvalidPhase(raw.to_string()))
}

pub async fn get_run_status(
    store: Arc<dyn StateStore>,
    liveness: Option<LivenessProbe<'_>>,
    req: GetRunStatusRequest,
) -> Result<RunStatus, QueryError> {
    let run_id = req.run_id;
    let record = store.load_run(&run_id).await?;
    let state = record.run_state();
    let active: Vec<ActiveStepInfo> = crate::scheduler::run_active_steps(&state)
        .into_iter()
        .map(|step_id| {
            let status = state
                .step(&step_id)
                .map(|s| status_str(s.status))
                .unwrap_or_else(|| "unknown".to_string());
            let last_poll_ago_secs = liveness
                .and_then(|f| f(&run_id, &step_id))
                .map(|d| d.as_secs());
            ActiveStepInfo {
                step_id,
                status,
                last_poll_ago_secs,
            }
        })
        .collect();

    let hook_runs = store.list_hook_runs(&run_id).await?;
    let run_hooks: Vec<HookInfo> = hook_runs
        .iter()
        .filter(|h| h.step_id.is_none())
        .map(hook_info)
        .collect();

    let steps: Vec<StepInfo> = record
        .latest_steps()
        .into_iter()
        .map(|s| {
            let last_poll_ago_secs = liveness
                .and_then(|f| f(&run_id, &s.run.step_id))
                .map(|d| d.as_secs());
            let step_hooks: Vec<&HookRunRecord> = hook_runs
                .iter()
                .filter(|h| h.step_id.as_deref() == Some(s.run.step_id.as_str()))
                .collect();
            // Not attempt-scoped: `HookRunRecord` doesn't carry an attempt
            // number, so a step with multiple attempts can't distinguish
            // which attempt's hook this was. Good enough for "was it a
            // hook or the step itself" triage; exact attempt matching would
            // need a schema change.
            let failed_in = (s.run.status == StepStatus::Failed).then(|| {
                if step_hooks.iter().any(|h| h.status == HookStatus::Failed) {
                    "after_hook".to_string()
                } else {
                    "step".to_string()
                }
            });
            StepInfo {
                step_id: s.run.step_id.clone(),
                status: status_str(s.run.status),
                attempt: s.run.attempt,
                output_summary: s
                    .run
                    .output
                    .as_ref()
                    .map(|o| summarize_output(o, OUTPUT_SUMMARY_MAX_CHARS)),
                failure_reason: s.run.failure_reason.clone(),
                failure: s.failure_detail.clone(),
                failed_in,
                hooks: step_hooks.into_iter().map(hook_info).collect(),
                feedback: s.run.feedback.clone(),
                last_poll_ago_secs,
                started_at: s.run.started_at.map(Timestamp::from),
                completed_at: s.run.completed_at.map(Timestamp::from),
            }
        })
        .collect();

    Ok(RunStatus {
        run_id: run_id.clone(),
        flow_name: record.flow_name,
        flow_version: record.flow_version,
        phase: serde_json::to_value(record.phase)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        active_steps: active,
        steps,
        run_hooks,
        created_at: record.created_at.into(),
        updated_at: record.updated_at.into(),
    })
}

/// Prefix flowspec puts on `idempotency_key` for runs started from the
/// platform's `trigger_run` action -- see `platform::poller`. Kept here
/// (rather than only in `flowspec-server`) because `flow_run_snapshot`
/// needs to strip it back off to recover the platform's own run id.
pub const PLATFORM_IDEMPOTENCY_PREFIX: &str = "platform:";

/// Mirrors the platform's `StepRunSchema` (`src/lib/run-schemas.ts`)
/// field-for-field. Distinct from `StepInfo`: that struct is the MCP host's
/// ergonomic, output-truncated view; this one is the platform's full replica
/// projection, pushed verbatim by the connector's pump loop.
#[derive(Debug, Clone, Serialize)]
pub struct StepRunSnapshot {
    pub step_id: String,
    pub status: String,
    #[serde(rename = "type")]
    pub step_type: String,
    pub with_resolved: Option<Value>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub duration_seconds: Option<f64>,
    pub input_resolved: Option<String>,
    pub child_run_id: Option<String>,
    pub human_loop: bool,
    pub approval_status: Option<String>,
    pub approval_comment: Option<String>,
    pub feedback: Option<String>,
    pub failure_reason: Option<String>,
    pub attempt: u32,
}

/// Mirrors the platform's `FlowRunSchema`. `run_id` here is the *platform's*
/// run id (the `idempotency_key` with its `platform:` prefix stripped), not
/// flowspec's own `run_<ulid>` -- the platform's push endpoints are keyed by
/// their own id, per `docs/platform-agent-api.md`. Tokens/cost are omitted
/// entirely rather than sent as zero/null: the platform's Zod schema marks
/// them optional and the endpoint is `.passthrough()`, so omission is
/// indistinguishable from "not yet known" instead of lying with a `0`.
#[derive(Debug, Clone, Serialize)]
pub struct FlowRunSnapshot {
    pub run_id: String,
    pub flow: String,
    pub flow_version: String,
    pub phase: String,
    pub active_steps: Vec<String>,
    pub started_at: Timestamp,
    pub completed_at: Option<Timestamp>,
    pub steps: Vec<StepRunSnapshot>,
}

/// Strips the `platform:` prefix flowspec adds to `idempotency_key` for
/// platform-originated runs. `None` if the run has no idempotency key or it
/// wasn't platform-prefixed (an MCP-started run has no platform mirror).
pub fn platform_run_id(idempotency_key: Option<&str>) -> Option<&str> {
    idempotency_key.and_then(|k| k.strip_prefix(PLATFORM_IDEMPOTENCY_PREFIX))
}

pub async fn flow_run_snapshot(
    store: Arc<dyn StateStore>,
    run_id: &RunId,
) -> Result<FlowRunSnapshot, QueryError> {
    let record = store.load_run(run_id).await?;
    let state = record.run_state();
    let active_steps = crate::scheduler::run_active_steps(&state);

    let steps: Vec<StepRunSnapshot> = record
        .latest_steps()
        .into_iter()
        .map(|s| {
            let step_def = record.definition.step(&s.run.step_id);
            let step_type = step_def
                .map(|st| st.type_name().to_string())
                .unwrap_or_else(|| "cli".to_string());
            let human_loop = step_def.map(|st| st.human_loop).unwrap_or(false);
            let duration_seconds = match (s.run.started_at, s.run.completed_at) {
                (Some(started), Some(completed)) => completed
                    .duration_since(started)
                    .ok()
                    .map(|d| d.as_secs_f64()),
                _ => None,
            };
            StepRunSnapshot {
                step_id: s.run.step_id.clone(),
                status: status_str(s.run.status),
                step_type,
                with_resolved: s.with_resolved.clone(),
                started_at: s.run.started_at.map(Timestamp::from),
                completed_at: s.run.completed_at.map(Timestamp::from),
                duration_seconds,
                input_resolved: s.run.input_resolved.clone(),
                child_run_id: s.run.child_run_id.clone(),
                human_loop,
                approval_status: s.run.approval_status.map(|a| {
                    serde_json::to_value(a)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string()
                }),
                approval_comment: s.run.approval_comment.clone(),
                feedback: s.run.feedback.clone(),
                failure_reason: s.run.failure_reason.clone(),
                attempt: s.run.attempt,
            }
        })
        .collect();

    let is_terminal = matches!(
        record.phase,
        RunPhase::Completed | RunPhase::Failed | RunPhase::Cancelled
    );
    let completed_at = is_terminal.then(|| {
        record
            .steps
            .iter()
            .filter_map(|s| s.run.completed_at)
            .max()
            .unwrap_or(record.updated_at)
            .into()
    });

    Ok(FlowRunSnapshot {
        run_id: platform_run_id(record.idempotency_key.as_deref())
            .unwrap_or(&record.run_id)
            .to_string(),
        flow: record.flow_name,
        flow_version: record.flow_version,
        phase: status_str_phase(record.phase),
        active_steps,
        started_at: record.created_at.into(),
        completed_at,
        steps,
    })
}

pub async fn get_step_output(
    store: Arc<dyn StateStore>,
    req: GetStepOutputRequest,
) -> Result<StepOutputResponse, QueryError> {
    let record = store.load_run(&req.run_id).await?;
    let step = match req.attempt {
        Some(attempt) => record
            .steps
            .iter()
            .find(|s| s.run.step_id == req.step_id && s.run.attempt == attempt)
            .ok_or_else(|| QueryError::AttemptNotFound(attempt, req.step_id.clone()))?,
        None => record
            .latest_steps()
            .into_iter()
            .find(|s| s.run.step_id == req.step_id)
            .ok_or_else(|| QueryError::StepNotFound(req.step_id.clone()))?,
    };
    Ok(StepOutputResponse {
        run_id: req.run_id,
        step_id: step.run.step_id.clone(),
        attempt: step.run.attempt,
        status: status_str(step.run.status),
        output: step.run.output.clone(),
        failure_reason: step.run.failure_reason.clone(),
        failure: step.failure_detail.clone(),
        feedback: step.run.feedback.clone(),
    })
}

pub async fn pending_approvals(
    store: Arc<dyn StateStore>,
    req: PendingApprovalsRequest,
) -> Result<Vec<PendingApproval>, QueryError> {
    let records = match req.run_id {
        Some(run_id) => vec![store.load_run(&run_id).await?],
        None => {
            let runs = store.list_runs(RunFilter::default()).await?;
            let mut records = Vec::with_capacity(runs.len());
            for summary in runs {
                records.push(store.load_run(&summary.run_id).await?);
            }
            records
        }
    };

    let mut out = Vec::new();
    for record in records {
        for step in record.latest_steps() {
            if step.run.status == StepStatus::WaitingApproval {
                out.push(PendingApproval {
                    run_id: record.run_id.clone(),
                    step_id: step.run.step_id.clone(),
                    output: step.run.output.clone(),
                });
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct RunSummaryView {
    pub run_id: RunId,
    pub flow_name: String,
    pub flow_version: String,
    pub phase: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl From<RunSummary> for RunSummaryView {
    fn from(s: RunSummary) -> Self {
        RunSummaryView {
            run_id: s.run_id,
            flow_name: s.flow_name,
            flow_version: s.flow_version,
            phase: status_str_phase(s.phase),
            created_at: s.created_at,
            updated_at: s.updated_at,
        }
    }
}

pub async fn list_runs(
    store: Arc<dyn StateStore>,
    req: ListRunsRequest,
) -> Result<Vec<RunSummaryView>, QueryError> {
    let phase = req.phase.as_deref().map(parse_phase).transpose()?;
    let runs = store
        .list_runs(RunFilter {
            flow_name: req.flow_name,
            phase,
            limit: req.limit,
        })
        .await?;
    Ok(runs.into_iter().map(RunSummaryView::from).collect())
}

pub async fn get_run_tree(
    store: Arc<dyn StateStore>,
    run_id: &RunId,
) -> Result<RunTreeNode, QueryError> {
    let tree = store.load_run_tree(run_id).await?;
    Ok(project_tree(tree))
}

#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct RunTreeNode {
    pub run_id: RunId,
    pub flow_name: String,
    pub flow_version: String,
    pub phase: String,
    pub children: Vec<RunTreeNode>,
}

fn project_tree(tree: crate::ports::RunTree) -> RunTreeNode {
    RunTreeNode {
        run_id: tree.root.run_id.clone(),
        flow_name: tree.root.flow_name.clone(),
        flow_version: tree.root.flow_version.clone(),
        phase: serde_json::to_value(tree.root.phase)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string(),
        children: tree.children.into_iter().map(project_tree).collect(),
    }
}

pub async fn list_flows(flows: Arc<dyn FlowSource>) -> Vec<FlowSummary> {
    list_flows_latest(flows)
        .await
        .into_values()
        .map(|f| FlowSummary {
            name: f.name.clone(),
            version: f.version.clone(),
            description: f.description.clone(),
            inputs: f
                .inputs
                .iter()
                .map(|(name, spec)| FlowInputSummary {
                    name: name.clone(),
                    ty: spec.ty.clone(),
                    required: spec.required,
                    default: spec.default.clone(),
                    description: spec.description.clone(),
                })
                .collect(),
        })
        .collect()
}

/// List flows keyed by name, keeping only the highest matching version.
pub async fn list_flows_latest(flows: Arc<dyn FlowSource>) -> HashMap<String, FlowDefinition> {
    let mut by_name: HashMap<String, FlowDefinition> = HashMap::new();
    for flow in flows.list().await {
        by_name
            .entry(flow.name.clone())
            .and_modify(|existing| {
                let new = semver::Version::parse(&flow.version).ok();
                let old = semver::Version::parse(&existing.version).ok();
                if matches!((new, old), (Some(n), Some(o)) if n > o) {
                    *existing = flow.clone();
                }
            })
            .or_insert(flow);
    }
    by_name
}

fn status_str_phase(phase: RunPhase) -> String {
    serde_json::to_value(phase)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn status_str(status: StepStatus) -> String {
    serde_json::to_value(status)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn status_str_hook_phase(phase: crate::ports::HookPhase) -> String {
    serde_json::to_value(phase)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn status_str_hook_status(status: HookStatus) -> String {
    serde_json::to_value(status)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}
