use crate::wire::JobFailure;
use async_trait::async_trait;
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_domain::run::types::{ApprovalStatus, RunPhase, RunState, StepRun, StepStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::SystemTime;

pub type RunId = String;

/// Persistence-only wrapper around a domain `StepRun`. Keeps devkitd's job
/// handle and the post-template `with:` snapshot out of the pure core.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    #[serde(flatten)]
    pub run: StepRun,
    pub job_id: Option<String>,
    pub with_resolved: Option<Value>,
    /// Structured sibling of `run.failure_reason`. Kept out of the pure
    /// domain `StepRun` -- the engine only needs the string reason for
    /// routing, this is a persistence/triage concern. Set via
    /// `Mutation::SetStepFailureDetail`, independently of `run.failure_reason`.
    #[serde(default)]
    pub failure_detail: Option<JobFailure>,
}

impl StepRecord {
    pub fn pending(step_id: impl Into<String>) -> Self {
        Self {
            run: StepRun::pending(step_id),
            job_id: None,
            with_resolved: None,
            failure_detail: None,
        }
    }
}

/// Everything stored about one run. The flow definition is snapshotted so a
/// run always executes the version it started with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: RunId,
    pub flow_name: String,
    pub flow_version: String,
    pub definition: FlowDefinition,
    pub inputs: Value,
    pub trigger: Value,
    pub phase: RunPhase,
    pub cancelled: bool,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub idempotency_key: Option<String>,
    pub steps: Vec<StepRecord>,
}

impl RunRecord {
    /// Project this record into the domain's runtime state. Only the latest
    /// attempt per step is visible to the engine.
    pub fn run_state(&self) -> RunState {
        RunState {
            steps: self
                .latest_steps()
                .into_iter()
                .map(|s| s.run.clone())
                .collect(),
            created_at: Some(self.created_at),
            cancelled: self.cancelled,
            inputs: self.inputs.clone(),
            trigger: self.trigger.clone(),
        }
    }

    /// The most recent attempt for each step id.
    pub fn latest_steps(&self) -> Vec<&StepRecord> {
        let mut latest: std::collections::HashMap<&str, &StepRecord> =
            std::collections::HashMap::new();
        for step in &self.steps {
            let entry = latest.entry(step.run.step_id.as_str()).or_insert(step);
            if step.run.attempt > entry.run.attempt {
                *entry = step;
            }
        }
        let mut out: Vec<&StepRecord> = latest.into_values().collect();
        out.sort_by_key(|s| s.run.step_id.as_str());
        out
    }
}

#[derive(Debug, Clone)]
pub struct NewRun {
    pub flow_name: String,
    pub flow_version: String,
    pub definition: FlowDefinition,
    pub inputs: Value,
    pub trigger: Value,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: RunId,
    pub flow_name: String,
    pub flow_version: String,
    pub phase: RunPhase,
    pub created_at: crate::wire::Timestamp,
    pub updated_at: crate::wire::Timestamp,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RunFilter {
    pub flow_name: Option<String>,
    pub phase: Option<RunPhase>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct RunTree {
    pub root: RunRecord,
    pub children: Vec<RunTree>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPhase {
    BeforeRun,
    AfterRun,
    BeforeStep,
    AfterStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookStatus {
    Completed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone)]
pub struct HookRunRecord {
    pub hook: String,
    pub phase: HookPhase,
    pub step_id: Option<String>,
    pub status: HookStatus,
    pub started_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub args_resolved: Option<Value>,
    pub output_ref: Option<String>,
    pub failure_reason: Option<String>,
    /// Structured sibling of `failure_reason`. See `StepRecord::failure_detail`.
    pub failure_detail: Option<JobFailure>,
    pub job_id: Option<String>,
}

/// Event types pushed to the platform's run-event log. Mirrors the
/// platform's `RunEventTypeSchema` (`src/lib/run-schemas.ts`) exactly, so a
/// typo can't reach the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunEventType {
    RunStarted,
    StepActivated,
    StepStarted,
    StepStreamDelta,
    StepCompleted,
    StepFailed,
    StepWaitingApproval,
    StepWaitingOnSubflow,
    ApprovalResolved,
    RunCompleted,
    RunFailed,
    RunCancelled,
}

/// A single outbox entry for the run-event log. `sequence` is deliberately
/// absent here -- the store assigns it (`MAX(sequence)+1` per run) inside
/// the same transaction the mutation batch commits in.
#[derive(Debug, Clone)]
pub struct RunEventRecord {
    pub event_type: RunEventType,
    pub step_id: Option<String>,
    pub timestamp: SystemTime,
    pub payload: Value,
}

/// A store-assigned, persisted run event ready to push to the platform.
#[derive(Debug, Clone)]
pub struct StoredRunEvent {
    pub sequence: u64,
    pub event_type: RunEventType,
    pub step_id: Option<String>,
    pub timestamp: SystemTime,
    pub payload: Value,
}

/// A single batched write to a run. Applied atomically by the store.
#[derive(Debug, Clone)]
pub enum Mutation {
    InsertStepRun(StepRecord),
    SetStepStatus {
        step_id: String,
        attempt: u32,
        status: StepStatus,
    },
    SetStepOutput {
        step_id: String,
        attempt: u32,
        output: Value,
    },
    SetJobId {
        step_id: String,
        attempt: u32,
        job_id: Option<String>,
    },
    SetApproval {
        step_id: String,
        attempt: u32,
        approval_status: ApprovalStatus,
        comment: Option<String>,
        feedback: Option<String>,
    },
    SetStepFailure {
        step_id: String,
        attempt: u32,
        reason: String,
    },
    /// Applied independently of `SetStepFailure`, directly from the job task
    /// that still holds the live `DevkitdError` -- see `JobFailure`.
    SetStepFailureDetail {
        step_id: String,
        attempt: u32,
        detail: JobFailure,
    },
    SetStepTimestamps {
        step_id: String,
        attempt: u32,
        started_at: Option<SystemTime>,
        completed_at: Option<SystemTime>,
    },
    SetChildRunId {
        step_id: String,
        attempt: u32,
        child_run_id: Option<String>,
    },
    SetRunPhase(RunPhase),
    SetRunCancelled,
    RecordHookRun(HookRunRecord),
    AppendEvent(RunEventRecord),
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("run not found: {0}")]
    NotFound(String),
    #[error("duplicate idempotency key: {0}")]
    Duplicate(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("backend error: {0}")]
    Backend(String),
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn create_run(&self, new: NewRun) -> Result<RunId, StoreError>;
    async fn load_run(&self, run_id: &str) -> Result<RunRecord, StoreError>;
    async fn find_by_idempotency_key(&self, key: &str) -> Result<Option<RunId>, StoreError>;
    async fn apply(&self, run_id: &str, mutations: Vec<Mutation>) -> Result<(), StoreError>;
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError>;
    async fn runs_in_phase(&self, phase: RunPhase) -> Result<Vec<RunRecord>, StoreError>;
    async fn link_runs(
        &self,
        parent_run: &str,
        parent_step: &str,
        child_run: &str,
    ) -> Result<(), StoreError>;
    async fn load_run_tree(&self, run_id: &str) -> Result<RunTree, StoreError>;
    /// Every hook execution recorded for a run, in execution order. Empty
    /// (not an error) for a run whose lifecycle declares no hooks.
    async fn list_hook_runs(&self, run_id: &str) -> Result<Vec<HookRunRecord>, StoreError>;

    /// Up to `limit` events not yet marked pushed, across all runs, oldest
    /// first per run. Used by the platform connector's pump loop.
    async fn unpushed_events(
        &self,
        limit: usize,
    ) -> Result<Vec<(RunId, StoredRunEvent)>, StoreError>;

    /// Marks every event for `run_id` with `sequence <= through_sequence` as
    /// pushed. Idempotent -- re-marking already-pushed events is a no-op.
    async fn mark_events_pushed(
        &self,
        run_id: &str,
        through_sequence: u64,
    ) -> Result<(), StoreError>;
}

/// Configuration for the scheduler: timeouts, polling, and executor binding.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Poll interval used by the devkitd adapter (seconds).
    pub poll_interval_secs: u64,
    /// Extra margin added to step deadlines before forcing cancellation (seconds).
    pub deadline_margin_secs: u64,
    /// Default timeout for a single step when the flow does not declare one (seconds).
    pub default_step_timeout_secs: u64,
    /// Maximum step output size the runtime will retain (kilobytes).
    pub max_step_output_kb: u64,
    /// devkitd tool name used for `type: cli` steps.
    pub executor_cli_tool: String,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 5,
            deadline_margin_secs: 30,
            default_step_timeout_secs: 3600,
            max_step_output_kb: 256,
            executor_cli_tool: "agent-run".to_string(),
        }
    }
}

/// Opaque handle returned by devkitd when a job is accepted. Persistable so that
/// a restart can re-attach to an in-flight job.
#[derive(Debug, Clone)]
pub struct JobHandle(pub String);

/// Structured output of a devkitd step or hook. The adapter is responsible for
/// truncation; the scheduler treats this as opaque JSON.
#[derive(Debug, Clone)]
pub struct StepOutput {
    pub output: Value,
}

/// Request to start a devkitd job. `tool` is the MCP tool name; `args` is the
/// fully-resolved argument map for that tool.
#[derive(Debug, Clone)]
pub struct StartRequest {
    pub tool: String,
    pub args: Value,
    pub timeout_seconds: Option<u64>,
}

/// Errors that can come out of the devkitd port. The scheduler maps these to
/// `StepFailed` / hook failure; the exact decode is adapter-specific in Phase 4.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DevkitdError {
    #[error("tool error: exit_code={exit_code} stdout={stdout} stderr={stderr}")]
    ToolError {
        stdout: String,
        stderr: String,
        exit_code: i32,
    },
    #[error("timeout")]
    Timeout,
    #[error("interrupted")]
    Interrupted,
    #[error("cancelled")]
    Cancelled,
    #[error("unreachable")]
    Unreachable,
}

/// Callback invoked by the devkitd adapter on every successful job-status poll.
/// The scheduler closes over `run_id`/`step_id`; the adapter only signals "still alive".
pub type LivenessSink = Arc<dyn Fn() + Send + Sync>;

/// Outbound port to devkitd. Both steps and hooks execute through this same
/// job-shaped interface; they differ only in trigger and result destination.
#[async_trait]
pub trait Devkitd: Send + Sync {
    /// Start a job and return its handle immediately. Argument validation errors
    /// are returned synchronously; execution happens in the background.
    async fn start(&self, req: StartRequest) -> Result<JobHandle, DevkitdError>;

    /// Poll until the job reaches a terminal state. `deadline` is the
    /// scheduler-computed backstop. Each successful poll calls `liveness`.
    async fn wait(
        &self,
        handle: &JobHandle,
        deadline: Option<SystemTime>,
        liveness: LivenessSink,
    ) -> Result<StepOutput, DevkitdError>;

    /// Cancel a running job. Independent of any connection lifetime.
    async fn cancel(&self, handle: &JobHandle) -> Result<(), DevkitdError>;
}

/// Source of flow definitions. The domain validates the returned shapes.
#[async_trait]
pub trait FlowSource: Send + Sync {
    async fn list(&self) -> Vec<FlowDefinition>;
    async fn get(&self, name: &str, version_req: Option<&str>) -> Option<FlowDefinition>;
}
