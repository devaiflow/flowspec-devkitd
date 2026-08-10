use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::SystemTime;

pub type Timestamp = SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPhase {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    WaitingApproval,
    WaitingOnSubflow,
    Completed,
    Failed,
    FailedRejected,
    Skipped,
    Cancelled,
}

impl StepStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            StepStatus::Completed
                | StepStatus::Failed
                | StepStatus::FailedRejected
                | StepStatus::Skipped
                | StepStatus::Cancelled
        )
    }

    pub fn is_active(self) -> bool {
        matches!(
            self,
            StepStatus::Running | StepStatus::WaitingApproval | StepStatus::WaitingOnSubflow
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRun {
    pub step_id: String,
    pub status: StepStatus,
    pub attempt: u32,

    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,

    pub input_resolved: Option<String>,
    pub output: Option<Value>,

    pub approval_status: Option<ApprovalStatus>,
    pub feedback: Option<String>,
    pub approval_comment: Option<String>,

    pub failure_reason: Option<String>,

    /// Subflow linkage; set once the child run is dispatched.
    pub child_run_id: Option<String>,
}

impl StepRun {
    pub fn pending(step_id: impl Into<String>) -> Self {
        StepRun {
            step_id: step_id.into(),
            status: StepStatus::Pending,
            attempt: 0,
            started_at: None,
            completed_at: None,
            input_resolved: None,
            output: None,
            approval_status: None,
            feedback: None,
            approval_comment: None,
            failure_reason: None,
            child_run_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Rejected,
}

/// The dynamic state of one run: every step's current `StepRun`. `phase` and
/// `active_steps` are *derived*, never stored (see `derive.rs`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunState {
    pub steps: Vec<StepRun>,
    pub created_at: Option<Timestamp>,
    pub cancelled: bool,
    /// Flow-declared inputs, resolved once at `RunStarted`. Template context for `{{ inputs.* }}`.
    pub inputs: Value,
    /// Raw trigger payload. Template context for `{{ trigger.* }}`.
    pub trigger: Value,
}

impl RunState {
    pub fn step(&self, step_id: &str) -> Option<&StepRun> {
        self.steps.iter().find(|s| s.step_id == step_id)
    }

    pub fn step_mut(&mut self, step_id: &str) -> Option<&mut StepRun> {
        self.steps.iter_mut().find(|s| s.step_id == step_id)
    }
}

/// Incoming events the engine reacts to.
#[derive(Debug, Clone)]
pub enum Event {
    RunStarted,
    StepCompleted {
        step_id: String,
        output: Value,
    },
    StepFailed {
        step_id: String,
        reason: String,
    },
    StepApproved {
        step_id: String,
        comment: Option<String>,
    },
    StepRejected {
        step_id: String,
        feedback: String,
    },
    SubflowFinished {
        step_id: String,
        phase: RunPhase,
        outputs: Option<Value>,
    },
    RunDeadlineExceeded,
    CancelRequested,
}

#[derive(Debug, Clone)]
pub struct StepActivation {
    pub step_id: String,
    pub attempt: u32,
    pub input: String,
}

#[derive(Debug, Clone)]
pub enum HookPhase {
    BeforeRun,
    AfterRun,
    BeforeStep { step_id: String },
    AfterStep { step_id: String },
}

/// Commands the engine emits. Pure data; the application layer executes them.
#[derive(Debug, Clone)]
pub enum Command {
    ActivateSteps(Vec<StepActivation>),
    WaitApproval {
        step_id: String,
    },
    StartChildRun {
        step_id: String,
    },
    RunHooks {
        phase: HookPhase,
        hooks: Vec<super::super::flow::types::HookCall>,
    },
    CancelSteps(Vec<String>),
    MarkStepStatus {
        step_id: String,
        status: StepStatus,
    },
    MarkRunTerminal(RunPhase),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EngineError {
    #[error("illegal transition for step '{step}': {from:?} -> {to:?}")]
    IllegalTransition {
        step: String,
        from: StepStatus,
        to: StepStatus,
    },
    #[error("unknown step '{0}' referenced by event")]
    UnknownStep(String),
}
