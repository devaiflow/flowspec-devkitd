use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Either one string target or a fan-out list. A single string is sugar for a one-element list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RouteTarget {
    One(String),
    Many(Vec<String>),
}

impl RouteTarget {
    pub fn as_slice(&self) -> Vec<&str> {
        match self {
            RouteTarget::One(s) => vec![s.as_str()],
            RouteTarget::Many(v) => v.iter().map(String::as_str).collect(),
        }
    }
}

pub const DONE: &str = "done";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum HookCondition {
    #[default]
    Succeeded,
    Failed,
    Cancelled,
    Always,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookCall {
    pub hook: String,
    #[serde(default)]
    pub args: IndexMap<String, serde_json::Value>,
    pub timeout: Option<String>,
    #[serde(default)]
    pub when: Option<HookCondition>,
    #[serde(default)]
    pub always_continue: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleHooks {
    #[serde(default)]
    pub before_run: Vec<HookCall>,
    #[serde(default)]
    pub after_run: Vec<HookCall>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliWith {
    pub cli: String,
    pub role: Option<String>,
    pub input: String,
    pub output: Option<String>,
    #[serde(flatten)]
    pub extra: IndexMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubflowWith {
    pub flow: String,
    pub version: Option<String>,
    #[serde(default)]
    pub inputs: IndexMap<String, serde_json::Value>,
    pub output: Option<String>,
}

/// The parsed, type-specific `with:` payload. Not deserialized directly onto
/// `Step` (see the note on `Step::with` below); built on demand via `Step::kind`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StepWith {
    Cli { with: CliWith },
    Subflow { with: SubflowWith },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepType {
    Cli,
    Subflow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    pub id: String,

    #[serde(rename = "type")]
    pub step_type: StepType,
    /// Raw `with:` payload, kept untyped here. Serde's `#[serde(flatten)]` does
    /// not cooperate with `deny_unknown_fields` (silently drops the unknown-field
    /// check), so the type-specific shape is parsed on demand via `Step::kind`
    /// instead of flattening `StepWith` onto this struct.
    pub with: serde_json::Value,

    #[serde(default)]
    pub human_loop: bool,
    pub timeout: Option<String>,
    #[serde(default)]
    pub retries: u32,
    pub reject_input: Option<String>,

    #[serde(default)]
    pub before: Vec<HookCall>,
    #[serde(default)]
    pub after: Vec<HookCall>,

    pub on_success: Option<RouteTarget>,
    pub on_failure: Option<RouteTarget>,
    pub on_approve: Option<RouteTarget>,
    pub on_reject: Option<RouteTarget>,

    #[serde(default)]
    pub needs: Vec<String>,
}

impl Step {
    pub fn type_name(&self) -> &'static str {
        match self.step_type {
            StepType::Cli => "cli",
            StepType::Subflow => "subflow",
        }
    }

    /// Parses `with` into its type-specific shape. `None` means `with` doesn't
    /// match `step_type`'s schema -- a "type/with consistency" violation the
    /// caller should surface (flow-load rule 6).
    pub fn kind(&self) -> Option<StepWith> {
        match self.step_type {
            StepType::Cli => serde_json::from_value(self.with.clone())
                .ok()
                .map(|with| StepWith::Cli { with }),
            StepType::Subflow => serde_json::from_value(self.with.clone())
                .ok()
                .map(|with| StepWith::Subflow { with }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowInput {
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub required: bool,
    pub description: Option<String>,
    pub default: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FlowDefaults {
    pub cli: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlowDefinition {
    pub name: String,
    pub version: String,
    pub description: Option<String>,

    #[serde(default)]
    pub inputs: IndexMap<String, FlowInput>,
    #[serde(default)]
    pub outputs: IndexMap<String, String>,
    #[serde(default)]
    pub defaults: FlowDefaults,

    pub timeout: Option<String>,
    #[serde(default)]
    pub lifecycle: LifecycleHooks,
    #[serde(default)]
    pub metadata: IndexMap<String, serde_json::Value>,
    pub flowspec_version: Option<String>,

    pub steps: Vec<Step>,
}

impl FlowDefinition {
    pub fn step(&self, id: &str) -> Option<&Step> {
        self.steps.iter().find(|s| s.id == id)
    }
}

/// Top-level file shape: either a single `flow:` or an array under `flows:`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FlowFile {
    Single { flow: Box<FlowDefinition> },
    Multiple { flows: Vec<SingleFlowEntry> },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SingleFlowEntry {
    pub flow: FlowDefinition,
}

impl FlowFile {
    pub fn into_definitions(self) -> Vec<FlowDefinition> {
        match self {
            FlowFile::Single { flow } => vec![*flow],
            FlowFile::Multiple { flows } => flows.into_iter().map(|e| e.flow).collect(),
        }
    }
}
