//! Filesystem-backed `FlowSource`: loads and validates flow definitions from
//! a directory of top-level `*.yaml`/`*.yml` files.

use async_trait::async_trait;
use flowspec_app::flow_source::select_flow;
use flowspec_app::ports::FlowSource;
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use flowspec_domain::flow::validate;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct FsFlowSourceError {
    pub path: PathBuf,
    pub detail: String,
}

impl fmt::Display for FsFlowSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.detail)
    }
}

impl std::error::Error for FsFlowSourceError {}

/// Loads flow definitions from the top level of `dir` (non-recursive).
/// Validation runs at construction — a malformed or invalid flow file fails
/// the load rather than surfacing later as a runtime error.
#[derive(Debug)]
pub struct FsFlowSource {
    flows: Vec<FlowDefinition>,
}

impl FsFlowSource {
    pub fn load(dir: &Path) -> Result<Self, FsFlowSourceError> {
        let mut flows = Vec::new();

        let entries = std::fs::read_dir(dir).map_err(|e| FsFlowSourceError {
            path: dir.to_path_buf(),
            detail: e.to_string(),
        })?;

        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| FsFlowSourceError {
                path: dir.to_path_buf(),
                detail: e.to_string(),
            })?;
            let path = entry.path();
            if !path.is_file() {
                continue; // subdirectories ignored
            }
            let is_yaml = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml"))
                .unwrap_or(false);
            if is_yaml {
                paths.push(path);
            }
        }
        paths.sort();

        for path in paths {
            let content = std::fs::read_to_string(&path).map_err(|e| FsFlowSourceError {
                path: path.clone(),
                detail: e.to_string(),
            })?;
            let file: FlowFile =
                serde_yaml_ng::from_str(&content).map_err(|e| FsFlowSourceError {
                    path: path.clone(),
                    detail: e.to_string(),
                })?;
            for def in file.into_definitions() {
                let violations = validate::validate(&def);
                if !violations.is_empty() {
                    let detail = violations
                        .iter()
                        .map(|v| format!("[{}] {}", v.rule, v.message))
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(FsFlowSourceError { path, detail });
                }
                flows.push(def);
            }
        }

        Ok(Self { flows })
    }
}

#[async_trait]
impl FlowSource for FsFlowSource {
    async fn list(&self) -> Vec<FlowDefinition> {
        self.flows.clone()
    }

    async fn get(&self, name: &str, version_req: Option<&str>) -> Option<FlowDefinition> {
        select_flow(&self.flows, name, version_req)
    }
}
