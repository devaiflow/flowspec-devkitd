//! Wire-format helpers for MCP tool responses: human/LLM-readable timestamps
//! and bounded output summaries. Kept in `flowspec-app` so the use-case
//! response structs can use them directly — no translation layer in the
//! server crate.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::SystemTime;

#[cfg(feature = "schema")]
use schemars::JsonSchema;

/// A `SystemTime` that serializes as an RFC3339 string
/// (`"2026-08-08T12:00:00Z"`) instead of serde's default
/// `{secs_since_epoch, nanos}` object, which an LLM host cannot read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(pub SystemTime);

impl From<SystemTime> for Timestamp {
    fn from(t: SystemTime) -> Self {
        Timestamp(t)
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let odt = time::OffsetDateTime::from(self.0);
        let text = odt
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        let odt =
            time::OffsetDateTime::parse(&text, &time::format_description::well_known::Rfc3339)
                .map_err(serde::de::Error::custom)?;
        Ok(Timestamp(odt.into()))
    }
}

#[cfg(feature = "schema")]
impl JsonSchema for Timestamp {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Timestamp".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "format": "date-time",
            "description": "RFC3339 timestamp, e.g. \"2026-08-08T12:00:00Z\"."
        })
    }
}

/// A size-bounded preview of a step's output, for status responses that must
/// never risk poisoning the host's context with megabytes of JSON. Full
/// content is available via `get_step_output`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct OutputSummary {
    /// The output, rendered as text and truncated to the summary budget.
    pub text: String,
    /// Size of the full (untruncated) output, in bytes.
    pub bytes: usize,
    /// Whether `text` was truncated. When true, call `get_step_output` for
    /// the complete value.
    pub truncated: bool,
}

/// Structured failure detail for a step or hook job, shared by both --
/// steps and hooks execute through the same devkitd job interface (see
/// `Devkitd` in `ports.rs`), and a caller triaging a failure needs the same
/// three facts (exit code, stdout, stderr) regardless of which one failed.
/// `failure_reason`/`failure_summary` fields elsewhere stay the flattened,
/// human-readable line; this is its machine-readable sibling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
pub struct JobFailure {
    /// tool_error | timeout | interrupted | cancelled | unreachable
    pub kind: String,
    /// Only present for `tool_error`.
    pub exit_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

impl From<&crate::ports::DevkitdError> for JobFailure {
    fn from(e: &crate::ports::DevkitdError) -> Self {
        use crate::ports::DevkitdError;
        match e {
            DevkitdError::ToolError {
                stdout,
                stderr,
                exit_code,
            } => JobFailure {
                kind: "tool_error".to_string(),
                exit_code: Some(*exit_code),
                stdout: Some(stdout.clone()),
                stderr: Some(stderr.clone()),
            },
            DevkitdError::Timeout => JobFailure {
                kind: "timeout".to_string(),
                exit_code: None,
                stdout: None,
                stderr: None,
            },
            DevkitdError::Interrupted => JobFailure {
                kind: "interrupted".to_string(),
                exit_code: None,
                stdout: None,
                stderr: None,
            },
            DevkitdError::Cancelled => JobFailure {
                kind: "cancelled".to_string(),
                exit_code: None,
                stdout: None,
                stderr: None,
            },
            DevkitdError::Unreachable => JobFailure {
                kind: "unreachable".to_string(),
                exit_code: None,
                stdout: None,
                stderr: None,
            },
        }
    }
}

/// Render `value` as text and truncate to `max_chars`, char-boundary-safe.
/// `bytes` reports the size of the full untruncated rendering.
pub fn summarize_output(value: &Value, max_chars: usize) -> OutputSummary {
    let text = match value {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    let bytes = text.len();
    if text.chars().count() <= max_chars {
        OutputSummary {
            text,
            bytes,
            truncated: false,
        }
    } else {
        let mut truncated: String = text.chars().take(max_chars).collect();
        truncated.push_str(&format!(
            "…[truncated, {bytes} bytes total; use get_step_output]"
        ));
        OutputSummary {
            text: truncated,
            bytes,
            truncated: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_roundtrips_through_rfc3339() {
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_754_640_000);
        let ts = Timestamp(now);
        let json = serde_json::to_string(&ts).unwrap();
        assert!(json.starts_with('"') && json.contains('T') && json.contains('Z'));
        let back: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(back.0, now);
    }

    #[test]
    fn summarize_output_passes_short_text_through() {
        let v = Value::String("hello".into());
        let s = summarize_output(&v, 500);
        assert_eq!(s.text, "hello");
        assert!(!s.truncated);
        assert_eq!(s.bytes, 5);
    }

    #[test]
    fn summarize_output_truncates_at_char_boundary_with_marker() {
        let v = Value::String("x".repeat(1000));
        let s = summarize_output(&v, 10);
        assert!(s.truncated);
        assert!(s.text.starts_with("xxxxxxxxxx"));
        assert!(s.text.contains("truncated"));
        assert_eq!(s.bytes, 1000);
    }

    #[test]
    fn summarize_output_serializes_structured_values_as_json_text() {
        let v = serde_json::json!({"a": 1});
        let s = summarize_output(&v, 500);
        assert_eq!(s.text, r#"{"a":1}"#);
    }

    #[test]
    fn job_failure_from_tool_error_keeps_exit_code_stdout_stderr() {
        let e = crate::ports::DevkitdError::ToolError {
            stdout: "".to_string(),
            stderr: "PROVISIONER_PASSWORD env var is not set".to_string(),
            exit_code: 1,
        };
        let f = JobFailure::from(&e);
        assert_eq!(f.kind, "tool_error");
        assert_eq!(f.exit_code, Some(1));
        assert_eq!(
            f.stderr.as_deref(),
            Some("PROVISIONER_PASSWORD env var is not set")
        );
    }

    #[test]
    fn job_failure_from_non_tool_error_has_no_exit_code() {
        let f = JobFailure::from(&crate::ports::DevkitdError::Timeout);
        assert_eq!(f.kind, "timeout");
        assert_eq!(f.exit_code, None);
        assert_eq!(f.stdout, None);
        assert_eq!(f.stderr, None);
    }
}
