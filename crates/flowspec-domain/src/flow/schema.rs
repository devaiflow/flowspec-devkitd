use super::types::FlowDefinition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldViolation {
    pub rule: &'static str,
    pub step: Option<String>,
    pub message: String,
}

fn is_valid_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Field-level constraints serde cannot express: non-empty step list, valid
/// identifier shapes, duration strings parseable. Structural rules (routing,
/// cycles, `needs:` consistency) live in `validate.rs`.
pub fn validate_fields(flow: &FlowDefinition) -> Vec<FieldViolation> {
    let mut violations = Vec::new();

    if flow.steps.is_empty() {
        violations.push(FieldViolation {
            rule: "non_empty_steps",
            step: None,
            message: "flow must declare at least one step".to_string(),
        });
    }

    for step in &flow.steps {
        if !is_valid_id(&step.id) {
            violations.push(FieldViolation {
                rule: "valid_step_id",
                step: Some(step.id.clone()),
                message: format!("step id '{}' must match [a-z0-9_-]+", step.id),
            });
        }

        if let Some(timeout) = &step.timeout
            && parse_duration(timeout).is_none()
        {
            violations.push(FieldViolation {
                rule: "valid_duration",
                step: Some(step.id.clone()),
                message: format!("step '{}' has invalid timeout '{}'", step.id, timeout),
            });
        }
    }

    if let Some(timeout) = &flow.timeout
        && parse_duration(timeout).is_none()
    {
        violations.push(FieldViolation {
            rule: "valid_duration",
            step: None,
            message: format!("flow timeout '{}' is invalid", timeout),
        });
    }

    violations
}

/// Parses simple duration strings like "10m", "1h", "45s". Returns seconds.
pub fn parse_duration(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, unit) = s.split_at(s.len() - 1);
    let n: u64 = num.parse().ok()?;
    match unit {
        "s" => Some(n),
        "m" => Some(n * 60),
        "h" => Some(n * 3600),
        _ => None,
    }
}
