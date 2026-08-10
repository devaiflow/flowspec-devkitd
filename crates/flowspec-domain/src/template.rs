use crate::flow::types::FlowDefinition;
use crate::run::types::RunState;
use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum TemplateError {
    #[error("unresolvable reference '{reference}' in step '{step}'")]
    UnresolvableReference { reference: String, step: String },
    #[error("step '{step}' output is unstructured text; cannot resolve '{reference}' into it")]
    UnstructuredOutput { reference: String, step: String },
    #[error("unterminated template expression in step '{step}': missing closing }}}}")]
    Unterminated { step: String },
}

#[derive(Debug, Clone, Default)]
pub struct StepContext {
    pub output: Option<Value>,
    pub feedback: Option<Value>,
    pub approval_comment: Option<Value>,
    /// Subflow audit metadata (`steps.<id>.run.*`).
    pub run: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct TemplateContext {
    pub inputs: Value,
    pub trigger: Value,
    /// Allow-listed runtime environment variables only.
    pub env: Value,
    pub steps: HashMap<String, StepContext>,
    /// Self-referential run metadata (`run.id`, `run.failed_step_id`, ...).
    pub run: Value,
}

/// Build the template context for a run from the snapshotted flow definition and
/// the current run state. `env` is the allow-listed runtime environment map.
pub fn build_context(flow: &FlowDefinition, state: &RunState, env: Value) -> TemplateContext {
    let mut ctx = TemplateContext {
        inputs: state.inputs.clone(),
        trigger: state.trigger.clone(),
        env,
        steps: Default::default(),
        run: serde_json::Value::Null,
    };
    for step in &flow.steps {
        if let Some(run_step) = state.step(&step.id) {
            ctx.steps.insert(
                step.id.clone(),
                StepContext {
                    output: run_step.output.clone(),
                    feedback: run_step.feedback.clone().map(serde_json::Value::String),
                    approval_comment: run_step
                        .approval_comment
                        .clone()
                        .map(serde_json::Value::String),
                    run: None,
                },
            );
        }
    }
    ctx
}

/// Recursively resolve `{{ ... }}` spans inside string values of a JSON value.
/// Object keys are left untouched; only string values are resolved.
pub fn resolve_value(
    value: &Value,
    ctx: &TemplateContext,
    step: &str,
) -> Result<Value, TemplateError> {
    match value {
        Value::String(s) => Ok(Value::String(resolve(s, ctx, step)?)),
        Value::Array(items) => Ok(Value::Array(
            items
                .iter()
                .map(|v| resolve_value(v, ctx, step))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(map) => Ok(Value::Object(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), resolve_value(v, ctx, step)?)))
                .collect::<Result<serde_json::Map<String, Value>, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

/// Resolves every `{{ ... }}` span in `text` against `ctx`. `step` names the step
/// being resolved, for error messages. Resolution is total: every span either
/// resolves or the whole call errors -- no silent pass-through of unresolved text.
pub fn resolve(text: &str, ctx: &TemplateContext, step: &str) -> Result<String, TemplateError> {
    // A template that is exactly one span (nothing else around it) returns the
    // raw scalar value (unquoted string) instead of a JSON-embedded fragment.
    if let Some(expr) = whole_span(text) {
        let value = resolve_path(expr, ctx, step)?;
        return Ok(render_value(&value));
    }

    let mut out = String::new();
    let mut rest = text;

    loop {
        let Some(start) = rest.find("{{") else {
            out.push_str(rest);
            break;
        };
        let (literal, after_open) = rest.split_at(start);
        out.push_str(literal);
        let after_open = &after_open[2..];

        let Some(end) = after_open.find("}}") else {
            return Err(TemplateError::Unterminated {
                step: step.to_string(),
            });
        };
        let expr = after_open[..end].trim();

        let value = resolve_path(expr, ctx, step)?;
        out.push_str(&render_value(&value));
        rest = &after_open[end + 2..];
    }

    Ok(out)
}

/// If `text` (once trimmed) is exactly one `{{ ... }}` span with nothing else
/// around it, returns the inner expression.
fn whole_span(text: &str) -> Option<&str> {
    let trimmed = text.trim();
    let inner = trimmed.strip_prefix("{{")?.strip_suffix("}}")?;
    if inner.contains("{{") || inner.contains("}}") {
        return None;
    }
    Some(inner.trim())
}

fn render_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn resolve_path(expr: &str, ctx: &TemplateContext, step: &str) -> Result<Value, TemplateError> {
    let parts: Vec<&str> = expr.split('.').collect();
    let err = || TemplateError::UnresolvableReference {
        reference: expr.to_string(),
        step: step.to_string(),
    };

    match parts.as_slice() {
        ["inputs", rest @ ..] => dot_get(&ctx.inputs, rest).ok_or_else(err).cloned(),
        ["trigger", rest @ ..] => dot_get(&ctx.trigger, rest).ok_or_else(err).cloned(),
        ["env", rest @ ..] => dot_get(&ctx.env, rest).ok_or_else(err).cloned(),
        ["run", rest @ ..] => dot_get(&ctx.run, rest).ok_or_else(err).cloned(),
        ["steps", id, field, rest @ ..] => {
            let step_ctx = ctx.steps.get(*id).ok_or_else(err)?;
            match *field {
                "output" => {
                    let output = step_ctx.output.as_ref().ok_or_else(err)?;
                    if rest.is_empty() {
                        return Ok(output.clone());
                    }
                    match output {
                        Value::String(_) => Err(TemplateError::UnstructuredOutput {
                            reference: expr.to_string(),
                            step: step.to_string(),
                        }),
                        _ => dot_get(output, rest).ok_or_else(err).cloned(),
                    }
                }
                "feedback" => {
                    if !rest.is_empty() {
                        return Err(err());
                    }
                    step_ctx.feedback.clone().ok_or_else(err)
                }
                "approval_comment" => {
                    if !rest.is_empty() {
                        return Err(err());
                    }
                    step_ctx.approval_comment.clone().ok_or_else(err)
                }
                "run" => {
                    let run = step_ctx.run.as_ref().ok_or_else(err)?;
                    if rest.is_empty() {
                        Ok(run.clone())
                    } else {
                        dot_get(run, rest).ok_or_else(err).cloned()
                    }
                }
                _ => Err(err()),
            }
        }
        _ => Err(err()),
    }
}

fn dot_get<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.as_object()?.get(*key)?;
    }
    Some(current)
}
