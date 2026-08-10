use flowspec_domain::template::{StepContext, TemplateContext, TemplateError, resolve};
use serde_json::json;

fn ctx() -> TemplateContext {
    let mut steps = std::collections::HashMap::new();
    steps.insert(
        "plan".to_string(),
        StepContext {
            output: Some(json!("PLAN.md")),
            feedback: Some(json!("needs more detail")),
            approval_comment: Some(json!("lgtm")),
            run: None,
        },
    );
    steps.insert(
        "deploy".to_string(),
        StepContext {
            output: Some(json!({ "id": "dep_123", "url": "https://example.com" })),
            feedback: None,
            approval_comment: None,
            run: Some(json!({ "run_id": "run_abc", "status": "completed" })),
        },
    );
    TemplateContext {
        inputs: json!({ "message": "add stripe" }),
        trigger: json!({ "user_id": "u1" }),
        env: json!({ "PROJECT_NAME": "flowspec" }),
        steps,
        run: json!({ "id": "run_xyz" }),
    }
}

#[test]
fn resolves_inputs_and_trigger_and_env_and_run() {
    assert_eq!(
        resolve("{{ inputs.message }}", &ctx(), "s").unwrap(),
        "add stripe"
    );
    assert_eq!(resolve("{{ trigger.user_id }}", &ctx(), "s").unwrap(), "u1");
    assert_eq!(
        resolve("{{ env.PROJECT_NAME }}", &ctx(), "s").unwrap(),
        "flowspec"
    );
    assert_eq!(resolve("{{ run.id }}", &ctx(), "s").unwrap(), "run_xyz");
}

#[test]
fn resolves_step_output_feedback_and_approval_comment() {
    assert_eq!(
        resolve("{{ steps.plan.output }}", &ctx(), "s").unwrap(),
        "PLAN.md"
    );
    assert_eq!(
        resolve("{{ steps.plan.feedback }}", &ctx(), "s").unwrap(),
        "needs more detail"
    );
    assert_eq!(
        resolve("{{ steps.plan.approval_comment }}", &ctx(), "s").unwrap(),
        "lgtm"
    );
}

#[test]
fn resolves_structured_output_dot_path() {
    assert_eq!(
        resolve("{{ steps.deploy.output.id }}", &ctx(), "s").unwrap(),
        "dep_123"
    );
    assert_eq!(
        resolve("{{ steps.deploy.output.url }}", &ctx(), "s").unwrap(),
        "https://example.com"
    );
}

#[test]
fn resolves_subflow_run_metadata_namespace() {
    assert_eq!(
        resolve("{{ steps.deploy.run.run_id }}", &ctx(), "s").unwrap(),
        "run_abc"
    );
    assert_eq!(
        resolve("{{ steps.deploy.run.status }}", &ctx(), "s").unwrap(),
        "completed"
    );
}

#[test]
fn dotted_access_into_unstructured_string_output_errors_precisely() {
    let err = resolve("{{ steps.plan.output.field }}", &ctx(), "implement").unwrap_err();
    assert_eq!(
        err,
        TemplateError::UnstructuredOutput {
            reference: "steps.plan.output.field".to_string(),
            step: "implement".to_string(),
        }
    );
}

#[test]
fn unresolvable_reference_errors_instead_of_substituting_empty_string() {
    let err = resolve("{{ inputs.missing }}", &ctx(), "plan").unwrap_err();
    assert_eq!(
        err,
        TemplateError::UnresolvableReference {
            reference: "inputs.missing".to_string(),
            step: "plan".to_string(),
        }
    );

    let err2 = resolve("{{ steps.unknown-step.output }}", &ctx(), "plan").unwrap_err();
    assert_eq!(
        err2,
        TemplateError::UnresolvableReference {
            reference: "steps.unknown-step.output".to_string(),
            step: "plan".to_string(),
        }
    );
}

#[test]
fn mixed_literal_and_span_text_renders_inline() {
    let text = "Plan output: {{ steps.plan.output }} (say hi to {{ inputs.message }})";
    assert_eq!(
        resolve(text, &ctx(), "s").unwrap(),
        "Plan output: PLAN.md (say hi to add stripe)"
    );
}

#[test]
fn structured_value_embedded_in_mixed_text_is_json_encoded() {
    let text = "id={{ steps.deploy.output.id }} full={{ steps.deploy.output }}";
    let resolved = resolve(text, &ctx(), "s").unwrap();
    assert!(resolved.starts_with("id=dep_123 full="));
    assert!(resolved.contains("\"id\":\"dep_123\"") || resolved.contains("\"id\": \"dep_123\""));
}

#[test]
fn plain_text_with_no_templates_passes_through() {
    assert_eq!(
        resolve("just a plain string", &ctx(), "s").unwrap(),
        "just a plain string"
    );
}

#[test]
fn unterminated_span_errors() {
    let err = resolve("{{ inputs.message", &ctx(), "plan").unwrap_err();
    assert_eq!(
        err,
        TemplateError::Unterminated {
            step: "plan".to_string()
        }
    );
}
