//! Regression guard: every top-level fixture in `flows-fixtures/` must load
//! and validate cleanly through the same `FsFlowSource` a real deployment
//! uses (the `invalid/` subdirectory is intentionally excluded — subdirs are
//! ignored by design, see `fs_flow_source.rs::subdirectories_are_ignored`).

use flowspec_app::ports::FlowSource;
use flowspec_domain::flow::types::FlowFile;
use flowspec_domain::flow::validate;
use flowspec_server::flows::FsFlowSource;
use serde_json::json;

#[tokio::test]
async fn all_top_level_fixtures_load_and_validate() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../flows-fixtures");
    let source = FsFlowSource::load(&dir).expect("all top-level fixtures must validate cleanly");
    let flows = source.list().await;
    assert!(!flows.is_empty(), "expected at least one fixture to load");
}

/// A platform-authored `flow_doc` (JSON, not YAML) is the by-value start
/// path's actual input (`PLAN-LIVERUN-CONNECTED.md` Step 1). It must parse
/// and validate through exactly the same `FlowFile` + `validate::validate`
/// gate `FsFlowSource` uses -- `flow.metadata.ui` (positions/appearance),
/// `on_success: "done"`, and an auto-derived `needs:` are all things
/// `devaiflow-platform/src/lib/serialize.ts` actually emits.
#[tokio::test]
async fn platform_shaped_flow_doc_parses_and_validates() {
    let flow_doc = json!({
        "flow": {
            "name": "feature-development",
            "version": "1.0.0",
            "description": "Full feature lifecycle: plan, review, implement, test",
            "flowspec_version": "0.3",
            "inputs": {
                "message": { "type": "string", "required": true, "description": "Feature description" }
            },
            "metadata": {
                "ui": {
                    "positions": { "plan": { "x": 50, "y": 150 }, "implement": { "x": 350, "y": 150 } },
                    "appearance": {}
                }
            },
            "steps": [
                {
                    "id": "plan",
                    "type": "cli",
                    "with": { "cli": "gemini-cli", "input": "{{ inputs.message }}", "output": "PLAN.md" },
                    "on_success": "implement"
                },
                {
                    "id": "implement",
                    "type": "cli",
                    "with": { "cli": "claude-code", "input": "PLAN.md", "output": "worktree" },
                    "needs": ["plan"],
                    "on_success": "done"
                }
            ]
        }
    });

    let file: FlowFile = serde_json::from_value(flow_doc).expect("flow_doc must deserialize");
    let definition = file
        .into_definitions()
        .into_iter()
        .next()
        .expect("flow_doc must contain one flow");
    let violations = validate::validate(&definition);
    assert!(
        violations.is_empty(),
        "platform-shaped flow_doc must validate cleanly: {violations:?}"
    );
}

/// The builder can't currently produce this (every step form requires an
/// input), but a malformed or hand-edited `flow_doc` can. `with.input` is a
/// required field on `CliWith` (`flow/types.rs`), so an empty `with:` fails
/// `Step::kind()` and is caught by `type_with_consistency` -- this must be a
/// clean rejection, not a panic, since the connector maps it straight to
/// `DELETE /actions/:id` + `trigger_removed_by_runtime`.
#[tokio::test]
async fn flow_doc_with_missing_step_input_is_rejected_not_panicking() {
    let flow_doc = json!({
        "flow": {
            "name": "broken",
            "version": "1.0.0",
            "steps": [
                { "id": "plan", "type": "cli", "with": { "cli": "gemini-cli" } }
            ]
        }
    });

    let file: FlowFile = serde_json::from_value(flow_doc).expect("flow_doc must deserialize");
    let definition = file.into_definitions().into_iter().next().unwrap();
    let violations = validate::validate(&definition);
    assert!(
        !violations.is_empty(),
        "a cli step with no input must fail validation, not silently pass"
    );
}
