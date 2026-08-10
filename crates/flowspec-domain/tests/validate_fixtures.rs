use flowspec_domain::flow::types::FlowFile;
use flowspec_domain::flow::validate;
use std::path::PathBuf;

fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel)
}

fn load(rel: &str) -> FlowFile {
    let text = std::fs::read_to_string(fixture_path(rel)).expect("fixture read");
    serde_yaml_ng::from_str(&text).expect("fixture parses")
}

fn assert_valid(rel: &str) {
    let defs = load(rel).into_definitions();
    for def in defs {
        let violations = validate(&def);
        assert!(
            violations.is_empty(),
            "expected '{rel}' to be valid, got violations: {violations:?}"
        );
    }
}

fn assert_invalid(rel: &str) {
    let defs = load(rel).into_definitions();
    let all_violations: Vec<_> = defs.iter().flat_map(validate).collect();
    assert!(
        !all_violations.is_empty(),
        "expected '{rel}' to be rejected, but it validated cleanly"
    );
}

#[test]
fn linear_is_valid() {
    assert_valid("linear.yaml");
}

#[test]
fn human_loop_is_valid() {
    assert_valid("human-loop.yaml");
}

#[test]
fn fan_out_is_valid() {
    assert_valid("fan-out.yaml");
}

#[test]
fn failure_routing_is_valid() {
    assert_valid("failure-routing.yaml");
}

#[test]
fn subflow_parent_and_child_are_valid() {
    assert_valid("subflow-parent.yaml");
    assert_valid("subflow-child.yaml");
}

#[test]
fn cycle_is_rejected() {
    assert_invalid("invalid/cycle.yaml");
}

#[test]
fn dangling_needs_is_rejected() {
    assert_invalid("invalid/dangling-needs.yaml");
}

#[test]
fn cross_sibling_is_rejected() {
    assert_invalid("invalid/cross-sibling.yaml");
}

#[test]
fn self_recursive_is_rejected() {
    assert_invalid("invalid/self-recursive.yaml");
}

#[test]
fn unknown_field_fails_to_parse() {
    let text = std::fs::read_to_string(fixture_path("invalid/unknown-field.yaml")).unwrap();
    let result: Result<FlowFile, _> = serde_yaml_ng::from_str(&text);
    assert!(
        result.is_err(),
        "expected unknown-field.yaml to fail deserialization"
    );
}
