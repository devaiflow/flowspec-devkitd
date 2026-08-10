//! `FsFlowSource` — construction-time validation, semver selection, and
//! filesystem edge cases (multi-flow files, subdirectories, invalid YAML).

use flowspec_app::ports::FlowSource;
use flowspec_server::flows::FsFlowSource;
use std::fs;

fn write(dir: &std::path::Path, name: &str, content: &str) {
    fs::write(dir.join(name), content).unwrap();
}

const SIMPLE_FLOW: &str = r#"
flow:
  name: simple
  version: 1.0.0
  inputs:
    message:
      type: string
      required: true
  steps:
    - id: only
      type: cli
      with:
        cli: claude-code
        input: "{{ inputs.message }}"
      on_success: done
"#;

#[tokio::test]
async fn loads_single_flow_file() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "simple.yaml", SIMPLE_FLOW);

    let source = FsFlowSource::load(dir.path()).unwrap();
    let flows = source.list().await;
    assert_eq!(flows.len(), 1);
    assert_eq!(flows[0].name, "simple");
}

#[tokio::test]
async fn loads_multi_flow_file() {
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "multi.yaml",
        r#"
flows:
  - flow:
      name: a
      version: 1.0.0
      inputs: {}
      steps:
        - id: only
          type: cli
          with: { cli: claude-code, input: "x" }
          on_success: done
  - flow:
      name: b
      version: 1.0.0
      inputs: {}
      steps:
        - id: only
          type: cli
          with: { cli: claude-code, input: "x" }
          on_success: done
"#,
    );

    let source = FsFlowSource::load(dir.path()).unwrap();
    let flows = source.list().await;
    assert_eq!(flows.len(), 2);
    let mut names: Vec<_> = flows.iter().map(|f| f.name.clone()).collect();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
}

#[tokio::test]
async fn highest_version_wins_and_version_req_filters() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "v1.yaml", SIMPLE_FLOW);
    write(
        dir.path(),
        "v2.yaml",
        &SIMPLE_FLOW.replace("1.0.0", "2.0.0"),
    );

    let source = FsFlowSource::load(dir.path()).unwrap();

    let latest = source.get("simple", None).await.unwrap();
    assert_eq!(latest.version, "2.0.0");

    let pinned = source.get("simple", Some("^1")).await.unwrap();
    assert_eq!(pinned.version, "1.0.0");

    assert!(source.get("simple", Some("^3")).await.is_none());
    assert!(source.get("missing", None).await.is_none());
}

#[tokio::test]
async fn invalid_yaml_fails_construction() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "broken.yaml", "flow: [this is not a flow");

    let err = FsFlowSource::load(dir.path()).unwrap_err();
    assert!(err.to_string().contains("broken.yaml"));
}

#[tokio::test]
async fn invalid_flow_fails_construction() {
    // Two entry steps with no routing between them -> not exactly one entry step.
    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path(),
        "invalid.yaml",
        r#"
flow:
  name: bad
  version: 1.0.0
  inputs: {}
  steps:
    - id: one
      type: cli
      with: { cli: claude-code, input: "x" }
      on_success: done
    - id: two
      type: cli
      with: { cli: claude-code, input: "x" }
      on_success: done
"#,
    );

    let err = FsFlowSource::load(dir.path()).unwrap_err();
    assert!(err.to_string().contains("invalid.yaml"));
}

#[tokio::test]
async fn subdirectories_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "simple.yaml", SIMPLE_FLOW);
    let nested = dir.path().join("nested");
    fs::create_dir(&nested).unwrap();
    write(&nested, "also.yaml", SIMPLE_FLOW);

    let source = FsFlowSource::load(dir.path()).unwrap();
    assert_eq!(source.list().await.len(), 1);
}
