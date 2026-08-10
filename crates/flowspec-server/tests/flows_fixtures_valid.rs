//! Regression guard: every top-level fixture in `flows-fixtures/` must load
//! and validate cleanly through the same `FsFlowSource` a real deployment
//! uses (the `invalid/` subdirectory is intentionally excluded — subdirs are
//! ignored by design, see `fs_flow_source.rs::subdirectories_are_ignored`).

use flowspec_app::ports::FlowSource;
use flowspec_server::flows::FsFlowSource;

#[tokio::test]
async fn all_top_level_fixtures_load_and_validate() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../flows-fixtures");
    let source = FsFlowSource::load(&dir).expect("all top-level fixtures must validate cleanly");
    let flows = source.list().await;
    assert!(!flows.is_empty(), "expected at least one fixture to load");
}
