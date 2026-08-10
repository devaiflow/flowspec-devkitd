//! Live devkitd chain: create-feature -> containers-up -> opencode -> claude.
//! `#[ignore]`: has real side effects (git worktree, docker containers, two
//! agent runs) — see `docs/devkitd-dev.md`. No `clean-feature`; the worktree
//! and containers are left up on purpose so `summary.md`/`blog.md` can be
//! inspected afterward.
//!
//! ```bash
//! cd ~/work/projects/devaiflow/devkitd && cargo run &   # once, separately
//! just chain
//! ```

use flowspec_app::ports::{FlowSource, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::use_cases::{start_flow, start_flow::StartFlowRequest};
use flowspec_server::devkitd::{DevkitdClient, DevkitdClientConfig};
use flowspec_server::flows::FsFlowSource;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const WORKTREE: &str = "/workspaces/projects/pro-rails/feat-testing-x";

fn devkitd_url() -> String {
    std::env::var("FLOWSPEC_TRACER_DEVKITD_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9000/mcp".to_string())
}

fn flows_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../flows-fixtures")
}

fn scheduler_config() -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1,
        deadline_margin_secs: 30,
        default_step_timeout_secs: 1800,
        max_step_output_kb: 256,
        executor_cli_tool: "agent-run".to_string(),
    }
}

#[tokio::test]
#[ignore = "real side effects: worktree, containers, two live agent runs; see docs/devkitd-dev.md"]
async fn devkit_chain_produces_summary_and_blog() {
    let db_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StateStore> = Arc::new(
        flowspec_server::state::SqliteStore::open(db_dir.path().join("flowspec.db")).unwrap(),
    );

    let mut devkitd_config = DevkitdClientConfig::new(devkitd_url());
    devkitd_config.poll_interval = Duration::from_secs(2);
    let devkitd = Arc::new(DevkitdClient::new(devkitd_config));

    let flow_source: Arc<dyn FlowSource> =
        Arc::new(FsFlowSource::load(&flows_dir()).expect("flows-fixtures must all be valid"));

    let scheduler = Arc::new(Scheduler::new(
        store.clone(),
        devkitd.clone(),
        scheduler_config(),
    ));

    let run_id = start_flow::start_flow(
        store.clone(),
        scheduler.clone(),
        flow_source.clone(),
        &scheduler_config(),
        StartFlowRequest {
            flow_name: "devkit-chain".to_string(),
            version_req: None,
            inputs: Value::Null,
            trigger: Value::Null,
            idempotency_key: None,
        },
    )
    .await
    .unwrap()
    .run_id;

    // Generous: worktree + containers provisioning + two full agent runs.
    let record = timeout(Duration::from_secs(30 * 60), async {
        loop {
            let record = store.load_run(&run_id).await.unwrap();
            if record.phase != flowspec_domain::run::types::RunPhase::Running {
                return record;
            }
            sleep(Duration::from_secs(2)).await;
        }
    })
    .await
    .expect("devkit-chain did not finish within 30 minutes — is devkitd running?");

    assert_eq!(
        record.phase,
        flowspec_domain::run::types::RunPhase::Completed,
        "devkit-chain should succeed; steps: {:#?}",
        record.steps
    );

    let summary = PathBuf::from(WORKTREE).join("summary.md");
    let blog = PathBuf::from(WORKTREE).join("blog.md");
    assert!(
        summary.exists(),
        "expected {} to exist after the opencode step",
        summary.display()
    );
    assert!(
        blog.exists(),
        "expected {} to exist after the claude step",
        blog.display()
    );

    eprintln!(
        "devkit-chain: run {run_id} completed; summary.md={} bytes, blog.md={} bytes",
        std::fs::metadata(&summary).map(|m| m.len()).unwrap_or(0),
        std::fs::metadata(&blog).map(|m| m.len()).unwrap_or(0),
    );
}
