//! Phase 4 tracer bullet — first real contact with a live devkitd over the
//! real rmcp 0.16 client against devkitd's rmcp 2.1.0 server. `#[ignore]`:
//! requires devkitd running (see `docs/devkitd-dev.md`).
//!
//! ```bash
//! cd ~/work/projects/devaiflow/devkitd && cargo run &   # once, separately
//! just tracer
//! ```
//!
//! Env `FLOWSPEC_TRACER_DEVKITD_URL` overrides the endpoint (default
//! `http://127.0.0.1:9000/mcp`).

use flowspec_app::ports::{FlowSource, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::use_cases::{start_flow, start_flow::StartFlowRequest};
use flowspec_server::devkitd::{DevkitdClient, DevkitdClientConfig};
use flowspec_server::flows::FsFlowSource;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{sleep, timeout};

fn devkitd_url() -> String {
    std::env::var("FLOWSPEC_TRACER_DEVKITD_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9000/mcp".to_string())
}

fn flows_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../flows-fixtures")
}

fn scheduler_config() -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1, // unused by the scheduler; the adapter owns polling
        deadline_margin_secs: 30,
        default_step_timeout_secs: 3600,
        max_step_output_kb: 256,
        executor_cli_tool: "echo-test".to_string(),
    }
}

#[tokio::test]
#[ignore = "requires a live devkitd; see docs/devkitd-dev.md"]
async fn tracer_runs_against_live_devkitd() {
    let db_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StateStore> = Arc::new(
        flowspec_server::state::SqliteStore::open(db_dir.path().join("flowspec.db")).unwrap(),
    );

    let mut devkitd_config = DevkitdClientConfig::new(devkitd_url());
    devkitd_config.poll_interval = Duration::from_millis(200);
    let devkitd = Arc::new(DevkitdClient::new(devkitd_config));

    let flow_source: Arc<dyn FlowSource> =
        Arc::new(FsFlowSource::load(&flows_dir()).expect("flows-fixtures must all be valid"));

    let scheduler = Arc::new(Scheduler::new(
        store.clone(),
        devkitd.clone(),
        scheduler_config(),
    ));

    let started_at = std::time::Instant::now();
    let run_id = start_flow::start_flow(
        store.clone(),
        scheduler.clone(),
        flow_source.clone(),
        &scheduler_config(),
        StartFlowRequest {
            flow_name: "tracer".to_string(),
            version_req: None,
            inputs: json!({"message": "phase-4-tracer"}),
            trigger: Value::Null,
            idempotency_key: None,
        },
    )
    .await
    .unwrap()
    .run_id;

    // Sample concurrently for "job_id landed while the run was still
    // running" — the property this test exists to prove, not just assume.
    let job_id_seen_before_completion = Arc::new(AtomicBool::new(false));
    let sampler = {
        let store = store.clone();
        let run_id = run_id.clone();
        let flag = job_id_seen_before_completion.clone();
        tokio::spawn(async move {
            loop {
                let record = store.load_run(&run_id).await.unwrap();
                let has_job_id = record.latest_steps().iter().any(|s| s.job_id.is_some());
                if has_job_id {
                    flag.store(true, Ordering::SeqCst);
                }
                if record.phase != flowspec_domain::run::types::RunPhase::Running {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let record = timeout(Duration::from_secs(120), async {
        loop {
            let record = store.load_run(&run_id).await.unwrap();
            if record.phase != flowspec_domain::run::types::RunPhase::Running {
                return record;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("tracer run did not finish within 120s — is devkitd running?");

    let _ = sampler.await;
    let elapsed = started_at.elapsed();

    assert_eq!(
        record.phase,
        flowspec_domain::run::types::RunPhase::Completed,
        "tracer run should succeed; steps: {:#?}",
        record.steps
    );
    assert!(
        job_id_seen_before_completion.load(Ordering::SeqCst),
        "job_id must be persisted before the run completes (re-attach property)"
    );

    let probe = record
        .latest_steps()
        .into_iter()
        .find(|s| s.run.step_id == "probe")
        .expect("probe step must exist");
    assert_eq!(
        probe.run.status,
        flowspec_domain::run::types::StepStatus::Completed
    );
    let output = probe.run.output.clone().expect("probe must have output");
    let output_text = output.as_str().unwrap_or_default();
    assert!(
        output_text.contains("phase-4-tracer"),
        "expected the templated message in echo-test's stdout, got: {output_text:?}"
    );

    // Both lifecycle hooks ran to completion — queried directly since
    // `RunRecord` doesn't surface hook_runs (server-storage detail).
    let conn = rusqlite::Connection::open(db_dir.path().join("flowspec.db")).unwrap();
    let completed_hooks: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM hook_runs WHERE run_id = ?1 AND status = 'completed'",
            [&run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        completed_hooks, 2,
        "expected both before_run and after_run echo-test hooks to complete"
    );

    eprintln!("tracer: run {run_id} completed in {elapsed:?}, output={output_text:?}");
}
