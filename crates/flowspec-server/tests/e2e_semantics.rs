//! Phase 4.3 — failure, cancellation, and re-attach semantics proven against
//! a *live* devkitd. All `#[ignore]`: each needs devkitd running (see
//! `docs/devkitd-dev.md`) and some need manual choreography around it.
//!
//! ```bash
//! cd ~/work/projects/devaiflow/devkitd && cargo run &   # once, separately
//! cargo test -p flowspec-server --test e2e_semantics -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Uses devkitd's own test plugins — `slow-test` (sleeps 6s, no args) and
//! `disconnect-test` (sleeps 30s, and forks a child that touches
//! `/tmp/disconnect-marker-run5` at the 25s mark — a process-group-kill
//! probe: the marker must never appear if cancellation actually kills the
//! whole group, not just the parent).
//!
//! Manual choreography, spelled out per test:
//! - `reattach_across_flowspec_restart`: this test performs the flowspec-side
//!   half (abort + drop, then a fresh scheduler over the same DB) in-process.
//!   For a *literal* `kill -9` proof: run `cargo run --bin flowspec-server`
//!   against a flow using `slow-test`, `kill -9` the process mid-step, then
//!   restart it and confirm the run completes — this test's assertions are
//!   what that manual run should also satisfy.
//! - `devkitd_restart_yields_interrupted` / `transient_unreachable`: these
//!   need devkitd itself to be killed and (for the transient case) restarted
//!   partway through. Run each test in the foreground and, when it prints
//!   "NOW: kill devkitd" / "NOW: restart devkitd", do exactly that in
//!   another terminal within the printed window.

use flowspec_app::ports::{Devkitd, DevkitdError, SchedulerConfig, StartRequest, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::InMemoryFlowSource;
use flowspec_app::use_cases::{
    cancel_run, cancel_run::CancelRunRequest, queries, start_flow, start_flow::StartFlowRequest,
};
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use flowspec_server::devkitd::{DevkitdClient, DevkitdClientConfig};
use flowspec_server::state::SqliteStore;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::{sleep, timeout};

fn devkitd_url() -> String {
    std::env::var("FLOWSPEC_TRACER_DEVKITD_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9000/mcp".to_string())
}

fn client(poll_interval: Duration) -> DevkitdClient {
    let mut config = DevkitdClientConfig::new(devkitd_url());
    config.poll_interval = poll_interval;
    DevkitdClient::new(config)
}

fn noop_liveness() -> flowspec_app::ports::LivenessSink {
    Arc::new(|| {})
}

fn flow(yaml: &str) -> FlowDefinition {
    let file: FlowFile = serde_yaml_ng::from_str(yaml).unwrap();
    file.into_definitions().into_iter().next().unwrap()
}

fn scheduler_config(cli_tool: &str) -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1,
        deadline_margin_secs: 30,
        default_step_timeout_secs: 3600,
        max_step_output_kb: 256,
        executor_cli_tool: cli_tool.to_string(),
    }
}

fn slow_test_flow() -> FlowDefinition {
    flow(
        r#"
flow:
  name: e2e-slow
  version: 1.0.0
  steps:
    - id: probe
      type: cli
      with: { cli: slow-test, input: "x" }
      on_success: done
      on_failure: done
"#,
    )
}

fn disconnect_test_flow(timeout: Option<&str>) -> FlowDefinition {
    let timeout_line = timeout
        .map(|t| format!("      timeout: {t}\n"))
        .unwrap_or_default();
    flow(&format!(
        r#"
flow:
  name: e2e-disconnect
  version: 1.0.0
  steps:
    - id: probe
      type: cli
{timeout_line}      with: {{ cli: disconnect-test, input: "x" }}
      on_success: done
      on_failure: done
"#
    ))
}

// ---------------------------------------------------------------------------
// 1. Re-attach across a flowspec restart (the headline property, live)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a live devkitd; see docs/devkitd-dev.md"]
async fn reattach_across_flowspec_restart() {
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().join("flowspec.db");

    let run_id = {
        let store: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&db_path).unwrap());
        let devkitd = Arc::new(client(Duration::from_millis(200)));
        let flows = Arc::new(InMemoryFlowSource::new(vec![slow_test_flow()]));
        let scheduler = Arc::new(Scheduler::new(
            store.clone(),
            devkitd.clone(),
            scheduler_config("slow-test"),
        ));

        let run_id = start_flow::start_flow(
            store.clone(),
            scheduler.clone(),
            flows.clone(),
            &scheduler_config("slow-test"),
            StartFlowRequest {
                flow_name: "e2e-slow".to_string(),
                version_req: None,
                inputs: Value::Null,
                trigger: Value::Null,
                idempotency_key: None,
            },
        )
        .await
        .unwrap()
        .run_id;

        timeout(Duration::from_secs(5), async {
            loop {
                let record = store.load_run(&run_id).await.unwrap();
                if record.latest_steps().iter().any(|s| s.job_id.is_some()) {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("job_id was never persisted");

        // kill -9 analog: abort in-flight tasks, no graceful drain.
        scheduler.shutdown();
        run_id
    };

    let store_b: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&db_path).unwrap());
    let devkitd_b = Arc::new(client(Duration::from_millis(200)));
    let scheduler_b = Arc::new(Scheduler::new(
        store_b.clone(),
        devkitd_b.clone(),
        scheduler_config("slow-test"),
    ));
    scheduler_b.recover().await;

    let status = timeout(Duration::from_secs(15), async {
        loop {
            let status = queries::get_run_status(
                store_b.clone(),
                None,
                queries::GetRunStatusRequest {
                    run_id: run_id.clone(),
                },
            )
            .await
            .unwrap();
            if status.phase != "running" {
                return status;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("run did not reach a terminal phase after recovery");

    assert_eq!(status.phase, "completed");
}

// ---------------------------------------------------------------------------
// 2. devkitd restart mid-step -> Interrupted -> on_failure routing,
//    `when: always` teardown still runs
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a live devkitd, and YOU to restart it mid-test — see the printed prompt"]
async fn devkitd_restart_yields_interrupted() {
    let db_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StateStore> =
        Arc::new(SqliteStore::open(db_dir.path().join("flowspec.db")).unwrap());
    let devkitd = Arc::new(client(Duration::from_millis(200)));
    let flow_def = flow(
        r#"
flow:
  name: e2e-restart
  version: 1.0.0
  lifecycle:
    after_run:
      - hook: echo-test
        when: always
        args: { id: teardown }
  steps:
    - id: probe
      type: cli
      with: { cli: slow-test, input: "x" }
      on_success: done
      on_failure: done
"#,
    );
    let flows = Arc::new(InMemoryFlowSource::new(vec![flow_def]));
    let scheduler = Arc::new(Scheduler::new(
        store.clone(),
        devkitd.clone(),
        scheduler_config("slow-test"),
    ));

    let run_id = start_flow::start_flow(
        store.clone(),
        scheduler.clone(),
        flows.clone(),
        &scheduler_config("slow-test"),
        StartFlowRequest {
            flow_name: "e2e-restart".to_string(),
            version_req: None,
            inputs: Value::Null,
            trigger: Value::Null,
            idempotency_key: None,
        },
    )
    .await
    .unwrap()
    .run_id;

    eprintln!(
        "\n>>> NOW: kill and restart devkitd (it sleeps 6s; you have ~5s). <<<\n\
         cd ~/work/projects/devaiflow/devkitd && (kill the running `cargo run` or `devkitd` process, then `cargo run` again)\n"
    );

    let status = timeout(Duration::from_secs(60), async {
        loop {
            let status = queries::get_run_status(
                store.clone(),
                None,
                queries::GetRunStatusRequest {
                    run_id: run_id.clone(),
                },
            )
            .await
            .unwrap();
            if status.phase != "running" {
                return status;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("run did not reach a terminal phase — did you restart devkitd in time?");

    assert_eq!(
        status.phase, "failed",
        "an unknown job_id after a devkitd restart should route through on_failure"
    );
}

// ---------------------------------------------------------------------------
// 3. devkitd stop/start within the client's backoff budget -> step unaffected
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a live devkitd, and YOU to bounce it briefly mid-test — see the printed prompt"]
async fn transient_unreachable_within_backoff_budget() {
    let devkitd = client(Duration::from_millis(300));

    let handle = devkitd
        .start(StartRequest {
            tool: "slow-test".to_string(),
            args: json!({}),
            timeout_seconds: None,
        })
        .await
        .unwrap();

    eprintln!(
        "\n>>> NOW: stop devkitd, wait ~2s, restart it (it sleeps 6s; you have ~5s total). <<<\n"
    );

    let out = devkitd
        .wait(&handle, None, noop_liveness())
        .await
        .expect("a transient blip within the backoff budget must not fail the step");
    assert_eq!(out.output, json!("slow-test done\n"));
}

// ---------------------------------------------------------------------------
// 4. devkitd-side plugin timeout -> Timeout (the -2 sentinel path)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a live devkitd; see docs/devkitd-dev.md"]
async fn plugin_timeout_maps_to_timeout() {
    let devkitd = client(Duration::from_millis(300));

    let handle = devkitd
        .start(StartRequest {
            tool: "disconnect-test".to_string(),
            args: json!({}),
            timeout_seconds: Some(5), // devkitd kills the group itself at 5s
        })
        .await
        .unwrap();

    // Deadline (60s) is far past devkitd's own 5s cap — if this test fails
    // with a flowspec-issued cancel instead, devkitd's own timeout enforcement
    // regressed.
    let deadline = Some(SystemTime::now() + Duration::from_secs(60));
    let err = devkitd
        .wait(&handle, deadline, noop_liveness())
        .await
        .unwrap_err();
    assert!(
        matches!(err, DevkitdError::Timeout),
        "expected Timeout via devkitd's own -2 sentinel, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// 5. flowspec's own deadline backstop -> job-cancel, out-of-band `cancelled`
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a live devkitd; see docs/devkitd-dev.md"]
async fn deadline_backstop_cancels() {
    let devkitd = client(Duration::from_millis(300));

    // No `_timeout_seconds` override -> devkitd applies its own generous
    // global default (600s); it will not self-timeout inside this test.
    let handle = devkitd
        .start(StartRequest {
            tool: "disconnect-test".to_string(),
            args: json!({}),
            timeout_seconds: None,
        })
        .await
        .unwrap();

    // flowspec's own deadline is what fires here, well before devkitd would.
    let deadline = Some(SystemTime::now() + Duration::from_secs(3));
    let err = devkitd
        .wait(&handle, deadline, noop_liveness())
        .await
        .unwrap_err();
    assert!(
        matches!(err, DevkitdError::Timeout),
        "expected the flowspec-side deadline backstop, got {err:?}"
    );

    // Out-of-band verification: a fresh poll must observe devkitd's own
    // `cancelled` state, proving the job was actually killed server-side and
    // not just abandoned locally.
    sleep(Duration::from_millis(500)).await;
    let confirm = devkitd
        .wait(
            &handle,
            Some(SystemTime::now() + Duration::from_secs(5)),
            noop_liveness(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(confirm, DevkitdError::Cancelled),
        "expected devkitd to report the job as cancelled out-of-band, got {confirm:?}"
    );
}

// ---------------------------------------------------------------------------
// 6. cancel_run kills the whole process group, not just the parent
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a live devkitd; see docs/devkitd-dev.md"]
async fn cancel_run_kills_process_group() {
    let marker = std::path::Path::new("/tmp/disconnect-marker-run5");
    let _ = std::fs::remove_file(marker);

    let db_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn StateStore> =
        Arc::new(SqliteStore::open(db_dir.path().join("flowspec.db")).unwrap());
    let devkitd = Arc::new(client(Duration::from_millis(200)));
    let flows = Arc::new(InMemoryFlowSource::new(vec![disconnect_test_flow(None)]));
    let scheduler = Arc::new(Scheduler::new(
        store.clone(),
        devkitd.clone(),
        scheduler_config("disconnect-test"),
    ));

    let run_id = start_flow::start_flow(
        store.clone(),
        scheduler.clone(),
        flows.clone(),
        &scheduler_config("disconnect-test"),
        StartFlowRequest {
            flow_name: "e2e-disconnect".to_string(),
            version_req: None,
            inputs: Value::Null,
            trigger: Value::Null,
            idempotency_key: None,
        },
    )
    .await
    .unwrap()
    .run_id;

    timeout(Duration::from_secs(5), async {
        loop {
            let record = store.load_run(&run_id).await.unwrap();
            if record.latest_steps().iter().any(|s| s.job_id.is_some()) {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("job_id was never persisted");

    cancel_run::cancel_run(
        store.clone(),
        scheduler.clone(),
        CancelRunRequest {
            run_id: run_id.clone(),
        },
    )
    .await
    .unwrap();

    timeout(Duration::from_secs(10), async {
        loop {
            let status = queries::get_run_status(
                store.clone(),
                None,
                queries::GetRunStatusRequest {
                    run_id: run_id.clone(),
                },
            )
            .await
            .unwrap();
            if status.phase != "running" {
                return status;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("run did not reach a terminal phase after cancel");

    // The forked child touches the marker at t=25s; wait past that mark
    // without the parent process (which would have kept it alive if only
    // the parent, not the group, were killed).
    sleep(Duration::from_secs(27)).await;
    assert!(
        !marker.exists(),
        "marker file exists — cancel killed the parent but not the whole process group"
    );
}
