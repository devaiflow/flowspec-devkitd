use flowspec_app::ports::{
    HookPhase, HookRunRecord, HookStatus, Mutation, NewRun, RunFilter, StateStore, StepRecord,
};
use flowspec_app::recovery::{RecoveryAction, RecoveryReason, recovery_plan};
use flowspec_domain::flow::types::{FlowDefinition, FlowFile};
use flowspec_domain::run::types::{RunPhase, StepRun, StepStatus};
use flowspec_server::state::SqliteStore;
use serde_json::json;
use std::path::PathBuf;
use std::time::SystemTime;

fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel)
}

fn load_flow(rel: &str) -> FlowDefinition {
    let text = std::fs::read_to_string(fixture_path(rel)).expect("fixture read");
    let file: FlowFile = serde_yaml_ng::from_str(&text).expect("fixture parses");
    file.into_definitions().into_iter().next().unwrap()
}

fn new_run_for(flow: &FlowDefinition, idem: Option<&str>) -> NewRun {
    NewRun {
        flow_name: flow.name.clone(),
        flow_version: flow.version.clone(),
        definition: flow.clone(),
        inputs: json!({"message": "test input"}),
        trigger: json!({"user": "ci"}),
        idempotency_key: idem.map(|s| s.into()),
    }
}

fn running_step(step_id: &str, attempt: u32, job_id: Option<&str>) -> StepRecord {
    StepRecord {
        run: StepRun {
            step_id: step_id.into(),
            status: StepStatus::Running,
            attempt,
            started_at: Some(SystemTime::now()),
            completed_at: None,
            input_resolved: Some("do it".into()),
            output: None,
            approval_status: None,
            feedback: None,
            approval_comment: None,
            failure_reason: None,
            child_run_id: None,
        },
        job_id: job_id.map(|s| s.into()),
        with_resolved: Some(json!({"cli": "claude-code"})),
        failure_detail: None,
    }
}

async fn round_trip_at_state(
    flow: &FlowDefinition,
    mutations: Vec<Mutation>,
    expected_recovery: impl FnOnce(&str) -> Vec<RecoveryAction>,
) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flowspec.db");

    let run_id = {
        let store = SqliteStore::open(&db_path).unwrap();
        let run_id = store.create_run(new_run_for(flow, None)).await.unwrap();
        store.apply(&run_id, mutations).await.unwrap();
        run_id
    };

    // Re-open from the same file and assert structural equality + recovery.
    let store = SqliteStore::open(&db_path).unwrap();
    let loaded = store.load_run(&run_id).await.unwrap();
    assert_eq!(loaded.run_id, run_id);
    assert_eq!(loaded.flow_name, flow.name);

    let running = store.runs_in_phase(RunPhase::Running).await.unwrap();
    let recovered: Vec<_> = running
        .into_iter()
        .flat_map(|r| recovery_plan(vec![r]))
        .collect();
    let expected = expected_recovery(&run_id);
    assert_eq!(recovered, expected);
}

#[tokio::test]
async fn linear_started_round_trips_and_has_no_recovery_actions() {
    // A `Running` run with no step rows yet is the gating-hook-mid case: the
    // runtime stopped before any step ever activated. Recovery now re-issues
    // `RunStarted` so the run makes forward progress (re-running the gating
    // hooks and entry activations) instead of hanging forever.
    let flow = load_flow("linear.yaml");
    round_trip_at_state(&flow, vec![], |run_id| {
        vec![RecoveryAction::ReissueRunStarted {
            run_id: run_id.into(),
        }]
    })
    .await;
}

#[tokio::test]
async fn linear_mid_step_round_trips_and_reattaches() {
    let flow = load_flow("linear.yaml");
    round_trip_at_state(
        &flow,
        vec![Mutation::InsertStepRun(running_step(
            "plan",
            1,
            Some("job_plan"),
        ))],
        |run_id| {
            vec![RecoveryAction::ReattachStep {
                run_id: run_id.into(),
                step_id: "plan".into(),
                attempt: 1,
                job_id: "job_plan".into(),
            }]
        },
    )
    .await;
}

#[tokio::test]
async fn linear_terminal_completed_round_trips_with_no_recovery() {
    let flow = load_flow("linear.yaml");
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flowspec.db");

    let store = SqliteStore::open(&db_path).unwrap();
    let run_id = store.create_run(new_run_for(&flow, None)).await.unwrap();
    store
        .apply(&run_id, vec![Mutation::SetRunPhase(RunPhase::Completed)])
        .await
        .unwrap();

    drop(store);
    let store = SqliteStore::open(&db_path).unwrap();
    let loaded = store.load_run(&run_id).await.unwrap();
    assert_eq!(loaded.phase, RunPhase::Completed);
    assert!(
        store
            .runs_in_phase(RunPhase::Running)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn human_loop_waiting_approval_round_trips() {
    let flow = load_flow("human-loop.yaml");
    let mut step = running_step("plan", 1, None);
    step.run.status = StepStatus::WaitingApproval;

    round_trip_at_state(&flow, vec![Mutation::InsertStepRun(step)], |run_id| {
        vec![RecoveryAction::LeaveWaitingApproval {
            run_id: run_id.into(),
            step_id: "plan".into(),
            attempt: 1,
        }]
    })
    .await;
}

#[tokio::test]
async fn fan_out_multiple_running_steps_round_trips() {
    let flow = load_flow("fan-out.yaml");
    let steps = vec![
        running_step("build", 1, Some("job_build")),
        running_step("unit-tests", 1, Some("job_unit")),
        running_step("integration-tests", 1, None),
    ];
    let mutations: Vec<_> = steps
        .clone()
        .into_iter()
        .map(Mutation::InsertStepRun)
        .collect();

    round_trip_at_state(&flow, mutations, |run_id| {
        vec![
            RecoveryAction::ReattachStep {
                run_id: run_id.into(),
                step_id: "build".into(),
                attempt: 1,
                job_id: "job_build".into(),
            },
            RecoveryAction::FailStep {
                run_id: run_id.into(),
                step_id: "integration-tests".into(),
                attempt: 1,
                reason: RecoveryReason::Interrupted,
            },
            RecoveryAction::ReattachStep {
                run_id: run_id.into(),
                step_id: "unit-tests".into(),
                attempt: 1,
                job_id: "job_unit".into(),
            },
        ]
    })
    .await;
}

#[tokio::test]
async fn failure_routing_terminal_failed_round_trips() {
    let flow = load_flow("failure-routing.yaml");
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flowspec.db");

    let store = SqliteStore::open(&db_path).unwrap();
    let run_id = store.create_run(new_run_for(&flow, None)).await.unwrap();
    store
        .apply(
            &run_id,
            vec![
                Mutation::InsertStepRun(running_step("implement", 1, None)),
                Mutation::SetRunPhase(RunPhase::Failed),
            ],
        )
        .await
        .unwrap();

    drop(store);
    let store = SqliteStore::open(&db_path).unwrap();
    let loaded = store.load_run(&run_id).await.unwrap();
    assert_eq!(loaded.phase, RunPhase::Failed);
}

#[tokio::test]
async fn atomicity_invalid_mutation_leaves_db_untouched() {
    let flow = load_flow("linear.yaml");
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flowspec.db");

    let store = SqliteStore::open(&db_path).unwrap();
    let run_id = store.create_run(new_run_for(&flow, None)).await.unwrap();

    let err = store
        .apply(
            &run_id,
            vec![
                Mutation::InsertStepRun(running_step("plan", 1, Some("job_1"))),
                Mutation::SetStepStatus {
                    step_id: "does-not-exist".into(),
                    attempt: 1,
                    status: StepStatus::Completed,
                },
            ],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, flowspec_app::ports::StoreError::NotFound(_)));

    // The first mutation must not have committed.
    let loaded = store.load_run(&run_id).await.unwrap();
    assert!(loaded.steps.is_empty());
}

#[tokio::test]
async fn schema_is_idempotent_on_second_open() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flowspec.db");

    let store = SqliteStore::open(&db_path).unwrap();
    let run_id = store
        .create_run(new_run_for(&load_flow("linear.yaml"), None))
        .await
        .unwrap();
    drop(store);

    let store = SqliteStore::open(&db_path).unwrap();
    let loaded = store.load_run(&run_id).await.unwrap();
    assert_eq!(loaded.run_id, run_id);
}

#[tokio::test]
async fn find_by_idempotency_key_finds_the_owning_run() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(dir.path().join("flowspec.db")).unwrap();
    let flow = load_flow("linear.yaml");
    let run_id = store
        .create_run(new_run_for(&flow, Some("idem-1")))
        .await
        .unwrap();

    assert_eq!(
        store.find_by_idempotency_key("idem-1").await.unwrap(),
        Some(run_id)
    );
    assert_eq!(
        store.find_by_idempotency_key("no-such-key").await.unwrap(),
        None
    );
}

// Regression: `list_runs` used to always bind a fixed 3-tuple
// (flow_name, phase, limit) regardless of how many `?` placeholders the
// filter actually pushed into the SQL string, so any subset of filters
// other than "all three" or "none" failed with "wrong number of
// parameters". Exercise each partial combination.
#[tokio::test]
async fn list_runs_with_a_partial_filter_combination_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::open(dir.path().join("flowspec.db")).unwrap();
    let flow = load_flow("linear.yaml");
    store.create_run(new_run_for(&flow, None)).await.unwrap();

    // flow_name + limit, no phase -- the combination that broke on the homelab.
    store
        .list_runs(RunFilter {
            flow_name: Some(flow.name.clone()),
            phase: None,
            limit: Some(5),
        })
        .await
        .unwrap();

    // flow_name only.
    store
        .list_runs(RunFilter {
            flow_name: Some(flow.name.clone()),
            phase: None,
            limit: None,
        })
        .await
        .unwrap();

    // limit only.
    store
        .list_runs(RunFilter {
            flow_name: None,
            phase: None,
            limit: Some(5),
        })
        .await
        .unwrap();

    // phase + limit, no flow_name.
    store
        .list_runs(RunFilter {
            flow_name: None,
            phase: Some(RunPhase::Running),
            limit: Some(5),
        })
        .await
        .unwrap();

    // no filters at all.
    store
        .list_runs(RunFilter {
            flow_name: None,
            phase: None,
            limit: None,
        })
        .await
        .unwrap();

    // all three filters together (the one combination that already worked).
    let all = store
        .list_runs(RunFilter {
            flow_name: Some(flow.name.clone()),
            phase: Some(RunPhase::Running),
            limit: Some(5),
        })
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
}

// Regression: `failure_detail` was added to `step_runs`/`hook_runs` after
// the initial schema shipped, and `schema.sql` is pure `CREATE ... IF NOT
// EXISTS` (no migration mechanism exists). On a database that already has
// these tables, the new column in the `CREATE TABLE` body is silently
// ignored -- so `SqliteStore::open` must `ALTER TABLE` it in. Build a DB
// against the *pre-change* schema, then confirm the new binary can open it,
// write through it, and read the new column back as `None` for old rows.
#[tokio::test]
async fn opening_a_pre_migration_database_adds_the_missing_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flowspec.db");

    {
        // Schema as it existed before `failure_detail` was added -- no
        // failure_detail column, no idx_hook_runs_run_id index.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                flow_name TEXT NOT NULL,
                flow_version TEXT NOT NULL,
                definition TEXT NOT NULL,
                inputs TEXT NOT NULL,
                trigger TEXT NOT NULL,
                phase TEXT NOT NULL,
                cancelled INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                idempotency_key TEXT NULL
            );
            CREATE UNIQUE INDEX idx_runs_idempotency_key
                ON runs(idempotency_key) WHERE idempotency_key IS NOT NULL;
            CREATE TABLE step_runs (
                run_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                job_id TEXT NULL,
                with_resolved TEXT NULL,
                input_resolved TEXT NULL,
                output TEXT NULL,
                failure_reason TEXT NULL,
                approval_status TEXT NULL,
                approval_comment TEXT NULL,
                feedback TEXT NULL,
                child_run_id TEXT NULL,
                started_at INTEGER NULL,
                completed_at INTEGER NULL,
                PRIMARY KEY (run_id, step_id, attempt),
                FOREIGN KEY (run_id) REFERENCES runs(id)
            );
            CREATE INDEX idx_step_runs_run_id ON step_runs(run_id);
            CREATE TABLE hook_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                hook TEXT NOT NULL,
                phase TEXT NOT NULL,
                step_id TEXT NULL,
                status TEXT NOT NULL,
                job_id TEXT NULL,
                args_resolved TEXT NULL,
                output_ref TEXT NULL,
                failure_reason TEXT NULL,
                started_at INTEGER NOT NULL,
                completed_at INTEGER NULL,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            );
            CREATE TABLE run_links (
                parent_run TEXT NOT NULL,
                parent_step TEXT NOT NULL,
                child_run TEXT NOT NULL PRIMARY KEY,
                FOREIGN KEY (parent_run) REFERENCES runs(id),
                FOREIGN KEY (child_run) REFERENCES runs(id)
            );",
        )
        .unwrap();
    }

    // Opening with the current binary must not error, and must have added
    // the missing columns/index rather than silently ignoring them.
    let store = SqliteStore::open(&db_path).unwrap();
    let flow = load_flow("linear.yaml");
    let run_id = store.create_run(new_run_for(&flow, None)).await.unwrap();
    store
        .apply(
            &run_id,
            vec![Mutation::RecordHookRun(HookRunRecord {
                hook: "create-feature".into(),
                phase: HookPhase::BeforeRun,
                step_id: None,
                status: HookStatus::Failed,
                started_at: std::time::SystemTime::now(),
                completed_at: Some(std::time::SystemTime::now()),
                args_resolved: None,
                output_ref: None,
                failure_reason: Some("tool error: exit_code=1".into()),
                failure_detail: None,
                job_id: Some("job-1".into()),
            })],
        )
        .await
        .unwrap();

    let hooks = store.list_hook_runs(&run_id).await.unwrap();
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0].hook, "create-feature");
    // Old rows (and this one, since we passed None) read back with no detail.
    assert!(hooks[0].failure_detail.is_none());
}
