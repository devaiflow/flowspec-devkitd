use flowspec_app::ports::{Mutation, NewRun, RunFilter, StateStore, StepRecord, StoreError};
use flowspec_app::testkit::InMemoryStateStore;
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_domain::run::types::{RunPhase, StepRun, StepStatus};
use serde_json::json;

fn empty_flow() -> FlowDefinition {
    FlowDefinition {
        name: "empty".into(),
        version: "1.0.0".into(),
        description: None,
        inputs: Default::default(),
        outputs: Default::default(),
        defaults: Default::default(),
        timeout: None,
        lifecycle: Default::default(),
        metadata: Default::default(),
        flowspec_version: None,
        steps: Vec::new(),
    }
}

fn new_run(idem: Option<&str>) -> NewRun {
    NewRun {
        flow_name: "empty".into(),
        flow_version: "1.0.0".into(),
        definition: empty_flow(),
        inputs: json!({"message": "hi"}),
        trigger: json!({"user": "test"}),
        idempotency_key: idem.map(|s| s.into()),
    }
}

#[tokio::test]
async fn create_run_then_load_round_trips() {
    let store = InMemoryStateStore::new();
    let run_id = store.create_run(new_run(None)).await.unwrap();
    let loaded = store.load_run(&run_id).await.unwrap();

    assert_eq!(loaded.run_id, run_id);
    assert_eq!(loaded.flow_name, "empty");
    assert_eq!(loaded.phase, RunPhase::Running);
    assert_eq!(loaded.inputs, json!({"message": "hi"}));
}

#[tokio::test]
async fn duplicate_idempotency_key_is_rejected() {
    let store = InMemoryStateStore::new();
    let run_id = store.create_run(new_run(Some("idem-1"))).await.unwrap();

    let err = store.create_run(new_run(Some("idem-1"))).await.unwrap_err();
    assert!(matches!(err, StoreError::Duplicate(_)));

    let loaded = store.load_run(&run_id).await.unwrap();
    assert_eq!(loaded.idempotency_key, Some("idem-1".into()));
}

#[tokio::test]
async fn find_by_idempotency_key_finds_the_owning_run() {
    let store = InMemoryStateStore::new();
    let run_id = store.create_run(new_run(Some("idem-1"))).await.unwrap();

    assert_eq!(
        store.find_by_idempotency_key("idem-1").await.unwrap(),
        Some(run_id)
    );
    assert_eq!(
        store.find_by_idempotency_key("no-such-key").await.unwrap(),
        None
    );
}

#[tokio::test]
async fn apply_mutations_then_reload_is_equal() {
    let store = InMemoryStateStore::new();
    let run_id = store.create_run(new_run(None)).await.unwrap();

    let step = StepRecord {
        run: StepRun {
            step_id: "plan".into(),
            status: StepStatus::Running,
            attempt: 1,
            input_resolved: Some("build it".into()),
            ..StepRun::pending("plan")
        },
        job_id: Some("job_1".into()),
        with_resolved: Some(json!({"cli": "claude-code"})),
        failure_detail: None,
    };

    store
        .apply(&run_id, vec![Mutation::InsertStepRun(step.clone())])
        .await
        .unwrap();

    let loaded = store.load_run(&run_id).await.unwrap();
    assert_eq!(loaded.steps.len(), 1);
    assert_eq!(loaded.steps[0].run.step_id, "plan");
    assert_eq!(loaded.steps[0].run.status, StepStatus::Running);
    assert_eq!(loaded.steps[0].job_id, Some("job_1".into()));
    assert_eq!(loaded.steps[0].run.input_resolved, Some("build it".into()));
}

#[tokio::test]
async fn apply_batch_is_atomic() {
    let store = InMemoryStateStore::new();
    let run_id = store.create_run(new_run(None)).await.unwrap();

    // First mutation is valid; second references a non-existent step.
    let step = StepRecord {
        run: StepRun {
            step_id: "plan".into(),
            status: StepStatus::Running,
            attempt: 1,
            ..StepRun::pending("plan")
        },
        job_id: None,
        with_resolved: None,
        failure_detail: None,
    };

    let err = store
        .apply(
            &run_id,
            vec![
                Mutation::InsertStepRun(step),
                Mutation::SetStepStatus {
                    step_id: "does-not-exist".into(),
                    attempt: 1,
                    status: StepStatus::Completed,
                },
            ],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    // The first mutation must not have committed.
    let loaded = store.load_run(&run_id).await.unwrap();
    assert!(loaded.steps.is_empty());
}

#[tokio::test]
async fn list_runs_filters_and_limits() {
    let store = InMemoryStateStore::new();
    let a = store.create_run(new_run(None)).await.unwrap();

    let mut other = new_run(None);
    other.flow_name = "other".into();
    let b = store.create_run(other).await.unwrap();

    store
        .apply(&a, vec![Mutation::SetRunPhase(RunPhase::Completed)])
        .await
        .unwrap();

    let all = store.list_runs(RunFilter::default()).await.unwrap();
    assert_eq!(all.len(), 2);

    let completed = store
        .list_runs(RunFilter {
            phase: Some(RunPhase::Completed),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].run_id, a);

    let other_flows = store
        .list_runs(RunFilter {
            flow_name: Some("other".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(other_flows.len(), 1);
    assert_eq!(other_flows[0].run_id, b);

    let limited = store
        .list_runs(RunFilter {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
}
