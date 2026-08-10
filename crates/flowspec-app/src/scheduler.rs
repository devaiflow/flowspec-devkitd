use crate::ports::{
    Devkitd, HookPhase as PortHookPhase, HookRunRecord, HookStatus, JobHandle, LivenessSink,
    Mutation, RunId, RunRecord, SchedulerConfig, StartRequest, StateStore, StepRecord,
};
use crate::recovery::{RecoveryAction, RecoveryReason, recovery_plan};
use crate::wire::JobFailure;
use flowspec_domain::flow::schema::parse_duration;
use flowspec_domain::flow::types::{FlowDefinition, HookCall, Step, StepWith};
use flowspec_domain::run::types::{
    ApprovalStatus, Command, Event, HookPhase, RunPhase, StepActivation, StepStatus,
};
use flowspec_domain::run::{decide, derive};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Mutex as TokioMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Map of in-flight step wait tasks keyed by run + step + attempt.
type InflightMap = Arc<Mutex<HashMap<(RunId, String, u32), JoinHandle<()>>>>;
/// Map of in-flight hook batch tasks keyed by run + phase descriptor.
type HookInflightMap = Arc<Mutex<HashMap<(RunId, String), JoinHandle<()>>>>;

/// The imperative shell around the domain engine. `submit(run_id, event)` is the
/// only entry point for runtime events; it serializes all state transitions for
/// a given run and spawns the long-running step/hook tasks outside the lock.
#[derive(Clone)]
pub struct Scheduler {
    store: Arc<dyn StateStore>,
    devkitd: Arc<dyn Devkitd>,
    config: SchedulerConfig,
    locks: Arc<Mutex<HashMap<RunId, Arc<TokioMutex<()>>>>>,
    liveness: Arc<Mutex<HashMap<(RunId, String), Instant>>>,
    inflight: InflightMap,
    hook_inflight: HookInflightMap,
    deadlines: Arc<Mutex<HashMap<RunId, JoinHandle<()>>>>,
    shutdown: CancellationToken,
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("store error: {0}")]
    Store(#[from] crate::ports::StoreError),
    #[error("engine error: {0}")]
    Engine(String),
}

impl Scheduler {
    pub fn new(
        store: Arc<dyn StateStore>,
        devkitd: Arc<dyn Devkitd>,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            store,
            devkitd,
            config,
            locks: Arc::new(Mutex::new(HashMap::new())),
            liveness: Arc::new(Mutex::new(HashMap::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            hook_inflight: Arc::new(Mutex::new(HashMap::new())),
            deadlines: Arc::new(Mutex::new(HashMap::new())),
            shutdown: CancellationToken::new(),
        }
    }

    /// Submit an event for a run. All state transitions for the run serialize
    /// through a per-run mutex; spawned step/hook tasks never hold that mutex
    /// while waiting on devkitd.
    pub async fn submit(&self, run_id: RunId, event: Event) {
        if self.shutdown.is_cancelled() {
            return;
        }
        let lock = self.run_lock(&run_id);
        let guard = lock.lock().await;
        let follow_ons = match self.run_locked_event(run_id.clone(), event).await {
            Ok(events) => events,
            Err(e) => {
                tracing::error!(run_id = %run_id, "error processing event: {e}");
                return;
            }
        };
        drop(guard);
        for follow_on in follow_ons {
            Box::pin(self.submit(run_id.clone(), follow_on)).await;
        }
    }

    /// Recovery at boot: turn every persisted running run into ordinary events.
    pub async fn recover(&self) {
        let running = match self.store.runs_in_phase(RunPhase::Running).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("recovery failed to load running runs: {e}");
                return;
            }
        };
        for action in recovery_plan(running) {
            match action {
                RecoveryAction::ReattachStep {
                    run_id,
                    step_id,
                    attempt,
                    job_id,
                } => {
                    self.spawn_reattach_task(run_id, step_id, attempt, job_id);
                }
                RecoveryAction::FailStep {
                    run_id,
                    step_id,
                    attempt: _,
                    reason,
                } => {
                    let reason = match reason {
                        RecoveryReason::Interrupted => "interrupted",
                    };
                    self.submit(
                        run_id,
                        Event::StepFailed {
                            step_id,
                            reason: reason.into(),
                        },
                    )
                    .await;
                }
                RecoveryAction::LeaveWaitingApproval { .. } => {}
                RecoveryAction::RecheckSubflow { .. } => {
                    // Deferred to Phase 6.
                }
                RecoveryAction::ReissueRunStarted { run_id } => {
                    // A run interrupted mid gating-hook (before any step row).
                    // Re-issue RunStarted so the engine re-emits the gating
                    // hooks and entry activations and the run makes progress.
                    self.submit(run_id, Event::RunStarted).await;
                }
            }
        }
        for record in self
            .store
            .runs_in_phase(RunPhase::Running)
            .await
            .unwrap_or_default()
        {
            self.start_deadline_for_record(&record).await;
        }
    }

    /// Graceful shutdown: stop accepting new events and abort in-flight wait
    /// tasks without cancelling their devkitd jobs. Recovery will re-attach on
    /// the next boot.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
        let mut inflight = self.inflight.lock().unwrap();
        for (_, handle) in inflight.drain() {
            handle.abort();
        }
        // Hook wait tasks hold no run lock either; abort them on shutdown too.
        // Their devkitd jobs are *not* cancelled — recovery re-drives the run.
        let mut hook_inflight = self.hook_inflight.lock().unwrap();
        for (_, handle) in hook_inflight.drain() {
            handle.abort();
        }
    }

    /// How long ago did we last receive a liveness pulse for this step?
    pub fn liveness_ago(&self, run_id: &RunId, step_id: &str) -> Option<Duration> {
        self.liveness
            .lock()
            .unwrap()
            .get(&(run_id.clone(), step_id.to_string()))
            .map(|t| t.elapsed())
    }

    /// Start the run-level deadline timer for a run that has a flow `timeout`.
    pub async fn start_deadline_for_run(&self, run_id: &RunId) {
        let record = match self.store.load_run(run_id).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(run_id = %run_id, "failed to load run for deadline: {e}");
                return;
            }
        };
        self.start_deadline_for_record(&record).await;
    }

    // ------------------------------------------------------------------
    // internal helpers
    // ------------------------------------------------------------------

    fn run_lock(&self, run_id: &RunId) -> Arc<TokioMutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        locks
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    }

    async fn run_locked_event(
        &self,
        run_id: RunId,
        event: Event,
    ) -> Result<Vec<Event>, SchedulerError> {
        let record = self.store.load_run(&run_id).await?;
        if is_terminal(record.phase) {
            return Ok(vec![]);
        }
        let state = record.run_state();
        let commands = decide(&record.definition, &state, event.clone(), SystemTime::now())
            .map_err(|e| SchedulerError::Engine(e.to_string()))?;
        // The record we just loaded is authoritative for this turn of the loop;
        // `drive_commands` consumes it directly rather than reloading.
        self.drive_commands(run_id, event, record, commands).await
    }

    async fn run_locked_commands(
        &self,
        run_id: RunId,
        event: Event,
        continuation: Vec<Command>,
    ) -> Result<Vec<Event>, SchedulerError> {
        // Continuation entry from a hook batch: reload so post-hook state
        // (apply mutations persisted between split and now) is fresh.
        let record = self.store.load_run(&run_id).await?;
        if is_terminal(record.phase) {
            return Ok(vec![]);
        }
        self.drive_commands(run_id, event, record, continuation)
            .await
    }

    async fn drive_commands(
        &self,
        run_id: RunId,
        event: Event,
        record: RunRecord,
        commands: Vec<Command>,
    ) -> Result<Vec<Event>, SchedulerError> {
        let (pre, hooks_continuation) = split_at_hooks(commands);
        let follow_ons = self.execute_commands(&run_id, &record, &event, pre).await?;
        if let Some(HooksContinuation { phase, hooks, tail }) = hooks_continuation {
            self.spawn_hook_batch(run_id, event, phase, hooks, tail);
            return Ok(vec![]);
        }
        Ok(follow_ons)
    }

    async fn execute_commands(
        &self,
        run_id: &RunId,
        record: &RunRecord,
        event: &Event,
        commands: Vec<Command>,
    ) -> Result<Vec<Event>, SchedulerError> {
        let mut follow_ons = Vec::new();
        let mut mutations = Vec::new();

        for cmd in commands {
            match cmd {
                Command::ActivateSteps(activations) => {
                    self.activate_steps(run_id, record, activations, &mut mutations)
                        .await?;
                }
                Command::WaitApproval { step_id } => {
                    let attempt = self.step_attempt(record, &step_id)?;
                    mutations.push(Mutation::SetStepStatus {
                        step_id,
                        attempt,
                        status: StepStatus::WaitingApproval,
                    });
                }
                Command::StartChildRun { step_id } => {
                    // Phase 3 stub: mark the subflow step failed and route.
                    let now = SystemTime::now();
                    mutations.push(Mutation::InsertStepRun(StepRecord {
                        run: flowspec_domain::run::types::StepRun {
                            step_id: step_id.clone(),
                            status: StepStatus::Running,
                            attempt: 1,
                            started_at: Some(now),
                            completed_at: None,
                            input_resolved: None,
                            output: None,
                            approval_status: None,
                            feedback: None,
                            approval_comment: None,
                            failure_reason: None,
                            child_run_id: None,
                        },
                        job_id: None,
                        with_resolved: None,
                        failure_detail: None,
                    }));
                    follow_ons.push(Event::StepFailed {
                        step_id,
                        reason: "subflows not yet supported".into(),
                    });
                }
                Command::RunHooks { .. } => {
                    // Hooks are always split out and handled by spawn_hook_batch.
                }
                Command::CancelSteps(step_ids) => {
                    let now = SystemTime::now();
                    for step_id in step_ids {
                        let Some((attempt, job_id)) = record
                            .latest_steps()
                            .into_iter()
                            .find(|s| s.run.step_id == step_id && s.run.status.is_active())
                            .map(|s| (s.run.attempt, s.job_id.clone()))
                        else {
                            continue;
                        };
                        if let Some(job_id) = job_id {
                            if let Err(e) = self.devkitd.cancel(&JobHandle(job_id)).await {
                                tracing::warn!(run_id = %run_id, step_id = %step_id, "cancel failed: {e}");
                            }
                            self.abort_inflight(run_id, &step_id, attempt);
                        }
                        mutations.push(Mutation::SetStepStatus {
                            step_id: step_id.clone(),
                            attempt,
                            status: StepStatus::Cancelled,
                        });
                        mutations.push(Mutation::SetStepTimestamps {
                            step_id: step_id.clone(),
                            attempt,
                            started_at: None,
                            completed_at: Some(now),
                        });
                        self.clear_liveness(run_id, &step_id);
                    }
                }
                Command::MarkStepStatus { step_id, status } => {
                    let attempt = match self.step_attempt(record, &step_id) {
                        Ok(a) => a,
                        Err(_) if status == StepStatus::Skipped => {
                            // Engine can mark an un-activated branch (e.g. the
                            // unused on_failure path) as skipped. There is no
                            // active attempt to update, so just record it.
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    mutations.push(Mutation::SetStepStatus {
                        step_id: step_id.clone(),
                        attempt,
                        status,
                    });
                    if status.is_terminal() {
                        let now = SystemTime::now();
                        mutations.push(Mutation::SetStepTimestamps {
                            step_id: step_id.clone(),
                            attempt,
                            started_at: None,
                            completed_at: Some(now),
                        });
                        self.clear_liveness(run_id, &step_id);
                        if status == StepStatus::Cancelled {
                            self.abort_inflight(run_id, &step_id, attempt);
                        }
                    }
                    self.add_event_data(event, &step_id, attempt, &mut mutations);
                }
                Command::MarkRunTerminal(phase) => {
                    mutations.push(Mutation::SetRunPhase(phase));
                    if phase == RunPhase::Cancelled {
                        mutations.push(Mutation::SetRunCancelled);
                    }
                    self.abort_deadline(run_id);
                }
            }
        }

        if !mutations.is_empty() {
            self.store.apply(run_id, mutations).await?;
        }
        Ok(follow_ons)
    }

    async fn activate_steps(
        &self,
        run_id: &RunId,
        record: &RunRecord,
        activations: Vec<StepActivation>,
        mutations: &mut Vec<Mutation>,
    ) -> Result<(), SchedulerError> {
        let now = SystemTime::now();
        let mut step_records = Vec::new();
        let mut tasks = Vec::new();

        for activation in activations {
            let step = record.definition.step(&activation.step_id).ok_or_else(|| {
                SchedulerError::Engine(format!("unknown step {}", activation.step_id))
            })?;
            let state = record.run_state();
            let ctx =
                flowspec_domain::template::build_context(&record.definition, &state, Value::Null);
            let (with_resolved, tool, timeout_seconds) = build_step_request(
                step,
                &ctx,
                &record.definition,
                &activation.input,
                &self.config,
            )?;

            let mut args = with_resolved.clone();
            if let Value::Object(ref mut map) = args {
                map.insert(
                    "step".to_string(),
                    Value::String(activation.step_id.clone()),
                );
                map.insert(
                    "flow".to_string(),
                    Value::String(record.definition.name.clone()),
                );
            }

            step_records.push(StepRecord {
                run: flowspec_domain::run::types::StepRun {
                    step_id: activation.step_id.clone(),
                    status: StepStatus::Running,
                    attempt: activation.attempt,
                    started_at: Some(now),
                    completed_at: None,
                    input_resolved: Some(activation.input.clone()),
                    output: None,
                    approval_status: None,
                    feedback: None,
                    approval_comment: None,
                    failure_reason: None,
                    child_run_id: None,
                },
                job_id: None,
                with_resolved: Some(with_resolved),
                failure_detail: None,
            });
            tasks.push((
                activation,
                StartRequest {
                    tool,
                    args,
                    timeout_seconds,
                },
            ));
        }

        for rec in step_records {
            mutations.push(Mutation::InsertStepRun(rec));
        }
        if !mutations.is_empty() {
            self.store.apply(run_id, std::mem::take(mutations)).await?;
        }

        for (activation, request) in tasks {
            self.spawn_step_task(run_id.clone(), activation, request);
        }
        Ok(())
    }

    fn add_event_data(
        &self,
        event: &Event,
        step_id: &str,
        attempt: u32,
        mutations: &mut Vec<Mutation>,
    ) {
        match event {
            Event::StepCompleted {
                step_id: ev_step,
                output,
            } if ev_step == step_id => {
                mutations.push(Mutation::SetStepOutput {
                    step_id: step_id.into(),
                    attempt,
                    output: output.clone(),
                });
            }
            Event::StepFailed {
                step_id: ev_step,
                reason,
            } if ev_step == step_id => {
                mutations.push(Mutation::SetStepFailure {
                    step_id: step_id.into(),
                    attempt,
                    reason: reason.clone(),
                });
            }
            Event::StepApproved {
                step_id: ev_step,
                comment,
            } if ev_step == step_id => {
                mutations.push(Mutation::SetApproval {
                    step_id: step_id.into(),
                    attempt,
                    approval_status: ApprovalStatus::Approved,
                    comment: comment.clone(),
                    feedback: None,
                });
            }
            Event::StepRejected {
                step_id: ev_step,
                feedback,
            } if ev_step == step_id => {
                mutations.push(Mutation::SetApproval {
                    step_id: step_id.into(),
                    attempt,
                    approval_status: ApprovalStatus::Rejected,
                    comment: None,
                    feedback: Some(feedback.clone()),
                });
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------------
    // step execution
    // ------------------------------------------------------------------

    fn spawn_step_task(&self, run_id: RunId, activation: StepActivation, request: StartRequest) {
        let scheduler = self.clone();
        let run_id_for_insert = run_id.clone();
        let step_id_for_insert = activation.step_id.clone();
        let attempt_for_insert = activation.attempt;
        let timeout_seconds = request.timeout_seconds;
        let handle = tokio::spawn(async move {
            let step_id = activation.step_id.clone();
            let attempt = activation.attempt;
            match scheduler.devkitd.start(request).await {
                Ok(handle) => {
                    if let Err(e) = scheduler
                        .store
                        .apply(
                            &run_id,
                            vec![Mutation::SetJobId {
                                step_id: step_id.clone(),
                                attempt,
                                job_id: Some(handle.0.clone()),
                            }],
                        )
                        .await
                    {
                        tracing::error!(run_id = %run_id, step_id = %step_id, "failed to persist job_id: {e}");
                        scheduler.remove_inflight(&run_id, &step_id, attempt);
                        scheduler
                            .submit(
                                run_id,
                                Event::StepFailed {
                                    step_id,
                                    reason: "failed to persist job_id".into(),
                                },
                            )
                            .await;
                        return;
                    }
                    let liveness = scheduler.liveness_sink(run_id.clone(), step_id.clone());
                    // The deadline backstop gives devkitd extra grace (its own
                    // server-side timeout fires first) before the runtime
                    // force-cancels the job.
                    let deadline = timeout_seconds.map(|s| {
                        SystemTime::now()
                            + Duration::from_secs(s)
                            + Duration::from_secs(scheduler.config.deadline_margin_secs)
                    });
                    match scheduler.devkitd.wait(&handle, deadline, liveness).await {
                        Ok(out) => {
                            scheduler.remove_inflight(&run_id, &step_id, attempt);
                            scheduler
                                .submit(
                                    run_id,
                                    Event::StepCompleted {
                                        step_id,
                                        output: out.output,
                                    },
                                )
                                .await;
                        }
                        Err(e) => {
                            scheduler.remove_inflight(&run_id, &step_id, attempt);
                            // Write the structured detail while `e` is still
                            // live -- `Event::StepFailed` only carries the
                            // flattened string, and this is the last point
                            // that has the original `DevkitdError`.
                            let _ = scheduler
                                .store
                                .apply(
                                    &run_id,
                                    vec![Mutation::SetStepFailureDetail {
                                        step_id: step_id.clone(),
                                        attempt,
                                        detail: JobFailure::from(&e),
                                    }],
                                )
                                .await;
                            scheduler
                                .submit(
                                    run_id,
                                    Event::StepFailed {
                                        step_id,
                                        reason: format!("{e}"),
                                    },
                                )
                                .await;
                        }
                    }
                }
                Err(e) => {
                    scheduler.remove_inflight(&run_id, &step_id, attempt);
                    let _ = scheduler
                        .store
                        .apply(
                            &run_id,
                            vec![Mutation::SetStepFailureDetail {
                                step_id: step_id.clone(),
                                attempt,
                                detail: JobFailure::from(&e),
                            }],
                        )
                        .await;
                    scheduler
                        .submit(
                            run_id,
                            Event::StepFailed {
                                step_id,
                                reason: format!("start failed: {e}"),
                            },
                        )
                        .await;
                }
            }
        });
        self.insert_inflight(
            run_id_for_insert,
            step_id_for_insert,
            attempt_for_insert,
            handle,
        );
    }

    fn spawn_reattach_task(&self, run_id: RunId, step_id: String, attempt: u32, job_id: String) {
        let scheduler = self.clone();
        let run_id_for_insert = run_id.clone();
        let step_id_for_insert = step_id.clone();
        let handle = tokio::spawn(async move {
            let record = match scheduler.store.load_run(&run_id).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(run_id = %run_id, "reattach load failed: {e}");
                    return;
                }
            };
            let step = match record.definition.step(&step_id) {
                Some(s) => s,
                None => {
                    tracing::error!(run_id = %run_id, "reattach unknown step {step_id}");
                    return;
                }
            };
            let state = record.run_state();
            let ctx =
                flowspec_domain::template::build_context(&record.definition, &state, Value::Null);
            let input = record
                .steps
                .iter()
                .find(|s| s.run.step_id == step_id && s.run.attempt == attempt)
                .and_then(|s| s.run.input_resolved.clone())
                .unwrap_or_default();
            let (with_resolved, _tool, timeout_seconds) =
                match build_step_request(step, &ctx, &record.definition, &input, &scheduler.config)
                {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!(run_id = %run_id, "reattach request build failed: {e}");
                        return;
                    }
                };
            let mut args = with_resolved;
            if let Value::Object(ref mut map) = args {
                map.insert("step".to_string(), Value::String(step_id.clone()));
                map.insert(
                    "flow".to_string(),
                    Value::String(record.definition.name.clone()),
                );
            }
            let liveness = scheduler.liveness_sink(run_id.clone(), step_id.clone());
            let deadline = timeout_seconds.map(|s| SystemTime::now() + Duration::from_secs(s));
            let handle = JobHandle(job_id);
            match scheduler.devkitd.wait(&handle, deadline, liveness).await {
                Ok(out) => {
                    scheduler
                        .submit(
                            run_id,
                            Event::StepCompleted {
                                step_id,
                                output: out.output,
                            },
                        )
                        .await;
                }
                Err(e) => {
                    scheduler
                        .submit(
                            run_id,
                            Event::StepFailed {
                                step_id,
                                reason: format!("{e}"),
                            },
                        )
                        .await;
                }
            }
        });
        self.insert_inflight(run_id_for_insert, step_id_for_insert, attempt, handle);
    }

    // ------------------------------------------------------------------
    // hook execution
    // ------------------------------------------------------------------

    fn spawn_hook_batch(
        &self,
        run_id: RunId,
        event: Event,
        phase: HookPhase,
        hooks: Vec<HookCall>,
        continuation: Vec<Command>,
    ) {
        let scheduler = self.clone();
        let key = hook_phase_key(&phase);
        let run_id_for_inflight = run_id.clone();
        let key_for_inflight = key.clone();
        let key_for_remove = key.clone();
        let run_id_for_remove = run_id.clone();
        let handle = tokio::spawn(async move {
            let mut failure: Option<(String, String)> = None;
            for hook in hooks {
                if let Err(e) = scheduler
                    .run_hook(run_id.clone(), phase.clone(), hook.clone())
                    .await
                {
                    let reason = format!("{e}");
                    // Does this failure abort the batch?
                    let aborts = match &phase {
                        // before_run gates the run: a failing hook aborts the
                        // gate unless `always_continue` says otherwise (audit-
                        // only failure, the gate lets the continuation through).
                        HookPhase::BeforeRun => !hook.always_continue,
                        // per-step gates route the step as failed on the first
                        // hook that fails; siblings are not run.
                        HookPhase::BeforeStep { .. } | HookPhase::AfterStep { .. } => true,
                        // after_run is pure audit on an already-decided terminal
                        // phase: each hook runs independently; failures never
                        // abort the batch (and the continuation must still drive
                        // `MarkRunTerminal`).
                        HookPhase::AfterRun => false,
                    };
                    if aborts {
                        failure = Some((hook.hook.clone(), reason));
                        break;
                    }
                    // Run already recorded the failure in `hook_runs`; keep
                    // going so the gate / portal can still drive the continuation.
                }
            }
            scheduler
                .hook_batch_finished(run_id, phase, failure, continuation, event)
                .await;
            // Self-unregister from the in-flight map once the batch (and any
            // continuation it spawned synchronously) is fully drained. Aborted
            // by shutdown() before reaching here; the map is drained there too.
            scheduler.remove_hook_inflight(&run_id_for_remove, &key_for_remove);
        });
        self.insert_hook_inflight(run_id_for_inflight, key_for_inflight, handle);
    }

    async fn run_hook(
        &self,
        run_id: RunId,
        phase: HookPhase,
        hook: HookCall,
    ) -> Result<Value, crate::ports::DevkitdError> {
        let store = self.store.clone();
        let devkitd = self.devkitd.clone();
        let started_at = SystemTime::now();
        let step_id = match &phase {
            HookPhase::BeforeStep { step_id } | HookPhase::AfterStep { step_id } => {
                Some(step_id.clone())
            }
            _ => None,
        };
        // Every exit from here on -- however early -- funnels through this
        // one closure so a hook that fails before `devkitd.start` (a bad
        // template, an unreachable devkitd) still gets a `hook_runs` row.
        // Previously those two paths `?`-ed straight out of `run_hook` with
        // no row written at all: the worst case for a tool meant to explain
        // *why* a hook failed.
        let record_failure = |args_resolved: Option<Value>,
                              job_id: Option<String>,
                              e: &crate::ports::DevkitdError| {
            tracing::warn!(run_id = %run_id, hook = %hook.hook, "hook failed: {e}");
            Mutation::RecordHookRun(HookRunRecord {
                hook: hook.hook.clone(),
                phase: map_hook_phase(&phase),
                step_id: step_id.clone(),
                status: HookStatus::Failed,
                started_at,
                completed_at: Some(SystemTime::now()),
                args_resolved,
                output_ref: None,
                failure_reason: Some(e.to_string()),
                failure_detail: Some(JobFailure::from(e)),
                job_id,
            })
        };

        let record = store
            .load_run(&run_id)
            .await
            .map_err(|_| crate::ports::DevkitdError::Interrupted)?;
        let state = record.run_state();
        let ctx = flowspec_domain::template::build_context(&record.definition, &state, Value::Null);
        let args_value = Value::Object(
            hook.args
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        let args_resolved =
            match flowspec_domain::template::resolve_value(&args_value, &ctx, &hook.hook) {
                Ok(v) => v,
                Err(e) => {
                    let e = crate::ports::DevkitdError::ToolError {
                        stdout: String::new(),
                        stderr: e.to_string(),
                        exit_code: 1,
                    };
                    let _ = store
                        .apply(&run_id, vec![record_failure(None, None, &e)])
                        .await;
                    return Err(e);
                }
            };
        let timeout_seconds = hook.timeout.as_ref().and_then(|s| parse_duration(s));
        let handle = match devkitd
            .start(StartRequest {
                tool: hook.hook.clone(),
                args: args_resolved.clone(),
                timeout_seconds,
            })
            .await
        {
            Ok(h) => h,
            Err(e) => {
                let _ = store
                    .apply(&run_id, vec![record_failure(Some(args_resolved), None, &e)])
                    .await;
                return Err(e);
            }
        };
        let noop_liveness: LivenessSink = Arc::new(|| {});
        let deadline = timeout_seconds.map(|s| SystemTime::now() + Duration::from_secs(s));
        let result = devkitd.wait(&handle, deadline, noop_liveness).await;
        let status = match &result {
            Ok(_) => HookStatus::Completed,
            Err(_) => HookStatus::Failed,
        };
        if let Err(e) = &result {
            tracing::warn!(run_id = %run_id, hook = %hook.hook, "hook failed: {e}");
        }
        let completed_at = SystemTime::now();
        let hook_record = HookRunRecord {
            hook: hook.hook.clone(),
            phase: map_hook_phase(&phase),
            step_id,
            status,
            started_at,
            completed_at: Some(completed_at),
            args_resolved: Some(args_resolved),
            output_ref: None,
            failure_reason: result.as_ref().err().map(|e| e.to_string()),
            failure_detail: result.as_ref().err().map(JobFailure::from),
            job_id: Some(handle.0.clone()),
        };
        let _ = store
            .apply(&run_id, vec![Mutation::RecordHookRun(hook_record)])
            .await;
        result.map(|out| out.output)
    }

    async fn hook_batch_finished(
        &self,
        run_id: RunId,
        phase: HookPhase,
        failure: Option<(String, String)>,
        continuation: Vec<Command>,
        event: Event,
    ) {
        match (&phase, failure) {
            (HookPhase::BeforeRun, Some((hook, reason))) => {
                // The reason is not lost here -- `run_hook` already wrote it
                // (with full structured detail) to `hook_runs` before this
                // was ever called. This just makes it visible without a
                // `get_run_status`/`list_hook_runs` round trip.
                tracing::warn!(run_id = %run_id, hook = %hook, "before_run hook failed, run marked failed: {reason}");
                let lock = self.run_lock(&run_id);
                let _guard = lock.lock().await;
                let _ = self
                    .store
                    .apply(&run_id, vec![Mutation::SetRunPhase(RunPhase::Failed)])
                    .await;
                self.abort_deadline(&run_id);
            }
            (HookPhase::BeforeStep { step_id }, Some((_, reason)))
            | (HookPhase::AfterStep { step_id }, Some((_, reason))) => {
                self.submit(
                    run_id,
                    Event::StepFailed {
                        step_id: step_id.clone(),
                        reason,
                    },
                )
                .await;
            }
            // AfterRun failure is audit-only in the sense that it doesn't flip
            // the run phase itself -- the engine already decided the terminal
            // phase. But the continuation (which applies `MarkRunTerminal`)
            // must still be driven, or the run would be stuck `Running` after a
            // failing post-completion hook. Falling through to the catch-all
            // below does exactly that.
            _ => {
                let lock = self.run_lock(&run_id);
                let guard = lock.lock().await;
                let follow_ons = match self
                    .run_locked_commands(run_id.clone(), event.clone(), continuation)
                    .await
                {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!(run_id = %run_id, "hook continuation failed: {e}");
                        return;
                    }
                };
                drop(guard);
                for follow_on in follow_ons {
                    self.submit(run_id.clone(), follow_on).await;
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // deadlines and liveness bookkeeping
    // ------------------------------------------------------------------

    async fn start_deadline_for_record(&self, record: &RunRecord) {
        let Some(timeout_str) = &record.definition.timeout else {
            return;
        };
        let Some(secs) = parse_duration(timeout_str.as_str()) else {
            return;
        };
        let deadline = record.created_at + Duration::from_secs(secs);
        let now = SystemTime::now();
        let delay = deadline.duration_since(now).unwrap_or(Duration::ZERO);
        let run_id = record.run_id.clone();
        let scheduler = self.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if !scheduler.shutdown.is_cancelled() {
                scheduler.submit(run_id, Event::RunDeadlineExceeded).await;
            }
        });
        self.deadlines
            .lock()
            .unwrap()
            .insert(record.run_id.clone(), handle);
    }

    fn abort_deadline(&self, run_id: &RunId) {
        if let Some(handle) = self.deadlines.lock().unwrap().remove(run_id) {
            handle.abort();
        }
    }

    fn liveness_sink(&self, run_id: RunId, step_id: String) -> LivenessSink {
        let liveness = self.liveness.clone();
        Arc::new(move || {
            liveness
                .lock()
                .unwrap()
                .insert((run_id.clone(), step_id.clone()), Instant::now());
        })
    }

    fn clear_liveness(&self, run_id: &RunId, step_id: &str) {
        self.liveness
            .lock()
            .unwrap()
            .remove(&(run_id.clone(), step_id.to_string()));
    }

    // ------------------------------------------------------------------
    // in-flight task bookkeeping
    // ------------------------------------------------------------------

    fn insert_inflight(
        &self,
        run_id: RunId,
        step_id: String,
        attempt: u32,
        handle: JoinHandle<()>,
    ) {
        self.inflight
            .lock()
            .unwrap()
            .insert((run_id, step_id, attempt), handle);
    }

    fn remove_inflight(&self, run_id: &RunId, step_id: &str, attempt: u32) {
        self.inflight
            .lock()
            .unwrap()
            .remove(&(run_id.clone(), step_id.to_string(), attempt));
    }

    fn abort_inflight(&self, run_id: &RunId, step_id: &str, attempt: u32) {
        if let Some(handle) =
            self.inflight
                .lock()
                .unwrap()
                .remove(&(run_id.clone(), step_id.to_string(), attempt))
        {
            handle.abort();
        }
    }

    fn insert_hook_inflight(&self, run_id: RunId, key: String, handle: JoinHandle<()>) {
        self.hook_inflight
            .lock()
            .unwrap()
            .insert((run_id, key), handle);
    }

    fn remove_hook_inflight(&self, run_id: &RunId, key: &str) {
        self.hook_inflight
            .lock()
            .unwrap()
            .remove(&(run_id.clone(), key.to_string()));
    }

    // ------------------------------------------------------------------
    // run record helpers
    // ------------------------------------------------------------------

    fn step_attempt(&self, record: &RunRecord, step_id: &str) -> Result<u32, SchedulerError> {
        record
            .latest_steps()
            .into_iter()
            .find(|s| s.run.step_id == step_id)
            .map(|s| s.run.attempt)
            .ok_or_else(|| SchedulerError::Engine(format!("step {step_id} not found")))
    }
}

fn is_terminal(phase: RunPhase) -> bool {
    !matches!(phase, RunPhase::Running)
}

fn split_at_hooks(commands: Vec<Command>) -> (Vec<Command>, Option<HooksContinuation>) {
    for (i, cmd) in commands.iter().enumerate() {
        if let Command::RunHooks { phase, hooks } = cmd {
            let pre = commands[..i].to_vec();
            let post = commands[i + 1..].to_vec();
            return (
                pre,
                Some(HooksContinuation {
                    phase: phase.clone(),
                    hooks: hooks.clone(),
                    tail: post,
                }),
            );
        }
    }
    (commands, None)
}

/// The split halves of a command stream around its first `RunHooks`.
struct HooksContinuation {
    phase: HookPhase,
    hooks: Vec<HookCall>,
    tail: Vec<Command>,
}

fn map_hook_phase(phase: &HookPhase) -> PortHookPhase {
    match phase {
        HookPhase::BeforeRun => PortHookPhase::BeforeRun,
        HookPhase::AfterRun => PortHookPhase::AfterRun,
        HookPhase::BeforeStep { .. } => PortHookPhase::BeforeStep,
        HookPhase::AfterStep { .. } => PortHookPhase::AfterStep,
    }
}

/// A stable string key for one in-flight hook batch, used by `hook_inflight`.
fn hook_phase_key(phase: &HookPhase) -> String {
    match phase {
        HookPhase::BeforeRun => "before_run".into(),
        HookPhase::AfterRun => "after_run".into(),
        HookPhase::BeforeStep { step_id } => format!("before_step:{step_id}"),
        HookPhase::AfterStep { step_id } => format!("after_step:{step_id}"),
    }
}

fn build_step_request(
    step: &Step,
    ctx: &flowspec_domain::template::TemplateContext,
    flow: &FlowDefinition,
    input: &str,
    config: &SchedulerConfig,
) -> Result<(Value, String, Option<u64>), SchedulerError> {
    match step.kind() {
        Some(StepWith::Cli { with }) => {
            let mut obj = serde_json::Map::new();
            obj.insert("cli".to_string(), Value::String(with.cli.clone()));
            if let Some(role) = &with.role {
                obj.insert("role".to_string(), Value::String(role.clone()));
            }
            obj.insert("input".to_string(), Value::String(input.to_string()));
            if let Some(output) = &with.output {
                obj.insert("output".to_string(), Value::String(output.clone()));
            }
            for (k, v) in &with.extra {
                obj.insert(k.clone(), v.clone());
            }
            if let Some(cli) = &flow.defaults.cli {
                obj.entry("cli")
                    .or_insert_with(|| Value::String(cli.clone()));
            }
            if let Some(provider) = &flow.defaults.provider {
                obj.entry("provider")
                    .or_insert_with(|| Value::String(provider.clone()));
            }
            if let Some(model) = &flow.defaults.model {
                obj.entry("model")
                    .or_insert_with(|| Value::String(model.clone()));
            }
            let resolved =
                flowspec_domain::template::resolve_value(&Value::Object(obj), ctx, &step.id)
                    .map_err(|e| SchedulerError::Engine(e.to_string()))?;
            let timeout_seconds = step
                .timeout
                .as_ref()
                .and_then(|s| parse_duration(s))
                .or(Some(config.default_step_timeout_secs));
            Ok((resolved, config.executor_cli_tool.clone(), timeout_seconds))
        }
        _ => Err(SchedulerError::Engine(format!(
            "step {} is not executable",
            step.id
        ))),
    }
}

// Re-export derive helpers so use cases can compute status without importing domain directly.
pub use derive::active_steps as run_active_steps;
pub use derive::phase as run_phase;
