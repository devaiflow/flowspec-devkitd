use crate::ids;
use crate::ports::{
    Devkitd, DevkitdError, FlowSource, HookRunRecord, JobHandle, LivenessSink, Mutation, NewRun,
    RunFilter, RunId, RunRecord, RunSummary, RunTree, StartRequest, StateStore, StepOutput,
    StepRecord, StoreError, StoredRunEvent,
};
use async_trait::async_trait;
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_domain::run::types::RunPhase;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, RwLock};

/// In-memory implementation of `StateStore` for unit tests and fakes.
/// Mirrors the SQLite adapter's behaviour (atomic batches, not-found errors,
/// idempotency-key uniqueness) without touching the filesystem.
pub struct InMemoryStateStore {
    runs: RwLock<HashMap<RunId, RunRecord>>,
    hook_runs: RwLock<HashMap<RunId, Vec<HookRunRecord>>>,
    events: RwLock<HashMap<RunId, Vec<StoredRunEvent>>>,
    /// Highest sequence marked pushed, per run. Mirrors `run_events.pushed_at
    /// IS NOT NULL` without needing a mutable flag per stored event.
    events_pushed_through: RwLock<HashMap<RunId, u64>>,
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStateStore {
    pub fn new() -> Self {
        Self {
            runs: RwLock::new(HashMap::new()),
            hook_runs: RwLock::new(HashMap::new()),
            events: RwLock::new(HashMap::new()),
            events_pushed_through: RwLock::new(HashMap::new()),
        }
    }

    async fn with_run<F, T>(&self, run_id: &str, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut RunRecord) -> Result<T, StoreError>,
    {
        let mut guard = self.runs.write().await;
        let run = guard
            .get_mut(run_id)
            .ok_or_else(|| StoreError::NotFound(run_id.into()))?;
        f(run)
    }

    fn apply_mutation(run: &mut RunRecord, m: Mutation) -> Result<(), StoreError> {
        match m {
            Mutation::InsertStepRun(step) => {
                let key = (&step.run.step_id, step.run.attempt);
                run.steps.retain(|s| (&s.run.step_id, s.run.attempt) != key);
                run.steps.push(step);
            }
            Mutation::SetStepStatus {
                step_id,
                attempt,
                status,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.run.status = status;
            }
            Mutation::SetStepOutput {
                step_id,
                attempt,
                output,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.run.output = Some(output);
            }
            Mutation::SetJobId {
                step_id,
                attempt,
                job_id,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.job_id = job_id;
            }
            // SetWithResolved / SetInputResolved were folded into InsertStepRun:
            // the post-template `with:` snapshot and the resolved `input` string
            // are written once, together with the step row itself, atomically.
            Mutation::SetApproval {
                step_id,
                attempt,
                approval_status,
                comment,
                feedback,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.run.approval_status = Some(approval_status);
                step.run.approval_comment = comment;
                step.run.feedback = feedback;
            }
            Mutation::SetStepFailure {
                step_id,
                attempt,
                reason,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.run.failure_reason = Some(reason);
            }
            Mutation::SetStepTimestamps {
                step_id,
                attempt,
                started_at,
                completed_at,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.run.started_at = started_at;
                step.run.completed_at = completed_at;
            }
            Mutation::SetChildRunId {
                step_id,
                attempt,
                child_run_id,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.run.child_run_id = child_run_id;
            }
            Mutation::SetRunPhase(phase) => {
                run.phase = phase;
            }
            Mutation::SetRunCancelled => {
                run.cancelled = true;
            }
            Mutation::SetStepFailureDetail {
                step_id,
                attempt,
                detail,
            } => {
                let step = find_step(run, &step_id, attempt)?;
                step.failure_detail = Some(detail);
            }
            Mutation::RecordHookRun(_) => {
                // Handled in `apply` (needs `&self`, not `&mut RunRecord`) --
                // this mutation doesn't touch `RunRecord` at all.
            }
            Mutation::AppendEvent(_) => {
                // Handled in `apply` (needs `&self` to assign a per-run
                // sequence) -- this mutation doesn't touch `RunRecord` either.
            }
        }
        Ok(())
    }
}

fn find_step<'a>(
    run: &'a mut RunRecord,
    step_id: &str,
    attempt: u32,
) -> Result<&'a mut StepRecord, StoreError> {
    run.steps
        .iter_mut()
        .find(|s| s.run.step_id == step_id && s.run.attempt == attempt)
        .ok_or_else(|| {
            StoreError::NotFound(format!(
                "step not found in run {}: {} attempt {}",
                run.run_id, step_id, attempt
            ))
        })
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn find_by_idempotency_key(&self, key: &str) -> Result<Option<RunId>, StoreError> {
        let guard = self.runs.read().await;
        Ok(guard
            .values()
            .find(|r| r.idempotency_key.as_deref() == Some(key))
            .map(|r| r.run_id.clone()))
    }

    async fn create_run(&self, new: NewRun) -> Result<RunId, StoreError> {
        let mut guard = self.runs.write().await;
        if let Some(key) = &new.idempotency_key {
            for run in guard.values() {
                if run.idempotency_key.as_ref() == Some(key) {
                    return Err(StoreError::Duplicate(key.clone()));
                }
            }
        }
        let run_id = ids::new_run_id();
        let now = SystemTime::now();
        guard.insert(
            run_id.clone(),
            RunRecord {
                run_id: run_id.clone(),
                flow_name: new.flow_name,
                flow_version: new.flow_version,
                definition: new.definition,
                inputs: new.inputs,
                trigger: new.trigger,
                phase: RunPhase::Running,
                cancelled: false,
                created_at: now,
                updated_at: now,
                idempotency_key: new.idempotency_key,
                steps: Vec::new(),
            },
        );
        Ok(run_id)
    }

    async fn load_run(&self, run_id: &str) -> Result<RunRecord, StoreError> {
        let guard = self.runs.read().await;
        guard
            .get(run_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(run_id.into()))
    }

    async fn apply(&self, run_id: &str, mutations: Vec<Mutation>) -> Result<(), StoreError> {
        // RecordHookRun/AppendEvent aren't part of RunRecord (they mirror the
        // SQLite adapter's separate hook_runs/run_events tables), so they
        // can't go through apply_mutation's &mut RunRecord signature. Pull
        // those out here, where &self is in scope, and append them alongside
        // the batch.
        let hook_records: Vec<HookRunRecord> = mutations
            .iter()
            .filter_map(|m| match m {
                Mutation::RecordHookRun(h) => Some(h.clone()),
                _ => None,
            })
            .collect();
        let event_records: Vec<_> = mutations
            .iter()
            .filter_map(|m| match m {
                Mutation::AppendEvent(e) => Some(e.clone()),
                _ => None,
            })
            .collect();
        self.with_run(run_id, |run| {
            // Validate the whole batch first, then commit -- mirrors SQLite
            // transaction semantics.
            let mut scratch = run.clone();
            for m in &mutations {
                Self::apply_mutation(&mut scratch, m.clone())?;
            }
            *run = scratch;
            run.updated_at = SystemTime::now();
            Ok(())
        })
        .await?;
        if !hook_records.is_empty() {
            let mut guard = self.hook_runs.write().await;
            guard
                .entry(run_id.to_string())
                .or_default()
                .extend(hook_records);
        }
        if !event_records.is_empty() {
            let mut guard = self.events.write().await;
            let list = guard.entry(run_id.to_string()).or_default();
            for record in event_records {
                let sequence = list.last().map(|e| e.sequence + 1).unwrap_or(1);
                list.push(StoredRunEvent {
                    sequence,
                    event_type: record.event_type,
                    step_id: record.step_id,
                    timestamp: record.timestamp,
                    payload: record.payload,
                });
            }
        }
        Ok(())
    }

    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        let guard = self.runs.read().await;
        let mut out: Vec<RunSummary> = guard
            .values()
            .filter(|r| filter.flow_name.as_ref().is_none_or(|n| &r.flow_name == n))
            .filter(|r| filter.phase.is_none_or(|p| r.phase == p))
            .map(|r| RunSummary {
                run_id: r.run_id.clone(),
                flow_name: r.flow_name.clone(),
                flow_version: r.flow_version.clone(),
                phase: r.phase,
                created_at: r.created_at.into(),
                updated_at: r.updated_at.into(),
            })
            .collect();
        out.sort_by_key(|r| r.created_at);
        if let Some(limit) = filter.limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    async fn runs_in_phase(&self, phase: RunPhase) -> Result<Vec<RunRecord>, StoreError> {
        let guard = self.runs.read().await;
        let mut out: Vec<RunRecord> = guard
            .values()
            .filter(|r| r.phase == phase)
            .cloned()
            .collect();
        out.sort_by_key(|r| r.created_at);
        Ok(out)
    }

    async fn link_runs(
        &self,
        _parent_run: &str,
        _parent_step: &str,
        _child_run: &str,
    ) -> Result<(), StoreError> {
        // In-memory linkage is not needed for Phase 2 domain tests; stored
        // only by the SQLite adapter.
        Ok(())
    }

    async fn load_run_tree(&self, run_id: &str) -> Result<RunTree, StoreError> {
        let root = self.load_run(run_id).await?;
        Ok(RunTree {
            root,
            children: Vec::new(),
        })
    }

    async fn list_hook_runs(&self, run_id: &str) -> Result<Vec<HookRunRecord>, StoreError> {
        let guard = self.hook_runs.read().await;
        Ok(guard.get(run_id).cloned().unwrap_or_default())
    }

    async fn unpushed_events(
        &self,
        limit: usize,
    ) -> Result<Vec<(RunId, StoredRunEvent)>, StoreError> {
        let events = self.events.read().await;
        let pushed_through = self.events_pushed_through.read().await;
        let mut out = Vec::new();
        // Deterministic ordering (run_id, sequence), matching the SQLite
        // adapter's `ORDER BY run_id, sequence`.
        let mut run_ids: Vec<&RunId> = events.keys().collect();
        run_ids.sort();
        'outer: for run_id in run_ids {
            let through = pushed_through.get(run_id).copied().unwrap_or(0);
            for event in &events[run_id] {
                if event.sequence > through {
                    out.push((run_id.clone(), event.clone()));
                    if out.len() >= limit {
                        break 'outer;
                    }
                }
            }
        }
        Ok(out)
    }

    async fn mark_events_pushed(
        &self,
        run_id: &str,
        through_sequence: u64,
    ) -> Result<(), StoreError> {
        let mut pushed_through = self.events_pushed_through.write().await;
        let entry = pushed_through.entry(run_id.to_string()).or_insert(0);
        *entry = (*entry).max(through_sequence);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fake devkitd
// ---------------------------------------------------------------------------

/// Scripted outcome for a step or hook job in the fake devkitd.
#[derive(Debug, Clone)]
pub enum Script {
    /// Job succeeds and returns the given JSON value.
    Succeed(Value),
    /// Job fails with the given error.
    Fail(DevkitdError),
    /// Job never completes unless cancelled or its deadline fires.
    Hang,
    /// Wait for the duration, then behave like the nested script.
    DelayThen(Duration, Box<Script>),
    /// Job survives a `FakeDevkitd::simulate_restart()`; otherwise behaves like Hang.
    VanishOnRestart,
    /// Use the first script for the first invocation, the second for the next,
    /// and so on. The last script is reused once the sequence is exhausted.
    Sequence(Vec<Script>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobStatus {
    Running,
    Done,
    Cancelled,
}

#[derive(Debug, Clone)]
struct JobEntry {
    script: Script,
    status: JobStatus,
    output: Option<Value>,
    error: Option<DevkitdError>,
    started_at: Instant,
}

#[derive(Debug, Clone)]
struct Invocation {
    pub tool: String,
    pub args: Value,
    pub timestamp: Instant,
}

/// In-memory, scripted devkitd double. Keeps a real job table so it can model
/// re-attach and jobs that vanish across a simulated restart.
pub struct FakeDevkitd {
    scripts: Mutex<HashMap<String, Script>>,
    jobs: Mutex<HashMap<String, JobEntry>>,
    invocations: Mutex<Vec<Invocation>>,
    counter: AtomicU64,
    poll_interval: Duration,
}

impl FakeDevkitd {
    pub fn new(scripts: HashMap<String, Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts),
            jobs: Mutex::new(HashMap::new()),
            invocations: Mutex::new(Vec::new()),
            counter: AtomicU64::new(0),
            poll_interval: Duration::from_millis(10),
        }
    }

    /// Create a fake with an even faster poll interval for tests that need to
    /// assert liveness quickly.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    async fn take_script(&self, tool: &str, args: &Value) -> Script {
        // Steps pass their step id under a hidden `step` key; hooks are keyed by
        // the hook name (which is the `tool`).
        let key = args
            .get("step")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| tool.to_string());
        let mut scripts = self.scripts.lock().await;
        let entry = scripts
            .entry(key)
            .or_insert_with(|| Script::Succeed(Value::Null));
        if let Script::Sequence(seq) = entry {
            if seq.is_empty() {
                return Script::Succeed(Value::Null);
            }
            let current = seq.remove(0);
            if seq.len() == 1 {
                let last = seq.remove(0);
                *entry = last;
            }
            current
        } else {
            entry.clone()
        }
    }

    /// Remove all jobs whose script is `VanishOnRestart`, simulating a devkitd
    /// restart that does not preserve those jobs.
    pub async fn simulate_restart(&self) {
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|_, entry| !matches!(entry.script, Script::VanishOnRestart));
    }

    /// All `start` calls recorded so far, with timestamps.
    pub async fn invocations(&self) -> Vec<(String, Value, Instant)> {
        self.invocations
            .lock()
            .await
            .iter()
            .map(|i| (i.tool.clone(), i.args.clone(), i.timestamp))
            .collect()
    }

    /// Total number of recorded `start` calls.
    pub async fn start_count(&self) -> usize {
        self.invocations.lock().await.len()
    }
}

#[async_trait]
impl Devkitd for FakeDevkitd {
    async fn start(&self, req: StartRequest) -> Result<JobHandle, DevkitdError> {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        let job_id = format!("job_{n:06x}");
        let script = self.take_script(&req.tool, &req.args).await;
        let mut jobs = self.jobs.lock().await;
        jobs.insert(
            job_id.clone(),
            JobEntry {
                script,
                status: JobStatus::Running,
                output: None,
                error: None,
                started_at: Instant::now(),
            },
        );
        drop(jobs);
        self.invocations.lock().await.push(Invocation {
            tool: req.tool,
            args: req.args,
            timestamp: Instant::now(),
        });
        Ok(JobHandle(job_id))
    }

    async fn wait(
        &self,
        handle: &JobHandle,
        deadline: Option<SystemTime>,
        liveness: LivenessSink,
    ) -> Result<StepOutput, DevkitdError> {
        let mut interval = tokio::time::interval(self.poll_interval);
        loop {
            interval.tick().await;
            liveness();

            if let Some(deadline) = deadline
                && SystemTime::now() >= deadline
            {
                return Err(DevkitdError::Timeout);
            }

            let mut jobs = self.jobs.lock().await;
            let Some(entry) = jobs.get_mut(&handle.0) else {
                return Err(DevkitdError::Interrupted);
            };

            match entry.status {
                JobStatus::Done => {
                    return match entry.error.take() {
                        Some(e) => Err(e),
                        None => Ok(StepOutput {
                            output: entry.output.take().unwrap_or(Value::Null),
                        }),
                    };
                }
                JobStatus::Cancelled => {
                    return Err(DevkitdError::Cancelled);
                }
                _ => {}
            }

            match &mut entry.script {
                Script::Succeed(value) => {
                    let value = value.clone();
                    entry.status = JobStatus::Done;
                    entry.output = Some(value);
                }
                Script::Fail(error) => {
                    let error = error.clone();
                    entry.status = JobStatus::Done;
                    entry.error = Some(error);
                }
                Script::Hang => {
                    // keep polling until deadline or cancellation
                }
                Script::DelayThen(delay, next) => {
                    if entry.started_at.elapsed() >= *delay {
                        entry.script = (**next).clone();
                        entry.started_at = Instant::now();
                    }
                }
                Script::VanishOnRestart => {
                    // survives until simulate_restart removes it
                }
                Script::Sequence(_) => {
                    // Should have been flattened by start(); treat as a no-op.
                    entry.script = Script::Succeed(Value::Null);
                }
            }
        }
    }

    async fn cancel(&self, handle: &JobHandle) -> Result<(), DevkitdError> {
        let mut jobs = self.jobs.lock().await;
        if let Some(entry) = jobs.get_mut(&handle.0) {
            entry.status = JobStatus::Cancelled;
            Ok(())
        } else {
            Err(DevkitdError::Interrupted)
        }
    }
}

// ---------------------------------------------------------------------------
// Fake flow source
// ---------------------------------------------------------------------------

/// In-memory flow registry for tests.
pub struct InMemoryFlowSource {
    flows: Vec<FlowDefinition>,
}

impl InMemoryFlowSource {
    pub fn new(flows: Vec<FlowDefinition>) -> Self {
        Self { flows }
    }
}

#[async_trait]
impl FlowSource for InMemoryFlowSource {
    async fn list(&self) -> Vec<FlowDefinition> {
        self.flows.clone()
    }

    async fn get(&self, name: &str, version_req: Option<&str>) -> Option<FlowDefinition> {
        crate::flow_source::select_flow(&self.flows, name, version_req)
    }
}
