use async_trait::async_trait;
use flowspec_app::ids;
use flowspec_app::ports::{
    HookRunRecord, Mutation, NewRun, RunEventType, RunFilter, RunId, RunRecord, RunSummary,
    RunTree, StateStore, StepRecord, StoreError, StoredRunEvent,
};
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_domain::run::types::RunPhase;
use rusqlite::{Connection, OptionalExtension, ToSql, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

/// SQLite implementation of `StateStore`. A single `Connection` is owned by a
/// dedicated OS thread; all writes (and reads, at this scale) go through that
/// thread as boxed closures. WAL mode + busy timeout are enabled at open.
pub struct SqliteStore {
    tx: Option<mpsc::UnboundedSender<Job>>,
    handle: Option<JoinHandle<()>>,
}

impl SqliteStore {
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self, StoreError> {
        let mut conn = Connection::open(db_path).map_err(|e| StoreError::Backend(e.to_string()))?;
        configure(&mut conn)?;
        apply_schema(&mut conn)?;

        let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
        let handle = thread::spawn(move || {
            while let Some(job) = rx.blocking_recv() {
                job(&mut conn);
            }
        });

        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    async fn run_on_thread<F, T>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.tx
            .as_ref()
            .expect("tx is only taken during drop")
            .send(Box::new(move |conn| {
                let _ = tx.send(f(conn));
            }))
            .map_err(|_| StoreError::Backend("state writer task gone".into()))?;
        rx.await
            .map_err(|_| StoreError::Backend("state writer task gone".into()))?
    }
}

impl Drop for SqliteStore {
    fn drop(&mut self) {
        // Dropping the sender closes the channel, which lets the writer thread
        // exit its recv loop.
        let _ = self.tx.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn configure(conn: &mut Connection) -> Result<(), StoreError> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    conn.pragma_update(None, "busy_timeout", 5000)
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    Ok(())
}

fn apply_schema(conn: &mut Connection) -> Result<(), StoreError> {
    conn.execute_batch(include_str!("schema.sql"))
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    // `schema.sql` is pure `CREATE ... IF NOT EXISTS`; on a database that
    // already exists, a column added to a table body there is silently
    // ignored (SQLite has no `ADD COLUMN IF NOT EXISTS`). There is no other
    // migration mechanism, so new columns on existing tables are added here,
    // guarded by a `PRAGMA table_info` check -- a no-op on a fresh database
    // where `apply_schema` above already created the column.
    ensure_column(conn, "step_runs", "failure_detail", "TEXT NULL")?;
    ensure_column(conn, "hook_runs", "failure_detail", "TEXT NULL")?;
    Ok(())
}

/// Add `column` to `table` via `ALTER TABLE ... ADD COLUMN` if it isn't
/// already present. `decl` is the column's type/nullability, e.g. `"TEXT NULL"`.
fn ensure_column(
    conn: &mut Connection,
    table: &str,
    column: &str,
    decl: &str,
) -> Result<(), StoreError> {
    let exists = {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let mut found = false;
        for name in names {
            if name.map_err(|e| StoreError::Backend(e.to_string()))? == column {
                found = true;
                break;
            }
        }
        found
    };
    if !exists {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"))
            .map_err(|e| StoreError::Backend(e.to_string()))?;
    }
    Ok(())
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn find_by_idempotency_key(&self, key: &str) -> Result<Option<RunId>, StoreError> {
        let key = key.to_string();
        self.run_on_thread(move |conn| {
            conn.query_row(
                "SELECT id FROM runs WHERE idempotency_key = ?",
                [&key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| StoreError::Backend(e.to_string()))
        })
        .await
    }

    async fn create_run(&self, new: NewRun) -> Result<RunId, StoreError> {
        let run_id = ids::new_run_id();
        let now = SystemTime::now();
        let id = run_id.clone();
        self.run_on_thread(move |conn| {
            conn.execute(
                "INSERT INTO runs (id, flow_name, flow_version, definition, inputs, trigger, phase, cancelled, created_at, updated_at, idempotency_key)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &id,
                    &new.flow_name,
                    &new.flow_version,
                    to_json(&new.definition)?,
                    to_json(&new.inputs)?,
                    to_json(&new.trigger)?,
                    enum_to_str(&RunPhase::Running),
                    0i32,
                    to_epoch(now),
                    to_epoch(now),
                    new.idempotency_key.as_ref(),
                ),
            )
            .map_err(|e| map_sqlite_err(e, new.idempotency_key.as_deref()))?;
            Ok(id)
        })
        .await
    }

    async fn load_run(&self, run_id: &str) -> Result<RunRecord, StoreError> {
        let run_id = run_id.to_string();
        self.run_on_thread(move |conn| load_run_sync(conn, &run_id))
            .await
    }

    async fn apply(&self, run_id: &str, mutations: Vec<Mutation>) -> Result<(), StoreError> {
        let run_id = run_id.to_string();
        self.run_on_thread(move |conn| {
            let tx = conn
                .transaction()
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            for m in mutations {
                apply_mutation(&tx, &run_id, m)?;
            }
            tx.execute(
                "UPDATE runs SET updated_at = ? WHERE id = ?",
                (to_epoch(SystemTime::now()), &run_id),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
            tx.commit().map_err(|e| StoreError::Backend(e.to_string()))
        })
        .await
    }

    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        self.run_on_thread(move |conn| {
            let mut sql =
                "SELECT id, flow_name, flow_version, phase, created_at, updated_at FROM runs WHERE 1=1"
                    .to_string();
            // Bind only the params matching the `?`s actually pushed above --
            // a fixed-arity tuple here previously mismatched whenever fewer
            // than all three filters were set (e.g. flow_name + limit, no
            // phase), causing "wrong number of parameters".
            let mut params: Vec<Box<dyn ToSql>> = Vec::new();
            if let Some(flow_name) = &filter.flow_name {
                sql.push_str(" AND flow_name = ?");
                params.push(Box::new(flow_name.clone()));
            }
            if let Some(phase) = &filter.phase {
                sql.push_str(" AND phase = ?");
                params.push(Box::new(enum_to_str(phase)));
            }
            sql.push_str(" ORDER BY created_at");
            if let Some(limit) = filter.limit {
                sql.push_str(" LIMIT ?");
                params.push(Box::new(limit as i64));
            }

            let mut stmt = conn.prepare(&sql).map_err(|e| StoreError::Backend(e.to_string()))?;
            let param_refs: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
            let rows = stmt
                .query_map(param_refs.as_slice(), |row| {
                    Ok(RunSummary {
                        run_id: row.get(0)?,
                        flow_name: row.get(1)?,
                        flow_version: row.get(2)?,
                        phase: enum_from_str(&row.get::<_, String>(3)?)
                            .map_err(sqlite_from_store)?,
                        created_at: from_epoch(row.get(4)?).into(),
                        updated_at: from_epoch(row.get(5)?).into(),
                    })
                })
                .map_err(|e| StoreError::Backend(e.to_string()))?;

            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StoreError::Backend(e.to_string()))?);
            }
            Ok(out)
        })
        .await
    }

    async fn runs_in_phase(&self, phase: RunPhase) -> Result<Vec<RunRecord>, StoreError> {
        let phase_str = enum_to_str(&phase);
        self.run_on_thread(move |conn| {
            let ids: Vec<String> = {
                let mut stmt = conn
                    .prepare("SELECT id FROM runs WHERE phase = ? ORDER BY created_at")
                    .map_err(|e| StoreError::Backend(e.to_string()))?;
                stmt.query_map([phase_str], |row| row.get(0))
                    .map_err(|e| StoreError::Backend(e.to_string()))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| StoreError::Backend(e.to_string()))?
            };
            ids.into_iter().map(|id| load_run_sync(conn, &id)).collect()
        })
        .await
    }

    async fn link_runs(
        &self,
        parent_run: &str,
        parent_step: &str,
        child_run: &str,
    ) -> Result<(), StoreError> {
        let parent_run = parent_run.to_string();
        let parent_step = parent_step.to_string();
        let child_run = child_run.to_string();
        self.run_on_thread(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO run_links (parent_run, parent_step, child_run) VALUES (?, ?, ?)",
                (&parent_run, &parent_step, &child_run),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
            Ok(())
        })
        .await
    }

    async fn load_run_tree(&self, run_id: &str) -> Result<RunTree, StoreError> {
        let run_id = run_id.to_string();
        self.run_on_thread(move |conn| load_run_tree_sync(conn, &run_id))
            .await
    }

    async fn list_hook_runs(&self, run_id: &str) -> Result<Vec<HookRunRecord>, StoreError> {
        let run_id = run_id.to_string();
        self.run_on_thread(move |conn| list_hook_runs_sync(conn, &run_id))
            .await
    }

    async fn unpushed_events(
        &self,
        limit: usize,
    ) -> Result<Vec<(RunId, StoredRunEvent)>, StoreError> {
        self.run_on_thread(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT run_id, sequence, event_type, step_id, timestamp, payload
                     FROM run_events WHERE pushed_at IS NULL
                     ORDER BY run_id, sequence LIMIT ?",
                )
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            let rows = stmt
                .query_map([limit as i64], |row| {
                    let run_id: String = row.get(0)?;
                    let event_type_str: String = row.get(2)?;
                    let event_type: RunEventType =
                        enum_from_str(&event_type_str).map_err(sqlite_from_store)?;
                    let payload_str: String = row.get(5)?;
                    let payload: Value = parse_json(&payload_str).map_err(sqlite_from_store)?;
                    Ok((
                        run_id,
                        StoredRunEvent {
                            sequence: row.get::<_, i64>(1)? as u64,
                            event_type,
                            step_id: row.get(3)?,
                            timestamp: from_epoch(row.get(4)?),
                            payload,
                        },
                    ))
                })
                .map_err(|e| StoreError::Backend(e.to_string()))?;

            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(|e| StoreError::Backend(e.to_string()))?);
            }
            Ok(out)
        })
        .await
    }

    async fn mark_events_pushed(
        &self,
        run_id: &str,
        through_sequence: u64,
    ) -> Result<(), StoreError> {
        let run_id = run_id.to_string();
        self.run_on_thread(move |conn| {
            conn.execute(
                "UPDATE run_events SET pushed_at = ?
                 WHERE run_id = ? AND sequence <= ? AND pushed_at IS NULL",
                (
                    to_epoch(SystemTime::now()),
                    &run_id,
                    through_sequence as i64,
                ),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
            Ok(())
        })
        .await
    }
}

fn list_hook_runs_sync(
    conn: &mut Connection,
    run_id: &str,
) -> Result<Vec<HookRunRecord>, StoreError> {
    let mut stmt = conn
        .prepare(
            "SELECT hook, phase, step_id, status, job_id, args_resolved, output_ref, failure_reason,
                    failure_detail, started_at, completed_at
             FROM hook_runs WHERE run_id = ? ORDER BY started_at, id",
        )
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    let rows = stmt
        .query_map([run_id], |row| {
            Ok(HookRunRecord {
                hook: row.get(0)?,
                phase: enum_from_str(&row.get::<_, String>(1)?).map_err(sqlite_from_store)?,
                step_id: row.get(2)?,
                status: enum_from_str(&row.get::<_, String>(3)?).map_err(sqlite_from_store)?,
                job_id: row.get(4)?,
                args_resolved: row
                    .get::<_, Option<String>>(5)?
                    .map(|s| parse_json(&s))
                    .transpose()
                    .map_err(sqlite_from_store)?,
                output_ref: row.get(6)?,
                failure_reason: row.get(7)?,
                failure_detail: row
                    .get::<_, Option<String>>(8)?
                    .map(|s| from_json(&s))
                    .transpose()
                    .map_err(sqlite_from_store)?,
                started_at: from_epoch(row.get(9)?),
                completed_at: row.get::<_, Option<i64>>(10)?.map(from_epoch),
            })
        })
        .map_err(|e| StoreError::Backend(e.to_string()))?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| StoreError::Backend(e.to_string()))?);
    }
    Ok(out)
}

fn load_run_sync(conn: &mut Connection, run_id: &str) -> Result<RunRecord, StoreError> {
    let run_row = conn
        .query_row(
            "SELECT flow_name, flow_version, definition, inputs, trigger, phase, cancelled, created_at, updated_at, idempotency_key
             FROM runs WHERE id = ?",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i32>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .optional()
        .map_err(|e| StoreError::Backend(e.to_string()))?
        .ok_or_else(|| StoreError::NotFound(run_id.into()))?;

    let (
        flow_name,
        flow_version,
        definition_json,
        inputs_json,
        trigger_json,
        phase_str,
        cancelled,
        created_at,
        updated_at,
        idempotency_key,
    ) = run_row;

    let definition: FlowDefinition = from_json(&definition_json)?;
    let inputs: Value = from_json(&inputs_json)?;
    let trigger: Value = from_json(&trigger_json)?;

    let mut stmt = conn
        .prepare(
            "SELECT step_id, status, attempt, job_id, with_resolved, input_resolved, output, failure_reason,
                    approval_status, approval_comment, feedback, child_run_id, started_at, completed_at, failure_detail
             FROM step_runs WHERE run_id = ? ORDER BY step_id, attempt",
        )
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    let steps = stmt
        .query_map([run_id], |row| {
            Ok(StepRecord {
                run: flowspec_domain::run::types::StepRun {
                    step_id: row.get(0)?,
                    status: enum_from_str(&row.get::<_, String>(1)?).map_err(sqlite_from_store)?,
                    attempt: row.get::<_, i64>(2)? as u32,
                    started_at: row.get::<_, Option<i64>>(12)?.map(from_epoch),
                    completed_at: row.get::<_, Option<i64>>(13)?.map(from_epoch),
                    input_resolved: row.get(5)?,
                    output: row
                        .get::<_, Option<String>>(6)?
                        .map(|s| parse_json(&s))
                        .transpose()
                        .map_err(sqlite_from_store)?,
                    approval_status: row
                        .get::<_, Option<String>>(8)?
                        .map(|s| enum_from_str(&s))
                        .transpose()
                        .map_err(sqlite_from_store)?,
                    feedback: row.get(10)?,
                    approval_comment: row.get(9)?,
                    failure_reason: row.get(7)?,
                    child_run_id: row.get(11)?,
                },
                job_id: row.get(3)?,
                with_resolved: row
                    .get::<_, Option<String>>(4)?
                    .map(|s| parse_json(&s))
                    .transpose()
                    .map_err(sqlite_from_store)?,
                failure_detail: row
                    .get::<_, Option<String>>(14)?
                    .map(|s| from_json(&s))
                    .transpose()
                    .map_err(sqlite_from_store)?,
            })
        })
        .map_err(|e| StoreError::Backend(e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| StoreError::Backend(e.to_string()))?;

    Ok(RunRecord {
        run_id: run_id.into(),
        flow_name,
        flow_version,
        definition,
        inputs,
        trigger,
        phase: enum_from_str(&phase_str)?,
        cancelled: cancelled != 0,
        created_at: from_epoch(created_at),
        updated_at: from_epoch(updated_at),
        idempotency_key,
        steps,
    })
}

fn load_run_tree_sync(conn: &mut Connection, run_id: &str) -> Result<RunTree, StoreError> {
    let root = load_run_sync(conn, run_id)?;
    let child_ids: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT child_run FROM run_links WHERE parent_run = ?")
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        stmt.query_map([run_id], |row| row.get(0))
            .map_err(|e| StoreError::Backend(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StoreError::Backend(e.to_string()))?
    };

    let children = child_ids
        .into_iter()
        .map(|id| load_run_tree_sync(conn, &id))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RunTree { root, children })
}

fn apply_mutation(tx: &Transaction, run_id: &str, m: Mutation) -> Result<(), StoreError> {
    match m {
        Mutation::InsertStepRun(step) => {
            tx.execute(
                "INSERT OR REPLACE INTO step_runs
                 (run_id, step_id, status, attempt, job_id, with_resolved, input_resolved, output,
                  failure_reason, failure_detail, approval_status, approval_comment, feedback, child_run_id, started_at, completed_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    &step.run.step_id,
                    enum_to_str(&step.run.status),
                    step.run.attempt as i64,
                    step.job_id.as_ref(),
                    step.with_resolved.as_ref().map(to_json).transpose()?.as_ref(),
                    step.run.input_resolved.as_ref(),
                    step.run.output.as_ref().map(to_json).transpose()?.as_ref(),
                    step.run.failure_reason.as_ref(),
                    step.failure_detail.as_ref().map(to_json).transpose()?.as_ref(),
                    step.run.approval_status.as_ref().map(enum_to_str).as_ref(),
                    step.run.approval_comment.as_ref(),
                    step.run.feedback.as_ref(),
                    step.run.child_run_id.as_ref(),
                    step.run.started_at.map(to_epoch),
                    step.run.completed_at.map(to_epoch),
                ),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
        Mutation::SetStepStatus {
            step_id,
            attempt,
            status,
        } => {
            update_step(
                tx,
                run_id,
                &step_id,
                attempt,
                "status",
                enum_to_str(&status),
            )?;
        }
        Mutation::SetStepOutput {
            step_id,
            attempt,
            output,
        } => {
            update_step(tx, run_id, &step_id, attempt, "output", to_json(&output)?)?;
        }
        Mutation::SetJobId {
            step_id,
            attempt,
            job_id,
        } => {
            update_step(tx, run_id, &step_id, attempt, "job_id", job_id)?;
        }
        // SetWithResolved / SetInputResolved were folded into InsertStepRun:
        // the resolved `with:` snapshot and the resolved `input` string are
        // written once, with the step row, in the same transaction.
        Mutation::SetApproval {
            step_id,
            attempt,
            approval_status,
            comment,
            feedback,
        } => {
            let n = tx
                .execute(
                    "UPDATE step_runs
                 SET approval_status = ?, approval_comment = ?, feedback = ?
                 WHERE run_id = ? AND step_id = ? AND attempt = ?",
                    (
                        enum_to_str(&approval_status),
                        comment.as_ref(),
                        feedback.as_ref(),
                        run_id,
                        &step_id,
                        attempt as i64,
                    ),
                )
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            if n == 0 {
                return Err(StoreError::NotFound(format!(
                    "step not found: {}/{}/{}",
                    run_id, step_id, attempt
                )));
            }
        }
        Mutation::SetStepFailure {
            step_id,
            attempt,
            reason,
        } => {
            update_step(tx, run_id, &step_id, attempt, "failure_reason", reason)?;
        }
        Mutation::SetStepFailureDetail {
            step_id,
            attempt,
            detail,
        } => {
            update_step(
                tx,
                run_id,
                &step_id,
                attempt,
                "failure_detail",
                to_json(&detail)?,
            )?;
        }
        Mutation::SetStepTimestamps {
            step_id,
            attempt,
            started_at,
            completed_at,
        } => {
            let n = tx
                .execute(
                    "UPDATE step_runs SET started_at = ?, completed_at = ?
                 WHERE run_id = ? AND step_id = ? AND attempt = ?",
                    (
                        started_at.map(to_epoch),
                        completed_at.map(to_epoch),
                        run_id,
                        &step_id,
                        attempt as i64,
                    ),
                )
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            if n == 0 {
                return Err(StoreError::NotFound(format!(
                    "step not found: {}/{}/{}",
                    run_id, step_id, attempt
                )));
            }
        }
        Mutation::SetChildRunId {
            step_id,
            attempt,
            child_run_id,
        } => {
            update_step(tx, run_id, &step_id, attempt, "child_run_id", child_run_id)?;
        }
        Mutation::SetRunPhase(phase) => {
            tx.execute(
                "UPDATE runs SET phase = ? WHERE id = ?",
                (enum_to_str(&phase), run_id),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
        Mutation::SetRunCancelled => {
            tx.execute("UPDATE runs SET cancelled = 1 WHERE id = ?", [run_id])
                .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
        Mutation::RecordHookRun(hook) => {
            tx.execute(
                "INSERT INTO hook_runs (run_id, hook, phase, step_id, status, job_id, args_resolved, output_ref, failure_reason, failure_detail, started_at, completed_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    &hook.hook,
                    enum_to_str(&hook.phase),
                    hook.step_id.as_ref(),
                    enum_to_str(&hook.status),
                    hook.job_id.as_ref(),
                    hook.args_resolved.as_ref().map(to_json).transpose()?.as_ref(),
                    hook.output_ref.as_ref(),
                    hook.failure_reason.as_ref(),
                    hook.failure_detail.as_ref().map(to_json).transpose()?.as_ref(),
                    to_epoch(hook.started_at),
                    hook.completed_at.map(to_epoch),
                ),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
        Mutation::AppendEvent(event) => {
            let next_sequence: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_events WHERE run_id = ?",
                    [run_id],
                    |row| row.get(0),
                )
                .map_err(|e| StoreError::Backend(e.to_string()))?;
            tx.execute(
                "INSERT INTO run_events (run_id, sequence, event_type, step_id, timestamp, payload)
                 VALUES (?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    next_sequence,
                    enum_to_str(&event.event_type),
                    event.step_id.as_ref(),
                    to_epoch(event.timestamp),
                    to_json(&event.payload)?,
                ),
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        }
    }
    Ok(())
}

fn update_step(
    tx: &Transaction,
    run_id: &str,
    step_id: &str,
    attempt: u32,
    column: &str,
    value: impl rusqlite::ToSql,
) -> Result<(), StoreError> {
    let sql = format!(
        "UPDATE step_runs SET {} = ? WHERE run_id = ? AND step_id = ? AND attempt = ?",
        column
    );
    let n = tx
        .execute(&sql, (value, run_id, step_id, attempt as i64))
        .map_err(|e| StoreError::Backend(e.to_string()))?;
    if n == 0 {
        return Err(StoreError::NotFound(format!(
            "step not found: {}/{}/{}",
            run_id, step_id, attempt
        )));
    }
    Ok(())
}

fn map_sqlite_err(e: rusqlite::Error, idem: Option<&str>) -> StoreError {
    if let rusqlite::Error::SqliteFailure(code, _) = &e
        && code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
        && let Some(key) = idem
    {
        return StoreError::Duplicate(key.into());
    }
    StoreError::Backend(e.to_string())
}

fn sqlite_from_store(e: StoreError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::other(e.to_string())),
    )
}

fn to_json<T: Serialize>(v: &T) -> Result<String, StoreError> {
    serde_json::to_string(v).map_err(|e| StoreError::Serialization(e.to_string()))
}

fn from_json<T: for<'de> Deserialize<'de>>(s: &str) -> Result<T, StoreError> {
    serde_json::from_str(s).map_err(|e| StoreError::Serialization(e.to_string()))
}

fn parse_json(s: &str) -> Result<Value, StoreError> {
    serde_json::from_str(s).map_err(|e| StoreError::Serialization(e.to_string()))
}

fn enum_to_str<T: Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .expect("enum serialization must produce a JSON value")
        .as_str()
        .expect("enum serialization must produce a string")
        .to_string()
}

fn enum_from_str<T: for<'de> Deserialize<'de>>(s: &str) -> Result<T, StoreError> {
    serde_json::from_value(Value::String(s.into()))
        .map_err(|e| StoreError::Serialization(e.to_string()))
}

fn to_epoch(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn from_epoch(secs: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs.max(0) as u64)
}
