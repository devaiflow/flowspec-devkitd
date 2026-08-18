CREATE TABLE IF NOT EXISTS runs (
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_idempotency_key
    ON runs(idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS step_runs (
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    job_id TEXT NULL,
    with_resolved TEXT NULL,
    input_resolved TEXT NULL,
    output TEXT NULL,
    failure_reason TEXT NULL,
    -- JSON-encoded JobFailure (crate::wire), the structured sibling of
    -- failure_reason. Added after the initial schema, so new databases get
    -- it via CREATE TABLE; existing ones get it via the ensure_column
    -- migration run in SqliteStore::open (there is no other migration path).
    failure_detail TEXT NULL,
    approval_status TEXT NULL,
    approval_comment TEXT NULL,
    feedback TEXT NULL,
    child_run_id TEXT NULL,
    started_at INTEGER NULL,
    completed_at INTEGER NULL,
    PRIMARY KEY (run_id, step_id, attempt),
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_step_runs_run_id
    ON step_runs(run_id);

CREATE TABLE IF NOT EXISTS hook_runs (
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
    -- See step_runs.failure_detail above.
    failure_detail TEXT NULL,
    started_at INTEGER NOT NULL,
    completed_at INTEGER NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_hook_runs_run_id
    ON hook_runs(run_id);

CREATE TABLE IF NOT EXISTS run_links (
    parent_run TEXT NOT NULL,
    parent_step TEXT NOT NULL,
    child_run TEXT NOT NULL PRIMARY KEY,
    FOREIGN KEY (parent_run) REFERENCES runs(id),
    FOREIGN KEY (child_run) REFERENCES runs(id)
);

-- Durable outbox for the platform connector's run-event pump. Written in the
-- same transaction as the state mutations that caused each event; sequence
-- is assigned here (MAX(sequence)+1 per run), never by the app layer.
-- Timestamps are INTEGER epoch seconds, matching every other table.
CREATE TABLE IF NOT EXISTS run_events (
    run_id     TEXT NOT NULL,
    sequence   INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    step_id    TEXT NULL,
    timestamp  INTEGER NOT NULL,
    payload    TEXT NOT NULL,
    pushed_at  INTEGER NULL,
    PRIMARY KEY (run_id, sequence),
    FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_run_events_unpushed
    ON run_events(run_id, sequence)
    WHERE pushed_at IS NULL;
