# flowspec-devkitd — Progress & Patch Log

Living log of *actual* implementation state: what shipped, where the code diverged from the
`plans/*.md` intent and why, patches applied, and what each phase deliberately hands to the next.
Append a section per phase. This is **not** a spec duplicate — only what code + git history don't
already say.

Last updated: 2026-08-09 (Phase 5.4, rmcp 0.16 → 3.1.2, done; 5.2 host validation still pending).

---

## Phase status matrix

| Phase | Status | Scope |
| --- | --- | --- |
| 0 | ✅ done | Workspace, crates, CI, layering guard, hello-MCP |
| 1 | ✅ done | Pure domain: flow schema, validation, DAG, templating |
| 2 | ✅ done | Persistence: `StateStore` + `Mutation`, in-memory + SQLite, `recovery_plan` |
| 3 | ✅ done | Execution loop vs fakes: scheduler, ports, fakes, use cases, integration suite |
| 4 | ✅ done | Real devkitd adapter (rmcp/MCP), tracer bullet, failure/cancel/re-attach e2e |
| 5 | 🟡 5.1 + 5.4 done, 5.2 pending | MCP tool surface (10 tools) done and tested; upgraded to rmcp 3.1.2; OpenClaw host validation (`docs/openclaw-validation.md`) not yet run |
| 6 | ⏳ pending | Subflow dispatch + chaos testing |

Verification at end of Phase 5.4: `cargo test --workspace` = **123 passed, 8 ignored** (the 8 need a
live devkitd, unchanged from Phase 4); `just lint` green (fmt + clippy `-D warnings` + layering).

---

## Phase 5 — MCP tool surface (`plans/plan5.md`)

**Scope:** the ten-tool MCP surface (`list_flows`, `start_flow`, `get_run_status`,
`pending_approvals`, `approve_step`, `reject_step`, `cancel_run`, `get_step_output`, `list_runs`,
`get_run_tree`), wired to the real `Container` in `crates/flowspec-server/src/mcp_server/`. 5.1 is
done; 5.2 (live OpenClaw validation) is documented in `docs/openclaw-validation.md` but not yet run
— that requires a live devkitd + OpenClaw and is explicitly out of scope for an automated pass.

**Decisions (per `plans/plan5.md`):**

1. **`artifacts[]` deferred.** No producer exists anywhere in the stack (domain has no field,
   devkitd emits none); shipping an always-empty field would mislead the host. Revisit alongside
   Phase 6 subflow work if a real artifact producer shows up.
2. **Timestamps are RFC3339 strings on the wire**, not serde's default
   `{secs_since_epoch, nanos}`. New `flowspec_app::wire::Timestamp` newtype (workspace `time` dep)
   used on every response DTO; `SystemTime` stays internal to `RunRecord`/`StepRun`.
3. ~~**Dual-emit tool results.**~~ **Retracted in Phase 5.4 — this decision was wrong from the
   start, on rmcp 0.16 as much as later.** rmcp's built-in `Json<T>` was never `content`-empty; its
   `into_call_tool_result` already called `CallToolResult::structured`, the exact same call
   `mcp_server::result::JsonOut<T>` made. `JsonOut` was a line-for-line reimplementation of `Json`
   the whole time. Deleted in Phase 5.4; tools now return `rmcp::handler::server::wrapper::Json<T>`
   directly.
4. ~~**`ToolResult<T>` instead of `Result<JsonOut<T>, ToolFailure>` directly.**~~ **Superseded by
   rmcp 3.x in Phase 5.4.** True as stated for 0.16 (the orphan rules blocked
   `impl IntoCallToolResult for Result<Json<T>, ToolFailure>` because 0.16's blanket impl only
   covered `IntoContents`). 3.1.2 generalised the blanket impl to
   `impl<T: IntoCallToolResult, E: IntoCallToolResult> IntoCallToolResult for Result<T, E>`, which
   covers `Result<Json<T>, ToolFailure>` directly with identical semantics (Err still yields a
   successful call with `is_error: true`). `ToolResult<T>` and `mcp_server/result.rs` deleted; tool
   signatures are now `Result<Json<T>, ToolFailure>`.
5. **MCP forbade an array-rooted `outputSchema` on rmcp 0.16.** `list_flows`, `pending_approvals`,
   and `list_runs` wrap their `Vec<T>` in a one-field response struct (`FlowsResponse { flows }`,
   `PendingApprovalsResponse { pending }`, `RunsResponse { runs }`) — discovered by the
   `#[tool(output_schema = ...)]` macro attribute panicking at startup ("MCP specification requires
   tool outputSchema to have root type 'object', but found 'array'"). **Superseded by rmcp 3.x**:
   the root-object restriction now applies only to `inputSchema`, not `outputSchema` — the wrappers
   are kept anyway (a named field reads better to an LLM host) but are now a choice, not a
   constraint. The ten hand-written `output_schema = schema_for_output::<T>().unwrap()` attributes
   are also gone: the `#[tool]` macro now auto-derives the schema for both `Json<T>` and
   `Result<Json<T>, E>` return types (0.16's macro only recognised the bare `Json<T>` form).
6. **`get_run_tree` returns a projected `RunTreeNode`** (`run_id`, `flow_name`, `flow_version`,
   `phase`, `children`), not the raw `ports::RunTree` (which embeds full `RunRecord`s, including the
   snapshotted `FlowDefinition` — deriving `JsonSchema` through that whole domain object graph
   wasn't worth it for Phase 5). Real per-run content depth is a Phase 6 item, matching the original
   phase5.md note.
7. **Idempotent `start_flow` is a real behavioural change**, not just a schema addition. The SQLite
   unique index on `idempotency_key` already existed but `start_flow` used to propagate the conflict
   as an error. New `StateStore::find_by_idempotency_key` port method; `start_flow` looks it up
   first (hit → replay, no second `RunStarted`) and also re-checks on a racing
   `StoreError::Duplicate` from `create_run` (replay instead of erroring). `StartFlowResponse` gained
   `replayed: bool`.
8. **`LivenessProbe` needed `+ Send + Sync`** on its trait object (`&dyn Fn(...)` isn't `Send` by
   default) — the `#[tool]`-generated async fn body must be `Send`, and `get_run_status`'s liveness
   closure crosses an `.await` point inside it.

**Tests:** `crates/flowspec-app/tests/use_case_queries.rs` (idempotent replay, attempt selection in
`get_step_output`, `pending_approvals` run_id filter, `parse_phase` rejection) plus
`find_by_idempotency_key` unit tests in both `in_memory_store.rs` and (server crate)
`state_roundtrip.rs`. `crates/flowspec-server/tests/mcp_tools.rs` (13 tests, driven through a real
rmcp client against an in-process server over `flowspec-app`'s in-memory fakes — no live devkitd, no
SQLite file): an `insta` snapshot of the full `list_tools` response (names, descriptions, input
**and** output schemas — the tool contract as a reviewed artifact), per-tool happy paths asserting
both `content` and `structured_content`, the error contract (`NotApprovable`, `RunNotFound`,
`FlowNotFound`, `InvalidPhase`, and a genuine protocol-level `-32602` for malformed arguments), a
full lifecycle through the client (`list_flows` → `start_flow` → `pending_approvals` →
`approve_step` → `get_run_status` → `get_step_output`), and idempotent replay through the wire.

**Known follow-up:** 5.2 (OpenClaw host validation, `docs/openclaw-validation.md`) has not been run
yet — it needs a live devkitd and a running OpenClaw instance, which weren't available for this
pass. Whoever runs it should fold the mistake-log findings back into `mcp_server/server.rs`'s tool
descriptions and re-snapshot.

---

## Phase 5.4 — rmcp 0.16.0 → 3.1.2 (`PLAN-UPDATE-RMCP.md`)

**Scope:** bump the workspace `rmcp` pin, delete the three 0.16-only workarounds (decisions 3-5
above), thread through the breaking-change fallout, re-snapshot the tool contract, and get
`cargo test --workspace` back to the Phase 5.1 baseline (123 passed, 8 ignored). Done ahead of
Phase 5.2 so the OpenClaw validation runs once, against the final contract, on the final rmcp
version — see `PLAN-UPDATE-RMCP.md` for the full crate-source-verified rationale.

**What changed in `src/`:**

- `mcp_server/result.rs` deleted (67 lines: `JsonOut`, `ToolResult`). Tools return
  `rmcp::handler::server::wrapper::Json<T>` (infallible `list_flows`) or
  `Result<Json<T>, ToolFailure>` (the other nine) directly; all ten hand-written
  `output_schema = schema_for_output::<T>().unwrap()` attributes are gone, macro-derived instead.
- `Content`/`RawContent`/`Annotated<T>` → `ContentBlock` (rmcp 2.0 unified the content model; a
  block *is* the enum now, no `.raw` projection) — `mcp_server/error.rs`'s `IntoContents` impl and
  `devkitd/client.rs`'s `first_text` (the double-decode's layer-1 entry point).
- `CallToolRequestParams` and `StreamableHttpServerConfig` are both `#[non_exhaustive]` now — every
  struct-literal construction (`devkitd/client.rs::invoke`, `mcp_server/mod.rs::serve`) became a
  `::new(...)`/builder-method chain (`.with_arguments(...)`, `.with_allowed_hosts(...)`, etc.).
  `ServerInfo` (= `InitializeResult`) lost its `Default` impl for the same reason; `get_info` now
  builds it via `ServerInfo::new(capabilities).with_instructions(...)`.
- **`allowed_hosts` — not anticipated by `PLAN-UPDATE-RMCP.md`, found during implementation.** rmcp
  3.x added DNS-rebinding protection: `StreamableHttpServerConfig::default()` only accepts
  `Host: localhost|127.0.0.1|::1` and 403s everything else. `mcp_server::serve` built its config
  with `..Default::default()`, so this would have compiled clean and then silently 403'd OpenClaw
  the moment Phase 5.2 pointed it at a non-loopback `listen_addr` over Tailscale. Fixed by adding
  `Config::allowed_hosts` (`crates/flowspec-server/src/config.rs`, defaults to the same loopback
  list rmcp uses) and setting it explicitly in `mcp_server::serve`. **For the Phase 5.2 run:**
  `listen_addr` must be non-loopback and `allowed_hosts` must list the exact `host:port` authority
  OpenClaw dials — see `docs/openclaw-validation.md`.
- `#[tool_handler]`'s default router expression changed between versions:  0.16 defaulted to the
  cached `self.tool_router` field (built once in `FlowspecServer::new`); 3.1.2 defaults to
  `Self::tool_router()`, rebuilding the router from scratch on every `call_tool`. Silent behavior
  change, caught by clippy's `dead_code` lint on the now-unread field. Pinned back to the cached
  field: `#[tool_handler(router = self.tool_router.clone())]`.
- `mcp_server/server.rs`'s wrapper-struct comment corrected: the array-root `outputSchema`
  restriction that motivated `FlowsResponse`/`PendingApprovalsResponse`/`RunsResponse` no longer
  exists in 3.x (see decision 5, retracted above); the wrappers stay for readability, not necessity.

**What changed in `tests/`:** mechanical follow-through of the above (`ContentBlock`, builder-style
`CallToolRequestParams`/`StreamableHttpServerConfig` construction, `.into()` on the two hand-written
`ServerHandler::call_tool` stub impls in `tests/devkitd_client.rs` and `tests/reattach_stub.rs` now
that `IntoCallToolResult` returns `Result<CallToolResponse, ErrorData>` instead of
`Result<CallToolResult, ErrorData>`), plus `tests/support/mod.rs`'s `fake_config()` gaining the new
`allowed_hosts` field.

**One real behavioral change caught by the test suite, not anticipated by the plan:** rmcp 3.x
converts `Parameters<T>` argument-deserialization failures from a JSON-RPC `-32602` protocol error
into a tool-result error (`is_error: true`, plain-text `content`) — deliberately, matching the
"let the host recover conversationally" philosophy our own `ToolFailure` channel already uses
(`mcp_server/error.rs`'s header doc). `tests/mcp_tools.rs`'s
`malformed_arguments_are_a_protocol_error_not_a_tool_result` renamed to
`malformed_arguments_surface_as_a_tool_result_error` and rewritten to assert the new shape.

**Verification:** `cargo build --workspace` clean; `just lint` green (fmt + clippy `-D warnings` +
layering); `cargo test --workspace` = 123 passed, 8 ignored (unchanged from Phase 5.1); tool-schema
snapshot re-generated and reviewed by hand — every diff line was the schemars null-representation
switch (`nullable: true`/`const: null` → 2020-12-idiomatic `type: [T, "null"]`) and dropped `title:`
fields, both from rmcp's internal schema-generator settings, uniform across every input and output
schema and independent of the decision-3/4/5 workaround removal; no field renamed, added, removed,
or retyped. `cargo tree -d` shows a single version each of `rmcp` (3.1.2), `reqwest` (0.13.4),
`schemars` (1.2.1) — no duplicate majors. `mcp_server/` net line count: 524 → 415 (-109 lines, all
from deleting `result.rs` and the ten `output_schema` attributes).

**Not touched:** `devkitd/client.rs`'s client-side lifecycle (`().serve(transport)`, still
negotiating protocol `2025-11-25` against devkitd's rmcp 2.1.0 server — 3.1.2's `LATEST` is still
`2025-11-25`, the `2026-07-28` stateless lifecycle is opt-in and only relevant once devkitd itself
upgrades). Live devkitd verification (`just tracer`, the 8 `#[ignore]`d tests, `e2e_semantics.rs`,
`just chain`) is a manual follow-up, not run as part of this pass.

---

## Phase 5.4.1 — `list_runs` "wrong number of parameters" bug

Found live, on a homelab `flowspec-server` deployment: filtering by `flow_name` + `limit` (no
`phase`) threw `"Wrong number of parameters passed to query. Got 3, needed 2"`. Root cause:
`sqlite_store.rs::list_runs` built its SQL string with `?` placeholders pushed conditionally per
filter, but always bound a fixed 3-tuple `(flow_name, phase, limit)` regardless of how many `?`s
were actually in the string — any filter combination other than "all three" or "none" mismatched.
Fixed by building `Vec<Box<dyn ToSql>>` conditionally, one push per `?` pushed into the SQL, then
passing `param_refs.as_slice()` to `query_map`. Regression test:
`tests/state_roundtrip.rs::list_runs_with_a_partial_filter_combination_does_not_panic`, exercising
all six filter combinations. Verified: `cargo test -p flowspec-server --test state_roundtrip`
(10/10), `just lint`, `cargo test --workspace` unaffected elsewhere.

---

## Phase 5.6 — hook and step failures visible and structured

**Problem, found on the same homelab session:** a `devkit-chain` run failed in ~5s with `phase:
"failed"` and `steps: []`. No MCP tool, and no server log at any `RUST_LOG` level, said why. The
real cause (`create-feature.sh: line 14: PROVISIONER_PASSWORD: Error: PROVISIONER_PASSWORD env var
is not set`) was only recoverable by curling devkitd's `job-status` directly with a `job_id`
scraped from debug logs. The data was never missing — `run_hook` already persisted the hook's
failure into the `hook_runs` table — it was unreachable: no `StateStore` method read the table,
and `hook_batch_finished`'s `(HookPhase::BeforeRun, Some((_, _reason)))` arm bound the reason to
`_reason` and discarded it before flipping the run to `failed`.

**What shipped:**

- `wire::JobFailure { kind, exit_code, stdout, stderr }` — one structured failure shape, shared by
  steps and hooks (they already execute through the same `Devkitd` port; `ports.rs`'s own doc
  comment says as much). `impl From<&DevkitdError> for JobFailure` covers all five variants.
  `failure_reason`/`failure_summary` string fields are unchanged everywhere — `JobFailure` is their
  machine-readable sibling, not a replacement.
- `step_runs.failure_detail` / `hook_runs.failure_detail` (JSON) — new nullable columns. **No
  migration mechanism exists** (`schema.sql` is pure `CREATE ... IF NOT EXISTS`, silently ignored on
  a table that already exists), so `SqliteStore::open` now runs `ensure_column` after `apply_schema`:
  a `PRAGMA table_info` check + guarded `ALTER TABLE ... ADD COLUMN`. Covered by
  `state_roundtrip.rs::opening_a_pre_migration_database_adds_the_missing_columns`, which builds a DB
  against the literal pre-change schema and confirms the new binary opens and writes to it. (Not
  verified against the homelab's actual `flowspec.db` file — that's the one step worth doing by hand
  before deploying this.)
- `StateStore::list_hook_runs(run_id)` — the missing read path. `Mutation::SetStepFailureDetail`,
  applied directly from the job task that still holds the live `DevkitdError` (`scheduler.rs`'s two
  `spawn_step_task` error arms), independently of the existing `SetStepFailure` (string) mutation.
  `run_hook` restructured so every exit — including the two that previously `?`-ed out before any
  `HookRunRecord` existed (template resolution, `devkitd.start`) — funnels through one record
  construction; added `tracing::warn!` on every hook failure, and on the `before_run` gate reason
  that used to be silently discarded.
- Surfaced **on the thing each hook is declared on**, matching the flow schema
  (`LifecycleHooks.before_run`/`after_run`, `Step.after`) rather than a new flat tool: `get_run_status`
  gained `run_hooks: Vec<HookInfo>` (this run's `before_run`/`after_run` executions) and, per step,
  `hooks: Vec<HookInfo>` (that step's `after:` executions) plus `failed_in: Option<String>`
  (`"step"` vs `"after_hook"`, derived at query time from whether a failed hook matches the step_id —
  distinguishes "the agent failed" from "the agent succeeded but its follow-up hook didn't").
  `get_step_output` and `StepInfo` both gained `failure: Option<JobFailure>`. No new MCP tool.
- Tests: `wire.rs` unit tests on the `DevkitdError` mapping; two `flowspec-app/tests/scheduler.rs`
  tests extended (not added — same fixtures, extra assertions) to cover the before_run-gate case, the
  after_hook-attribution case, and a plain step failure's structured detail; the migration test above;
  two new `mcp_tools.rs` tests (`get_run_status_surfaces_a_failed_before_run_hook` — the literal
  homelab reproduction with `PROVISIONER_PASSWORD` as the stderr — and the `RunNotFound` contract for
  an unknown `run_id`); tool-schema snapshot re-generated and reviewed by hand (only `HookInfo`/
  `JobFailure` defs and the four new fields — no tool added/removed, no existing field changed).

**Known defect found, left unfixed (separate change):** `Step.before` — a per-step gate distinct
from the flow-level `before_run` used above — is parsed (`flow/types.rs::Step.before`), has a
`HookPhase::BeforeStep`, and is handled in four `scheduler.rs` branches, but **nothing ever
constructs `Command::RunHooks { phase: BeforeStep }`**; `engine.rs` only emits `RunHooks` for
`BeforeRun`, `AfterRun`, and `AfterStep`. A flow declaring `before:` on a step parses fine and the
hook silently never runs. Out of scope here because making an unexecuted lifecycle phase start
executing is a behavior change needing its own engine tests, not a debugging-visibility fix.
`failed_in`'s `"after_hook"` value is reserved for `before_hook` once this lands.

**Verification:** `cargo build --workspace` clean; `cargo fmt --check`, `cargo clippy --workspace -- 
-D warnings`, `./scripts/check-layering.sh` all clean; `cargo test --workspace` = 129 passed, 8
ignored (up from the 124 baseline: +3 from this session's `list_runs`/`JobFailure`-mapping unit
tests plus the migration test, +2 from the new `mcp_tools.rs` tests). Live-devkitd verification
against a real failing hook (confirming the diagnosis is reachable from `get_run_status` alone, no
curl) not yet run — do this before the next Phase 5.2 attempt.

---

## Phase 3 — decisions & divergences from `plans/plan3.md`

- **Two-batch activate (not "one apply per decide").** Plan §3.3 said one atomic `apply` batch per
  decide. `activate_steps` instead flushes accumulated mutations *early* (`store.apply(mem::take)`)
  so the `InsertStepRun` row exists **before** the spawned step task writes its `job_id`; remaining
  commands apply in a second batch. Intentional — spawn needs the persisted row. Consequence: on the
  SQLite adapter each `apply` is its own transaction, so a crash between the two leaves partial run
  state. The window is **recoverable** (boot recovery re-derives), but the plan's atomicity wording
  was retired to match reality (`plans/plan3.md` §3.3 updated).
- **`with_resolved` / `input_resolved` folded into `InsertStepRun`.** Plan decision 2 said persist
  via `Mutation::SetWithResolved` + `SetInputResolved`. Implementation writes both fields directly in
  the `InsertStepRun` StepRecord (one write, cleaner). Those two `Mutation` variants were **removed**
  as dead code from `ports.rs`, `testkit.rs`, `sqlite_store.rs`.
- **Hooks are a separate inline machine, not literally the step machine.** Plan decision 3 said "one
  execution machine." In practice `run_hook` is its own inline `start → wait` (own path, results land
  in `hook_runs` via `RecordHookRun`, not step routing). Hook batch tasks are now tracked in
  `hook_inflight` and aborted on `shutdown()` (jobs left running, re-attached next boot) — matching
  the step contract.

## Phase 3 — patches applied (remediation)

Two of these are **latent stuck-Running bugs** the original untested hook path hid:

- **Bug: `after_run` hook failure hung the run.** `hook_batch_finished`'s `(AfterRun, Some(_))` arm
  was labeled "audit-only" but never drove the `MarkRunTerminal` continuation → run stuck `Running`
  after a failing post-completion hook. Fix: removed the arm so `after_run` failures fall through to
  the catch-all that drives the continuation.
- **Bug: `before_run` `always_continue` ignored.** `spawn_hook_batch`'s loop blocked the gate on any
  `before_run` failure. Fix: `BeforeRun` gate = `!always_continue`; `AfterRun` hooks are independent
  audit-only (no abort on failure).
- **`RecoveryAction::ReissueRunStarted`.** A `Running` run with zero step rows = interrupted mid
  `before_run` gating (hooks are detached tasks `recovery_plan` can't re-attach). Recovery now
  re-emits `RunStarted` so the run makes forward progress instead of hanging.
- **`deadline_margin_secs` now applied** in `spawn_step_task` (deadline = `now + timeout + margin`),
  giving devkitd its server-side timeout window before the runtime force-cancels.
- **`check-layering.sh`** now also asserts `cargo tree -i rusqlite -p flowspec-app` fails — the
  no-adapter-leak rule is guarded against regression.
- New fixtures: `hooks.yaml`, `hooks-gating-failure.yaml`, `hooks-always-continue.yaml`; 6 hook tests
  + 1 `ReissueRunStarted` unit test. Clippy `type_complexity` cleaned via aliases
  (`InflightMap`, `HookInflightMap`, `HooksContinuation`, `LivenessProbe`).

---

## Phase 4 — real devkitd adapter, tracer, e2e semantics

`plans/phase4.md` (intent) and `plans/plan4.md` (execution, written against devkitd's
`feat-refactor` branch) both predate a few upstream changes; corrected here rather than re-editing
the historical plan docs.

### Divergences from `plans/plan4.md` (devkitd's code moved since it was written)

1. **No server-side timeout cap.** plan4 divergence 1 said the 600s global default caps every job.
   Current devkitd (`core/executor.rs:133-136`) removed that cap — `_timeout_seconds` is
   authoritative with no ceiling. The 600s config value is only a *fallback* for jobs that omit the
   override.
2. **Terminal job retention is 24h, not 15min.** plan4 divergence 3 was reading `JobRegistry::default()`
   (a test-only constructor, 15min). The real config default (`devkitd.toml`, `config/mod.rs`) is
   `job_retention_seconds = 86400`. Purge is still opportunistic-on-insert only, terminal jobs only.
3. **The tool is `containers-up`, not `containers-start`.** `containers-start.sh` is that plugin's
   *executable*, not its MCP tool name — a naming mismatch worth remembering when writing fixtures
   from memory.
4. **`_timeout_seconds` (underscore, string-valued), sentinel `-1`/`-2` semantics, unmapped-arg
   drop, and `null`→`""` stringification** were all as plan4 described and are implemented in
   `crates/flowspec-server/src/devkitd/client.rs` exactly per that decode table.

### What shipped

- `FsFlowSource` (`crates/flowspec-server/src/flows/mod.rs`) — fails fast at construction; the
  semver selection logic was extracted to `flowspec_app::flow_source::select_flow` and is now shared
  with `InMemoryFlowSource` rather than duplicated.
- `DevkitdClient` (`crates/flowspec-server/src/devkitd/client.rs`) — single cached `RunningService`
  connection behind a `tokio::sync::Mutex`, reconnecting on any transport error. `start`/`wait`/
  `cancel` implement the full decode chain from `plans/plan4.md` Step 2, including: dropping
  `null`-valued args before sending (devkitd turns `null` into `""` server-side, corrupting bool
  flags), truncating both `StepOutput` *and* `ToolError`'s stdout/stderr (the scheduler persists the
  latter verbatim via `Display`), and a deadline-aware backoff loop for transport errors
  (`poll_retry_delays`, default `[1,2,4,8,16,30]s`, ~61s budget) that never sleeps past the caller's
  deadline.
- 18 contract tests (`tests/devkitd_client.rs`) against an in-process stub `ServerHandler`, covering
  every branch of the decode table plus transport-blip/backoff-exhaustion via a real bind-drop-rebind
  on the same port.
- `Container`/`main.rs` wiring: `scheduler.recover()` runs before `mcp_server::serve` (boot
  recovery), `scheduler.shutdown()` runs after it exits (abort in-flight tasks, jobs keep running on
  devkitd, re-attached next boot). `check-layering.sh` gained a third assertion:
  `cargo tree -i rmcp -p flowspec-app` must fail.
- `tests/reattach_stub.rs` — the CI-safe headline property (job_id persisted before `wait`, so a
  `shutdown()`+drop mid-poll loses zero work) proven through the **real** `DevkitdClient` against a
  stub server, no live devkitd needed. This is what actually runs in CI; the live variant below is
  the same property proven end-to-end.
- Two fixtures: `flows-fixtures/tracer.yaml` (hooks + a `cli` step against devkitd's own `echo-test`
  plugin, no agent tokens) and `flows-fixtures/devkit-chain.yaml` (`create-feature` →
  `containers-up` → opencode → claude, real worktree + containers, deliberately no `clean-feature`).
  `tests/flows_fixtures_valid.rs` guards every top-level fixture against `FsFlowSource` as a
  permanent regression check.
- `docs/devkitd-dev.md` — manual devkitd setup, the one manifest edit below, and the operational
  constraints (retention, `max_parallel=5`, unmapped-arg drop, `null` handling).

### The two devkitd-repo edits (approved; devkitd otherwise untouched)

1. `plugins/agent-run.json` — added `"input": { "flag": "--prompt" }` alongside the existing
   `prompt` mapping. flowspec's scheduler always sends a cli step's prompt as `input`
   (`CliWith.input` is schema-required); the plugin only mapped `prompt`, so every real cli step
   would have silently produced an empty prompt.
2. `scripts/agent-run.sh` — the `-e PATH=...` passed to `docker run` was hardcoded to
   `${CONTAINER_HOME}/.local/bin:/usr/local/sbin:...`, clobbering the `coding-agent:latest` image's
   own `PATH` (which already includes `${CONTAINER_HOME}/.opencode/bin` — confirmed via
   `docker inspect`). `claude` is symlinked into `.local/bin` so it worked; `opencode` isn't, so
   every opencode step failed with `command not found` until this line added
   `${CONTAINER_HOME}/.opencode/bin` back into the override. Found by running `devkit-chain` live —
   exactly the kind of thing the tracer/chain split was for.

### Live-run numbers (this checkout, this session)

- **Tracer** (`echo-test` hook ×2 + `cli` step, real devkitd, `poll_interval=200ms`): **309ms**
  end-to-end, job_id observed persisted well before completion, both hooks `completed`.
- **devkit-chain** (`create-feature` + `containers-up` + opencode + claude, real worktree +
  containers): **150s** end-to-end after the PATH fix; `summary.md` 2191 bytes, `blog.md` 12909
  bytes. First attempt (before the PATH fix) failed at the `summary` step in 24s with a clean
  `ToolError{exit_code:1, stderr:"...opencode: command not found"}` — the adapter's error mapping
  surfaced the real failure exactly, which is how the PATH bug was found.
- **e2e semantics, all 6 run live against real devkitd** (not just the CI-safe stub variant):
  - `reattach_across_flowspec_restart` (slow-test, 6s): scheduler A shutdown mid-poll, scheduler B
    recovers over the same SQLite file + same live devkitd → `completed` in 6.25s total.
  - `devkitd_restart_yields_interrupted`: `kill -9` + restart devkitd mid-job (job registry is
    in-memory, so the restart genuinely forgets the job) → next poll's `isError` → `Interrupted` →
    run `failed`, `after_run` `when: always` hook still ran. 5.09s.
  - `transient_unreachable_within_backoff_budget`: `SIGSTOP`/`SIGCONT` on the live devkitd process
    (same process, temporarily unresponsive — distinct from a restart, which loses job state) → step
    completed normally, 6.12s.
  - `plugin_timeout_maps_to_timeout`: `disconnect-test` (30s) with `_timeout_seconds=5` → devkitd's
    own kill fires the `-2` sentinel → `Timeout`, 5.26s.
  - `deadline_backstop_cancels`: `disconnect-test` with no timeout override (devkitd's 600s default
    never fires) but a 3s client-side deadline → flowspec issues `job-cancel`, `Timeout`; a second,
    independent `wait()` call afterward observed devkitd's own `cancelled` state out-of-band,
    confirming the kill was real. 3.55s.
  - `cancel_run_kills_process_group`: `disconnect-test`'s forked child (marks a file at t=25s)
    never left a trace after `cancel_run` — the whole process group died, not just the parent.
    27.14s (dominated by the deliberate wait past the 25s mark).

### Deferred-from-Phase-3 items — all closed

- Output truncation, real polling, the full `DevkitdError` decode table, and `with:`
  stringification are all implemented in `DevkitdClient` per the divergence table above.
- The `kill -9` / re-attach property is proven twice: CI-safe (`tests/reattach_stub.rs`, real
  adapter + stub server) and live (`e2e_semantics.rs::reattach_across_flowspec_restart`, above).
- Subflow dispatch + chaos remain Phase 6, unchanged.

---

## Known follow-ups / debt

- **`Step.before` is dead code.** Parsed, has a `HookPhase`, handled in the scheduler, but nothing
  ever emits `Command::RunHooks { phase: BeforeStep }` — see Phase 5.6 below for the full trace.
  Flows declaring a per-step `before:` gate get a silent no-op today.
- **Hook subsystem now has a real-devkitd test** (`tracer.yaml`'s before/after hooks against
  `echo-test`) — the Phase 3 debt item is closed.
- Revisit the **two-batch activate atomicity** window if a real SQLite crash between the insert and
  the job_id write proves to matter in practice (recovery covers it today).
- `transient_unreachable_within_backoff_budget` uses `SIGSTOP`/`SIGCONT` rather than a real network
  partition; it proves "step survives a hung backend" but doesn't force the client's poll to
  actually observe a connection-level error and retry (a stalled request may just resume once the
  process is resumed). `devkitd_restart_yields_interrupted`'s real kill+restart does exercise the
  connection-refused → backoff → retry path along the way. Good enough for Phase 4; a firewall-rule
  or proxy-based blip would be a stronger version of the transient test if this ever needs revisiting.
- `docs/devkitd-dev.md`'s PATH fix (edit 2 above) is specific to `coding-agent:latest`'s current
  layout; if that image changes how `opencode`/`claude` are installed, re-verify with
  `docker inspect coding-agent:latest --format '{{range .Config.Env}}{{println .}}{{end}}'`.
