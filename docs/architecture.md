# flowspec-devkitd — Architecture

**What this is:** the FlowSpec runtime. It loads flow definitions (per `flowspec-spec.md` v0.3), executes them as durable state machines, and delegates all real work — agent steps and lifecycle hooks — to **devkitd**, a Rust daemon that is itself a native MCP server over Streamable HTTP (single service; the former devkitd-mcp intermediary is retired). flowspec consumes it as an MCP client. Inbound, flowspec exposes its own MCP tool surface so an AI host (OpenClaw) can trigger, supervise, and approve runs.

**devkitd execution contract (fixed by devkitd's design; this runtime adapts to it):** job-based, faithful to devkitd's original fire-and-callback protocol translated to MCP tools. a plugin tool call returns `{ job_id }` almost immediately (never blocks on execution; argument-validation failures are rejected synchronously in the same call, before any job exists); `job-status` / `job-cancel` are builtin tools. `job-status` on a terminal job carries the full result — job states are `received | running | done | cancelled`; failure lives in `exit_code`, not the state (sentinels `-1` spawn failure / `-2` timeout, safely outside the real 0–255 range). Unknown `job_id` is an `isError:true` tool result — and since it's the only error `job-status` can produce, the adapter discriminates `JobUnknown` by call-site, never by parsing message text. All payloads travel as JSON serialized inside `content[0].text` (MCP CallToolResult is content blocks, not structured fields). Cancellation kills the process *group* devkitd-side (SIGTERM → grace → SIGKILL). Timeouts are enforced server-side by devkitd (global default, per-plugin override). Every tool call is short-lived — no long-held HTTP connections anywhere on this edge. Jobs are not assumed to survive a devkitd restart: an unknown `job_id` maps to a step failure.

**Style:** hexagonal (ports & adapters) with a functional core / imperative shell split. Implementation-language agnostic; every named module maps to whatever unit the language uses (module, crate module, package).

**Living state:** for *actual* implementation status, plan divergences, applied patches, and per-phase handoffs, see [`docs/notes.md`](./notes.md) — the running progress & patch log.

---

## Core principles

1. **The domain decides, it never acts.** Given a flow definition, the current run state, and an event ("step X finished", "step Y approved"), pure functions return *commands* (`ActivateSteps`, `CancelSteps`, `StartChildRun`, `MarkRunTerminal`). No IO of any kind in this layer — no filesystem, network, clock, or randomness. Timestamps and IDs enter as arguments.
2. **The application layer executes commands** against ports, transactionally, and feeds resulting events back into the domain. It owns concurrency control.
3. **Adapters are the only code that knows protocols exist** — MCP inbound, MCP-client outbound to devkitd, SQLite, filesystem.
4. **Dependency rule:** `domain ← application ← adapters`. The composition root sees everything and wires it; nothing else crosses layers.
5. **One repo.** "devkitd-backed" is an outbound adapter, not a separate project. A different executor later is a new adapter, not a fork.

---

## Directory structure

```
flowspec-devkitd/
├── Cargo.toml                      # [workspace], shared deps in [workspace.dependencies]
├── crates/
│   ├── flowspec-domain/            # pure logic — no IO, no protocol/SDK types
│   │   └── src/
│   │       ├── flow/
│   │       │   ├── schema          # v0.3 definition shape + field-level validation
│   │       │   ├── types           # FlowDefinition, Step (tagged union), HookCall
│   │       │   ├── validate        # flow-load rule checklist → list of violations
│   │       │   └── dag             # graph queries over a definition
│   │       ├── run/
│   │       │   ├── types           # FlowRun, StepRun, statuses, Command (tagged union)
│   │       │   ├── engine          # decide(flow, state, event) → [Command]
│   │       │   └── derive          # phase + active_steps computed from StepRuns
│   │       └── template            # {{ }} parser + resolver
│   │
│   ├── flowspec-app/               # orchestration: domain decisions → port effects
│   │   └── src/
│   │       ├── ports               # all port interfaces (traits), one module
│   │       ├── scheduler           # command execution loop + per-run mutual exclusion
│   │       ├── subflows            # in-process child-run dispatch and linkage
│   │       ├── recovery            # startup scan, per-status interruption rules
│   │       ├── use_cases/
│   │       │   ├── start_flow
│   │       │   ├── advance_run
│   │       │   ├── approvals       # approve + reject (mirror images)
│   │       │   ├── cancel_run
│   │       │   └── queries         # all read-only operations
│   │       └── testkit             # FakeDevkitd (scripted), InMemoryStateStore
│   │
│   └── flowspec-server/            # binary: adapters + composition + main
│       └── src/
│           ├── mcp_server/
│           │   ├── tools           # tool definitions + input schemas → use cases
│           │   └── server          # MCP server setup, Streamable HTTP transport
│           ├── devkitd/
│           │   └── client          # MCP client to the devkitd daemon
│           ├── state/
│           │   ├── sqlite_store    # StateStore implementation, transaction wrapper
│           │   └── schema.sql      # tables: runs, step_runs, hook_runs, run_links
│           ├── flows/
│           │   └── local_dir_source # read + parse YAML from the flows directory
│           ├── config              # validated config: devkitd URL/auth, paths, limits
│           ├── container           # composition root: build adapters, wire use cases
│           └── main                # load config → recovery → start MCP server
│
├── flows-fixtures/                 # shared YAML fixtures: one per feature + invalid flows that must fail load
└── plans/                          # per-phase detailed plans
```

---

## Global decisions

### Workspace layout

```
flowspec-devkitd/
├── Cargo.toml                      # [workspace], shared deps in [workspace.dependencies]
├── crates/
│   ├── flowspec-domain/            # pure core
│   │   └── src/{flow/, run/, template.rs, lib.rs}
│   ├── flowspec-app/               # ports + orchestration
│   │   └── src/{ports.rs, scheduler.rs, subflows.rs, recovery.rs, use_cases/, lib.rs}
│   └── flowspec-server/            # binary: adapters + composition + main
│       └── src/{mcp_server/, devkitd/, state/, flows/, config.rs, container.rs, main.rs}
├── flows-fixtures/                 # shared YAML fixtures (used by domain, app, server tests)
└── plans/                          # per-phase detailed plans (this doc = phases 0–4)
```

### Dependency policy — the compile-time layering rule

| Crate | Allowed dependencies |
| --- | --- |
| `flowspec-domain` | `serde`, `thiserror`, `indexmap` (deterministic step ordering), `semver` (subflow version-constraint matching — pure). **No tokio, no rmcp, no IO crates.** |
| `flowspec-app` | `flowspec-domain`, `tokio` (full — the scheduler is a runtime shell: `tokio::spawn`, `sleep_until`, `Mutex`, `CancellationToken`), `async-trait`, `thiserror`, `tracing`, `tokio-util` (CancellationToken) |
| `flowspec-server` | everything: `rmcp`, `axum`, `rusqlite`, `figment`, `serde_yaml_ng`, `tracing-subscriber`, `reqwest` (transitively via rmcp) |

This table *is* the architecture's dependency rule; CI can enforce it with `cargo tree -i tokio -p flowspec-domain` expected to fail and `cargo tree -i rusqlite -p flowspec-app` expected to fail (the SQLite adapter lives in `flowspec-server`, never the orchestration layer).

### Cross-cutting conventions

- **Errors:** each crate defines its own `thiserror` enums. Domain errors are *values the engine returns*, never panics. `anyhow` only inside `main.rs`. Illegal state transitions are `EngineError::IllegalTransition { step, from, to }` — returned, logged, never silently applied.
- **Tracing:** one span per run (`run`, fields: `run_id`, `flow`), child span per step (`step`, fields: `step_id`, `attempt`). Every engine decision logged at `debug` with the input event and output commands — this is the primary debugging surface for a state machine. `tracing-subscriber` with `EnvFilter` (`RUST_LOG`) + optional JSON output for the homelab.
- **IDs and time:** generated in `flowspec-app` (a tiny `ids` module: `run_id = "run_" + ulid`, timestamps from `std::time::SystemTime`) and passed *into* the engine as arguments. The domain never reads a clock.
- **Async traits:** ports use `#[async_trait]` so they can be `Box<dyn StateStore>` etc. in the container. Native async-fn-in-traits isn't dyn-compatible; this is the pragmatic standard.

---

## `domain/` — the FlowSpec standard, as code

This layer implements the spec and nothing else. When `flowspec-spec.md` changes, the change lands here. Its purity is what makes the standard cheap to evolve: every semantic is testable as plain in/out assertions.

**`flow/`** — the *static* side, flow definitions:

- `schema` — the structural shape of a v0.3 flow: step tagged-union (`cli` | `subflow`), inputs/outputs/defaults, routing fields, `needs:`, lifecycle hooks. Field-level constraints live here (required fields, enum values). YAML *parsing* happens in the flows adapter; this validates the parsed structure.
- `types` — the domain vocabulary derived from the schema. Kept distinct so the rest of the codebase depends on types, not on the validation machinery.
- `validate` — flow-*load* validation: the spec's rule checklist (acyclicity, `needs:` consistency, no cross-sibling references, subflow recursion, unreachable steps, …) as functions returning a list of violations. One module; split a rule out only if it grows complex.
- `dag` — pure graph queries over a definition: routing edges out of a step, which steps gate on it via `needs:`, reachability. Its own module because it has two consumers: `validate` and `run/engine`.

**`run/`** — the *dynamic* side, run state:

- `types` — `FlowRun`, `StepRun`, status enums, and the `Command` tagged union. Commands are data; nothing here executes.
- `engine` — the heart of the system. The decide-function family: given (definition, current step runs, incoming event) → commands. All v0.3 execution semantics live here: which pending steps activate, whether a `needs:` barrier opened, fan-out (routing lists activating multiple distinct steps) and fan-in, the fail-fast cancellation set, approve/reject routing including reject self-loops (re-runs consume `reject_input` — default: original input + appended feedback), the per-step `retries` budget (exhausted on `failed` before `on_failure` routes; never on `failed_rejected`/`cancelled`), run-level deadline events (optional flow `timeout`; expiry → cancel active steps, terminal `failed` with `run_timeout`), marking never-reached steps `skipped` at terminal, subflow start/completion, terminal-phase detection. Legal status transitions are enforced here — an illegal transition is a returned error, never a silent write.
- `derive` — `phase` and `active_steps` computed from step runs. The spec's "derived, never stored" rule made into one obvious place.

**`template`** — resolution of `{{ inputs.x }}`, `{{ trigger.* }}`, `{{ env.* }}` (allow-listed), `{{ steps.<id>.output }}` / `.feedback` / `.approval_comment` / `.run.*`, and `{{ run.* }}`. Strict rule from the spec, no exceptions: an unavailable reference fails the step with a precise error, never substitutes an empty string. (There is no special `project.*` context — project/feature are ordinary flow inputs; devkitd resolves them to paths.) Part of the standard, not plumbing — but a single module: a minimal span parser plus a resolver against a context structure. No template library.

Example of why the command pattern pays: to test fail-fast, you construct a state with three parallel branch steps running, feed the event `StepFailed(branch_b)`, and assert the engine returns `CancelSteps([branch_a, branch_c])` + the correct routing command. No processes, no containers, no time.

---

## `application/` — making decisions happen

- `ports` — all port interfaces in one module. There are three:
  - `StateStore` — load/persist runs and step runs, `run_links`, all mutations inside a transaction boundary the store provides.
  - `Devkitd` — two operations: execute a step, run a hook. Both are tool calls on devkitd, so one port covers steps and lifecycle hooks. Steps and hooks share the same execution machine end-to-end (start → poll → result, persisted handle, re-attach); they differ only in what triggers them — routing vs. lifecycle phases + `when:` — and where results are recorded (`step_runs` vs `hook_runs`). The port is job-shaped without naming MCP: `start(req) -> JobHandle`, `wait(handle, deadline) -> StepOutput` (polling hidden inside), `cancel(handle)`. The handle is persistable — that single property is what turns recovery from "mark failed" into "re-attach and continue".
  - `FlowSource` — load all flow definitions.
  Three interfaces don't justify a directory. (`tracing` is used directly; there is no `Logger` port.)
- `scheduler` — the imperative shell around the engine: take commands, execute them transactionally (persist transitions, fire devkitd calls, dispatch child runs), translate completions/failures into events, feed them back to the engine until quiescent. Owns **per-run mutual exclusion**: a devkitd completion, an `advance_run`, and an `approve_step` can arrive concurrently for the same run; state transitions for one run must serialize. Cross-run concurrency stays unrestricted. Cancelling a step issues `cancel(job_id)` to devkitd, which terminates the real process group — cancellation is end-to-end, not bookkeeping.
- `subflows` — child-run dispatch: internally invokes start-flow, records parent↔child linkage, enforces the depth limit, converts child terminal phase into a parent-step completion event.
- `recovery` — startup scan of runs in `running` phase, applying per-status rules: `running` steps with a persisted job handle → **re-attach** (poll the job; still alive → the step continues as if nothing happened; terminal → consume its result; unknown → failed with `Interrupted`, then normal `on_failure` routing); `running` steps without a handle (crashed between activation and job start) → failed; `waiting_approval` untouched; `waiting_on_subflow` re-attached to the child's fate.
- `use_cases/` — one thin module per tool category: validate input → acquire the run's exclusion → engine/scheduler → respond. `approvals` holds approve and reject together; `queries` holds every read-only operation, since each is a handful of StateStore reads.

---

## `adapters/` — the outside world

- `mcp_server/` — the inbound edge. `tools`: tool names, input schemas, and descriptions mapped onto use cases — descriptions are UX copy for the AI host and deserve the same care as an API doc. `server`: MCP server over Streamable HTTP. No transport abstraction until a second transport is actually needed.
- `devkitd/client` — the outbound edge: an MCP *client* pointed at the devkitd daemon. Implements the `Devkitd` port over devkitd's job tools. Adapter rules, all consequences of devkitd's contract:
  - **Start, expose the handle, then poll.** `run` → `job_id`, surfaced immediately through the port so the scheduler can persist it *before* waiting; then a `job-status` poll loop (configurable interval) until terminal — the terminal status response *is* the result, no second fetch. Each poll doubles as the liveness signal ("last successful poll N seconds ago") — no progress notifications, no SSE, no long-held connections on this edge.
  - **Arguments are strings.** Templated values are stringified at this boundary; non-scalar values are JSON-encoded strings. The rule lives here and nowhere else.
  - **Double-decode, then the output convention.** Layer 1: `content[0].text` → the job envelope JSON (`job_id` or `state`/stdout/stderr/exit_code). Layer 2: the envelope's `stdout` string → try JSON parse: success → structured step output (dot-path templating works), failure → plain string. Uniform for steps and hooks. Yes, that's up to three nested JSON levels — documented so nobody "simplifies" it into a bug.
  - **Timeout hierarchy: devkitd first, deadline backstop second.** devkitd enforces execution timeouts server-side (it alone can kill the process group). The adapter additionally tracks the step's deadline across polls; deadline + margin exceeded with the job still running → `cancel(job_id)` → step failed with `Timeout`.
  - **Cancellation = `job-cancel`.** An explicit tool call, independent of any connection's lifetime — this is what makes the runtime's `cancel_run` real rather than cosmetic.
  - **Re-attach is just polling.** Given a persisted `job_id`, "resume waiting on a step" and "wait on a step" are the same code path. `JobUnknown` (devkitd restarted, job GC'd — surfaced as the poll's `isError`) maps to a step failure routed via `on_failure`.
  - Transport errors between polls are retried with capped backoff — polling an existing job is idempotent, so unlike the blocking model, transient network failures don't kill steps.
- `state/` — `sqlite_store` implementing `StateStore` over embedded SQLite; `schema.sql` as the human-readable source of truth (`runs`, `step_runs`, `hook_runs`, `run_links`). Schema applied idempotently at startup; no migration framework until the schema actually churns.
- `flows/local_dir_source` — read the flows directory, parse YAML (both `flow:` single and `flows:` array file shapes), hand parsed structures to `domain/flow/schema` for validation. Flows are keyed `(name, version)`; subflow `version` constraints are matched with the `semver` crate (`VersionReq::matches`): resolution picks the highest available version satisfying the constraint; no constraint = latest. The matching itself is pure and lives in the domain. An invalid flow is reported and skipped; it never prevents the runtime from starting.

---

## Root modules

- `config` — validated at startup, fail fast: devkitd URL and auth token, the type→tool executor binding (`cli_tool`, default `"agent-run"`), flows directory, database path, default step timeout + deadline margin, poll interval, max step output size, max subflow depth. (No global concurrency cap — a conscious decision; the seam is a one-line semaphore if the hardware ever objects.)
- `container` — manual dependency wiring, no DI framework: construct adapters from config, inject into use cases, hand the wired tool handlers to the MCP server.
- `main` — load config → build container → run recovery → serve.

---

## Test strategy (shapes the architecture, so it belongs here)

- `flows-fixtures/` — one flow per semantic feature: linear, human-loop, fan-out/fan-in, failure routing, subflow — plus invalid definitions (cycle, dangling `needs:`, cross-sibling reference) that must be rejected at load.
- fakes (`flowspec-app`'s `testkit`) — `FakeDevkitd` with scripted per-step results and controllable delays/failures; `InMemoryStateStore`. These two make the full runtime executable in milliseconds.
- domain tests — the bulk of all coverage: engine decisions, every validation rule (accept + reject pair), template resolution.
- integration tests — fixture flows end-to-end through real use cases and scheduler against the fakes; a separate suite against real SQLite for persistence round-trips and recovery (write state, rebuild, assert recovery commands).

---

## Artifacts convention (reference, not storage)

flowspec never sees files (worktrees live on devkitd's host), so it stores **references**, never content. Steps/hooks that produce durable artifacts emit them in their output JSON:

```json
{ "artifacts": [
    { "uri": "s3://flows/run_01ABC/plan/1/plan.md", "kind": "file" },
    { "uri": "git://repo@abc123", "kind": "commit" }
] }
```

Uploading is devkitd's side (an `artifact-upload` plugin/hook targeting MinIO or cloud S3, invoked by the flow via `on_success`/`after` hooks with paths + key prefix); committing is the agent's or a hook's job. flowspec stores the `artifacts[]` opaquely in the step output (bytes, not blobs) and surfaces it in `get_run_status`. The spec's `stream_log_ref` (full agent stream-json) is just another artifact under this convention — flows that upload it retain it; flows that don't, don't. Post-MVP seam: an `ArtifactStore` port for presigned URLs, enabled by the URIs already being structured.

---

## Explicitly out of scope (cut, with the seam preserved)

- devkitd internals (process supervision, plugin registry, group-kill, server-side timeouts) — devkitd's repo, not this one. flowspec sees only MCP tools.
- FlowSpec HTTP inbound interface — a future second inbound adapter over the same use cases.
- Streaming step *output* — progress notifications carry liveness, not content; stdout arrives only with the final result. Incremental output capture would require a devkitd contract change plus a new adapter, which the port permits.
- Git/hub flow sources, cross-runtime subflows, second MCP transport, migration tooling.
