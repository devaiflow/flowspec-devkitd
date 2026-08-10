# FlowSpec

## The Universal Standard for AI Agent Flows

**Spec version: 0.3 (draft)**

---

## What FlowSpec Is

FlowSpec is a standard for defining and executing AI agent workflows. It has three parts:

1. **A flow definition format** — how to describe what the AI should do, step by step, in a portable YAML file.
2. **An execution protocol** — the HTTP interface any runtime must implement to be FlowSpec-compatible.
3. **A logging schema** — the structured record of what happened during a run, for audit, debugging, and observability.

It is a *standard*, not a runtime. Multiple runtimes can implement it: a generic `flowspec-mcp` reference runtime, a devkitd-bound `flowspec-devkitd`, a Kubernetes-backed implementation, a serverless variant. A flow written against FlowSpec runs identically across any compliant runtime.

This document defines the standard. If you are implementing a runtime, this is what you must support. If you are writing flows, this is what you can rely on.

---

## Why FlowSpec Exists

The state of AI agent workflows today is fragmented in a specific and avoidable way: every tool invents its own concept of an "agent," its own way of chaining steps, its own log format (or no log format at all). The result:

- You cannot move a workflow from one vendor to another without rewriting it.
- You cannot audit what an AI did after a run, unless the vendor gave you that capability.
- You cannot define human checkpoints in a way another tool would understand.
- Community-contributed flows do not compose because there is nothing to compose them into.

This is the same problem that existed in CI/CD pipelines before GitHub Actions, GitLab CI, and CircleCI converged on similar YAML patterns. The answer is not "pick one vendor" — it is "have a standard so vendors can compete on implementation while flows remain portable."

FlowSpec is that standard for AI agent orchestration.

---

## Design Principles

Seven principles shape every decision in this spec. They come before the field definitions because they explain the *why* when field choices might otherwise look arbitrary.

**1. Portability over cleverness.** If a flow can be expressed either in a simple, broadly-supported way or a sophisticated way that needs specific runtime features, the simple way wins. FlowSpec is the lowest common denominator that still does real work.

**2. The developer is the author, not the product.** Flows are plain YAML files. They live in your repository or your knowledge hub. They are not rows in a vendor database. You can read them, edit them, diff them, version them, and take them somewhere else.

**3. Human loops are first-class.** Any step can require explicit approval before the flow continues. This is not a plugin or an advanced feature — it is baked into the core spec because responsible AI-assisted development requires it.

**4. Flows are directed acyclic graphs, not pipelines.** A rejected plan re-runs with feedback. A failing test routes back to implementation. Branches run in parallel and converge on a barrier. Real development has structure; FlowSpec reflects that.

**5. Execution produces a full audit trail.** Every step's executor, role, timing, token count, output, and approval state is captured in a structured log. You can reconstruct exactly what happened and why.

**6. Step types describe execution shape, not domain.** A step is either a CLI invocation or a sub-flow invocation. The set is closed in v0.3. The variation between *kinds of CLI* (which binary, which provider, which model) lives inside the step's `with` block and is validated by the runtime.

**7. Sub-flows are black boxes.** A flow that invokes another flow sees only the child's declared inputs and outputs. Internal step ids are not addressable from the parent. This is the same discipline functions impose on callers, and for the same reason.

---

## Part 1 — Flow Definition Format

### The Anatomy of a Flow

A flow is a named, versioned set of steps connected by routing edges. Here is a complete example showing most of the features at once:

```yaml
flow:
  name: feature-development
  version: 1.0.0
  description: "Full feature lifecycle: plan, review, implement, test"

  inputs:
    message:
      type: string
      required: true
      description: "Feature description from the user"

  outputs:
    plan_ref:    "{{ steps.plan.output }}"
    test_status: "{{ steps.test.output }}"

  steps:
    - id: plan
      type: cli
      with:
        cli: gemini-cli
        provider: vertex
        model: gemini-2.5-pro
        role: feature_planner
        input: "{{ inputs.message }}"
        output: PLAN.md
      human_loop: true
      on_approve: implement
      on_reject: plan              # re-run with feedback

    - id: implement
      type: cli
      with:
        cli: claude-code
        provider: anthropic
        model: claude-opus-4-5
        role: backend_engineer
        input: PLAN.md
        output: worktree
      human_loop: true
      on_approve: test
      on_reject: implement

    - id: test
      type: cli
      with:
        cli: claude-code
        provider: anthropic
        model: claude-sonnet-4-5
        role: test_runner
        input: "Run the test suite against the implementation"
        output: test-report.md
      on_success: done
      on_failure: implement
```

A developer proposes a feature, a planner AI plans it, the developer approves, an implementer AI builds it, the developer reviews the diff, tests run automatically, and if tests pass the flow completes. If tests fail, it loops back to implementation.

The rest of Part 1 breaks this down field by field.

---

### Top-Level Fields

Every flow has three required top-level fields and a handful of optional ones.

**`name`** (required) — Unique identifier for this flow within its context. Kebab-case. This is how the flow is referenced when triggered or when invoked as a sub-flow.

**`version`** (required) — Semantic version. Breaking changes bump major; additive changes bump minor; fixes bump patch. A runtime MAY hold multiple versions of the same flow; callers can request a specific one or use a constraint like `^1.0`.

**`description`** (optional but recommended) — Human-readable explanation of what the flow does. Shown in UIs and used by AI hosts to reason about which flow to pick when a user describes an intent in natural language.

**`inputs`** (optional) — Declared inputs the flow expects when triggered. If omitted, the flow takes no input parameters. Declared inputs are referenced inside step definitions as `{{ inputs.<name> }}`.

**`outputs`** (optional) — Declared outputs the flow exposes to its caller. Evaluated at flow completion. Only meaningful when the flow is invoked as a sub-flow; ignored when triggered as a top-level run. See "Sub-Flows" below.

**`defaults`** (optional) — Flow-level defaults for fields inside `with`, currently `cli`, `provider`, `model`. Step-level fields override these. Useful when most steps in a flow target the same execution stack.

**`timeout`** (optional) — Maximum wall-clock duration for the entire run (e.g. `"2h"`, `"45m"`). If exceeded, all active steps are cancelled and the run terminates as `failed` with `failure_reason: "run_timeout"`. If omitted, the run has no deadline — it can wait on approvals indefinitely.

**`lifecycle`** (optional) — Hooks that run before the first step, after the last step, or around individual steps. See "Lifecycle Hooks."

**`metadata`** (optional) — Arbitrary key-value pairs for tagging, ownership, authoring. Runtimes MAY use metadata for filtering or display but MUST NOT let metadata affect execution semantics.

**`flowspec_version`** (optional) — Spec version this flow targets, e.g. `"0.3"`. If omitted, runtimes assume the latest spec version they support.

**`steps`** (required) — The set of steps. Order in the YAML is irrelevant; the entry step is determined by routing (the unique step that no other step routes to).

---

### Steps: The Unit of Execution

Every step is a self-contained unit with a stable shape:

```typescript
interface Step {
  id: string;                  // Unique within the flow

  type: StepType;              // "cli" | "subflow"
  with: CliWith | SubflowWith; // Type-specific configuration

  // Optional control fields (apply to any type)
  human_loop?: boolean;        // Default: false
  timeout?: string;            // e.g., "10m", "1h"
  retries?: number;            // Default: 0. See "Retries" below.
  reject_input?: string;       // Template used as input on re-runs after
                               // rejection (on_reject self-loop). The original
                               // `with.input` applies only to the first
                               // activation and to retries of `failed`.
  before?: HookCall[];         // Step-scoped before hook
  after?: HookCall[];          // Step-scoped after hook

  // Routing (at least one required unless this is a terminal step)
  on_success?: string | string[];
  on_failure?: string | string[];
  on_approve?: string | string[];   // Only with human_loop: true
  on_reject?:  string | string[];   // Only with human_loop: true

  // Fan-in barrier
  needs?: string[];            // Step ids that must complete before this step starts
}

type StepType = "cli" | "subflow";
```

The shell of a step — `id`, `type`, control fields, routing — is universal. What changes between step types is `with`.

**`retries`** — Number of automatic re-executions when the step ends `failed` (never `failed_rejected`, never `cancelled`). Retries are exhausted *before* `on_failure` routing applies; each retry increments `attempt` and re-evaluates the original `input`. Default: 0.

Richer retry policies (retry only on specific error kinds, backoff) are deliberately deferred to a future spec version.

---

### Step Type: `cli`

A `cli` step invokes a CLI binary that the runtime knows how to spawn. This is the workhorse step type for LLM agents (`claude-code`, `gemini-cli`) but is not limited to them — a runtime MAY register any executable as a CLI.

```typescript
interface CliWith {
  cli: string;                  // Required. Runtime resolves to a binary.
  role?: string;                // Optional. Hub-resolved role identifier.
  input: string;                // Required. Template-resolved task input.
  output?: string;              // Optional. Reference name or path for the result.

  // CLI-specific fields. Validated by the runtime, not by the spec.
  // Common examples for LLM CLIs:
  provider?: string;            // Backend the CLI authenticates against
  model?: string;               // Model identifier passed to the CLI
  [key: string]: unknown;
}
```

**`cli`** — The CLI binary the runtime invokes. Examples: `claude-code`, `gemini-cli`, `aider`, `local-llama-cli`. The flow declares which tool runs the step; the runtime resolves the `cli` string to an actual binary and command shape. A runtime MUST document which CLIs it supports.

**`role`** — A role identifier from agent-context-hub. When this step starts, the runtime configures the CLI's environment so that `agent-context-hub` is registered as one of its MCP servers, and instructs the agent's first action to be `hub.bootstrap(role=<role>, task=<input>)`. The hub resolution happens *inside* the CLI's session — the runtime does not call the hub directly. Steps MAY omit `role` for CLIs that work from raw prompts only.

**`input`** — What the step receives as its task input. Supports template expressions (see "Templating").

**`output`** — How the step's result is identified. Can be:

- A file path (`PLAN.md`, `test-report.md`) — the runtime captures the file and makes it referenceable.
- A reference name that subsequent steps can use.
- A symbolic label like `worktree` for "wrote directly to the workspace" — the step modified files in place and has no single output artifact. This is documentation, not a template: the runtime does not resolve it to a path.

**CLI-specific fields.** Beyond the four universal fields above, `with` may contain any fields the chosen CLI requires. For LLM agent CLIs the common ones are `provider` and `model`:

- **`provider`** — The backend the CLI authenticates against. The same CLI can target multiple providers — Claude Code can talk to `anthropic`, `bedrock`, or `vertex`. Gemini CLI can target `vertex` or `ai-studio`. The provider determines which credentials, base URL, and quota the runtime applies. Credentials never appear in the flow file; the runtime maps `provider` strings to its own secret store.
- **`model`** — The model identifier passed to the CLI. Format is provider-specific (`claude-opus-4-5`, `gemini-2.5-pro`, `llama-3.1-70b-instruct`). The runtime MUST NOT silently substitute a different model — if the requested model is unavailable, the step fails with a clear error.

A non-LLM CLI registered by a runtime might require entirely different fields (e.g., a `shell` CLI taking `cmd` and `cwd`). The spec does not constrain this; the runtime validates.

**Resolution order for `with` fields.** Step-level → flow-level `defaults` → runtime default. A runtime MAY refuse to load a flow whose steps don't fully resolve all fields the chosen CLI requires.

```yaml
flow:
  defaults:
    cli: claude-code
    provider: anthropic
    model: claude-sonnet-4-5
  steps:
    - id: plan
      type: cli
      with:
        model: claude-opus-4-5    # overrides default; cli + provider inherited
        role: feature_planner
        input: "{{ inputs.message }}"
```

---

### Step Type: `subflow`

A `subflow` step invokes another FlowSpec flow as if it were a function call. The parent step blocks until the child run reaches a terminal state.

```typescript
interface SubflowWith {
  flow: string;                          // Required. Sub-flow name.
  version?: string;                      // Optional. Semver constraint, e.g. "^1.0".
  inputs?: Record<string, unknown>;      // Required if the sub-flow declares required inputs.
  output?: string;                       // Optional. Reference name for the child FlowRun.
}
```

```yaml
- id: deploy-staging
  type: subflow
  with:
    flow: deploy-to-environment
    version: "^1.0"
    inputs:
      environment: staging
      artifact: "{{ steps.build.output }}"
    output: staging-deployment
  on_success: notify
  on_failure: rollback
```

**`flow`** — Name of the sub-flow to invoke. The runtime resolves this against its flow registry.

**`version`** — Optional semver constraint. If omitted, the runtime selects the latest version it has.

**`inputs`** — Structured inputs passed to the sub-flow. Must satisfy the sub-flow's declared `inputs:` block (all `required: true` inputs present, types compatible).

**`output`** — Optional reference name. The parent's `steps.<id>.output` resolves to the sub-flow's declared `outputs:` block (see below).

**Black-box semantics.** The parent flow can only reference what the sub-flow exposes through its `outputs:` declaration. Internal step ids of the sub-flow are not addressable from the parent. This is enforced at templating time: `{{ steps.deploy-staging.output.deployment_id }}` is valid (named output); `{{ steps.deploy-staging.output.steps.deploy.output }}` is not.

**Recursion is forbidden.** A flow cannot invoke itself, directly or transitively. The runtime MUST detect cycles in the sub-flow call graph at flow-load time and refuse to load a flow that recurses. Iteration belongs inside a single CLI step; reuse belongs in non-recursive sub-flows.

**Audit linkage.** A `subflow` step records the child's `run_id` in its `child_run_id` field. The child run records `parent_run_id` and `parent_step_id`. See "Part 3 — Logging Schema."

---

### Flow Outputs

A flow MAY declare an `outputs:` block at the top level. The block defines the contract a sub-flow exposes to its callers.

```yaml
flow:
  name: deploy-to-environment

  inputs:
    environment: { type: string, required: true }
    artifact:    { type: string, required: true }

  outputs:
    deployment_id: "{{ steps.deploy.output.id }}"
    smoke_status:  "{{ steps.smoke-tests.output }}"
    deployed_url:  "{{ steps.deploy.output.url }}"

  steps:
    # ...
```

Each entry maps a stable public name to a template expression. Templates are resolved at the moment the flow completes. The result is a structured object that becomes `steps.<id>.output` in the parent.

**Why outputs are declared, not implicit.** Without an explicit contract, callers would couple to the sub-flow's internal step ids. Renaming an internal step would silently break every caller. Declared outputs are a refactor barrier: as long as the named outputs resolve to the same shape, the sub-flow can change its internals freely.

A flow without `outputs:` returns `null`. Such a flow is still useful as a sub-flow if it is invoked for its side effects (deploy, notify, clean up), but template expressions like `{{ steps.X.output.foo }}` against it MUST fail at flow-load time.

---

### Routing: The `on_*` Fields

A step is not complete until the runtime knows where to go next. Routing is explicit through four fields:

- `on_success` — where to go if the step completed successfully.
- `on_failure` — where to go if the step completed but reported failure.
- `on_approve` — where to go if `human_loop: true` and the human approved.
- `on_reject` — where to go if `human_loop: true` and the human rejected.

The special destination `"done"` (string literal) means the flow terminates at this step.

**Fan-out.** Each routing field accepts either a single step id or a list of step ids. A list means "activate all of these concurrently."

```yaml
- id: build
  type: cli
  with: { cli: claude-code, provider: anthropic, model: claude-sonnet-4-5, role: builder, input: "Build" }
  on_success: [unit-tests, integration-tests, lint]
  on_failure: report-failure
```

When `build` succeeds, all three named steps activate at the same time. A single string remains valid as syntactic sugar for a one-element list.

**Fan-in.** A step that depends on multiple predecessors declares them via `needs:`.

```yaml
- id: gate
  type: cli
  needs: [unit-tests, integration-tests, lint]
  with:
    cli: claude-code
    provider: anthropic
    model: claude-sonnet-4-5
    role: reviewer
    input: |
      Unit:  {{ steps.unit-tests.output }}
      Integ: {{ steps.integration-tests.output }}
      Lint:  {{ steps.lint.output }}
  on_success: deploy
```

`gate` does not start until *all* three predecessors have completed successfully. If any predecessor fails, the run takes the failure routing of that predecessor — `gate` is not activated.

**Routing rules:**

- A step without `human_loop` MUST have `on_success`; `on_failure` is strongly recommended.
- A step with `human_loop: true` MUST have both `on_approve` and `on_reject`.
- A step without `human_loop` MUST NOT have `on_approve` or `on_reject`.
- A step with `human_loop: true` MAY also have `on_failure`: if the step fails before reaching the approval gate, `on_failure` applies; if it succeeds and reaches approval, `on_approve`/`on_reject` apply.
- If a step is the target of routing from more than one source, it MUST declare `needs:` listing those sources. Implicit joins are not allowed.
- The set of routing edges across all steps MUST form a directed acyclic graph. Cycles are detected at flow-load time. (Self-loops via `on_reject: <same step>` are the one allowed exception, since rejection re-runs the same step with feedback.)

---

### Concurrency Semantics

When a routing list activates multiple steps, all of them run concurrently. The runtime maintains a *set* of currently-active steps, not a single "current step."

**Failure of a fan-out branch.** In v0.3, the only behavior is **fail-fast**: the first branch to fail causes the runtime to cancel all sibling branches. The run then routes through the failed step's `on_failure` (or terminates if none is declared).

This is the simple, predictable default. More nuanced policies (`wait-all`, `continue-on-failure`) are deferred to a later spec version. If you need them now, model them by routing each branch's `on_failure` to a sentinel step that aggregates results.

**Resource contention.** Concurrent steps may share the same underlying workspace if the executor gives them one. If two branches both write to that workspace, they will collide. Either model branches as separate sub-flows with their own provisioned environments, or have the runtime expose isolated workspaces per branch through hooks.

**Templating across branches.** A step may only reference `steps.<id>.output` for steps that are guaranteed to have completed before it activates. Specifically:

- A predecessor reachable transitively through routing edges that *must* fire before this step activates — typically through a `needs:` chain.
- A step listed in this step's `needs:` array.

References to siblings (steps in parallel branches of the same fan-out) MUST fail at flow-load time. The runtime cannot guarantee ordering between siblings, so any cross-sibling reference is a race.

---

### Human Loops

Human loops keep developers in control of AI-assisted workflows. A step with `human_loop: true` does not advance automatically when it completes — it produces its output and then pauses, waiting for a decision.

What the runtime does when a human-loop step finishes its work:

1. Captures the step output.
2. Transitions the step to `waiting_approval`.
3. Emits a `step_waiting_approval` event with the step's output or a summary.
4. Holds the step indefinitely until approval or rejection arrives.

**How approval arrives.** The runtime MUST accept at least:

- `POST /runs/{run_id}/approve` with optional `step_id` and `comment`.
- `POST /runs/{run_id}/reject` with optional `step_id` and `feedback`.

The `step_id` field is required when more than one step in the run is currently `waiting_approval` — possible under parallelism.

**On approve.** The step transitions to `completed`, the run resumes from `on_approve`, and the comment becomes available as `{{ steps.<id>.approval_comment }}`.

**On reject.** The step transitions to `failed_rejected`, the run resumes from `on_reject` (often the same step, looping back), and the feedback becomes available as `{{ steps.<id>.feedback }}`.

**Re-run input.** When `on_reject` targets the same step (self-loop), the re-run does not re-evaluate the original `with.input`. Instead it evaluates the step's `reject_input` template, whose context is guaranteed: by the time it runs, `{{ steps.<id>.feedback }}` exists. If `reject_input` is not declared, the default is the original resolved input with the feedback appended:

    <original input>

    Feedback: <feedback>

This keeps the strict templating rule exception-free: `input` never references feedback (attempt 1 would fail), `reject_input` always can. Whether the re-run continues the previous agent session or starts fresh is a runtime/CLI concern, expressed through CLI-specific `with` fields (e.g. `resume: true`) — the spec only defines what the re-run receives as input. When `on_reject` routes to a *different* step, that step's own `input` may reference `{{ steps.<id>.feedback }}` legally (the rejection already happened before it activates); `reject_input` is meaningless there and MUST be rejected at flow-load time.

A step can remain in `waiting_approval` indefinitely. The runtime persists this state — a human can approve a plan created three days earlier without the run being lost to a restart.

**Human loops in sub-flows do not propagate.** A `human_loop: true` step inside a sub-flow blocks the child run, not the parent. The parent step transitions to `waiting_on_subflow` and the approver interacts with the child run directly. This is the black-box property: the parent treats the child as opaque, including its approval surface. UIs are expected to deep-link from the parent's `child_run_id` to the child run.

---

### Templating

Template expressions use `{{ ... }}` syntax and are evaluated by the runtime before a step executes (or, for `outputs:`, when the flow completes). Available contexts:

| Context                         | Scope                                       | Example                                  |
|---------------------------------|---------------------------------------------|------------------------------------------|
| `inputs.*`                      | Flow-declared inputs                        | `{{ inputs.message }}`                   |
| `trigger.*`                     | Raw trigger payload fields                  | `{{ trigger.user_id }}`                  |
| `steps.<id>.output`             | Output of a `cli` step or a sub-flow's outputs object | `{{ steps.plan.output }}`     |
| `steps.<id>.run.*`              | Audit metadata of a `subflow` step's child run | `{{ steps.deploy.run.run_id }}`        |
| `steps.<id>.feedback`           | Rejection feedback                          | `{{ steps.plan.feedback }}`              |
| `steps.<id>.approval_comment`   | Approval comment                            | `{{ steps.plan.approval_comment }}`      |
| `run.*`                         | Self-referential run metadata               | `{{ run.id }}`, `{{ run.failed_step_id }}` |
| `env.*`                         | Allow-listed runtime environment variables  | `{{ env.PROJECT_NAME }}`                 |

**Two namespaces for sub-flow steps.** A `subflow` step exposes both `output` (the contract — fields declared in the child's `outputs:` block) and `run` (audit metadata — `run_id`, `duration_seconds`, `status`, etc.). Use `output` for everything the parent flow logic depends on; use `run` only for diagnostic or logging purposes.

**Evaluation rules:**

- Templates are resolved at step-start time, not at flow-definition time.
- Runtimes MUST evaluate templates in a safe context — no arbitrary code execution.
- Referencing a context that is not available MUST fail the step with a clear error, never substitute empty strings.
- Referencing a `steps.<id>.output` field that the sub-flow does not declare in its `outputs:` block MUST fail at flow-load time when statically determinable, otherwise at step-start time.

---

### Project Context (removed as a spec concept)

Earlier drafts defined a mandatory `project.*` template context populated by the runtime. v0.3 removes it: which project/feature a flow operates on is ordinary flow `inputs`, supplied by the caller like any other input and passed to executors and hooks through normal templating. Filesystem resolution (worktrees, repo paths, branches) is an executor concern — the orchestrating runtime never needs to know a path.

Runtimes MAY expose additional runtime-specific template contexts, but flows relying on them are not portable and runtimes MUST document them.

---

### Lifecycle Hooks

Real flows almost always need work that surrounds the step graph but is not itself a step: provisioning a worktree, starting service containers, running migrations, tearing things down at the end. Putting these as additional steps is wrong — they are not LLM-driven, they do not have routing, and they need to run on failure paths too. FlowSpec models them as **named hooks** the runtime resolves to actual operations.

A hook reference is a name plus arguments. The runtime ships a registry of hook implementations; the flow file only references them by name. This keeps flows portable across runtimes.

```yaml
flow:
  lifecycle:
    before_run:
      - hook: create-feature
        args: { project: "{{ inputs.project_name }}", feature: "{{ inputs.feature_name }}" }
      - hook: containers-start
        args: { project: "{{ inputs.project_name }}", feature: "{{ inputs.feature_name }}" }

    after_run:
      - hook: containers-down
        args: { project: "{{ inputs.project_name }}", feature: "{{ inputs.feature_name }}" }
        when: always
      - hook: clean-feature
        args: { project: "{{ inputs.project_name }}", feature: "{{ inputs.feature_name }}" }
        when: always
```

#### Hook Phases

- **`before_run`** — Runs once after the FlowRun is created but before the first step starts. If a hook fails (and is not `always_continue: true`), the run fails before any step executes.
- **`after_run`** — Runs once after the run terminates. Conditional on `when` (see below).
- **`before`** — Per-step, declared on a step's `before` field. Runs after the step's input is resolved but before the executor is invoked.
- **`after`** — Per-step, declared on a step's `after` field. Runs after the executor returns, before routing decisions.

#### HookCall Shape

```typescript
interface HookCall {
  hook: string;                        // Name the runtime resolves to an implementation
  args?: Record<string, unknown>;      // Template-resolved arguments
  timeout?: string;                    // Default: runtime-configurable
  when?: HookCondition;                // When this hook fires (after_run / step-after only)
  always_continue?: boolean;           // before_run only: don't fail the run if this hook fails
}

type HookCondition = "succeeded" | "failed" | "cancelled" | "always";
```

The `when` field replaces the older `always: true` boolean and gives finer control over conditional execution. Default is `succeeded` (the previous default behavior).

#### Hook Registry: A Runtime Concern

FlowSpec does not standardize *which* hooks exist — only the shape of how they are invoked. A runtime MUST document its registered hooks. Common hooks the DevKit reference runtime ships: `create-feature`, `containers-start`, `containers-down`, `clean-feature`.

Hooks not present in the runtime's registry MUST cause the flow to fail at load time, not at execution time.

#### Why Named Hooks, Not Inline Shell

A previous draft considered allowing inline shell. It was rejected:

1. **Portability.** Inline shell makes the flow a bash script in YAML clothing. Flows with `run: "./create-feature.sh"` only work on systems with that script in that path.
2. **Auditability.** Hook execution is captured in the FlowRun log with well-defined inputs and exit conditions; arbitrary shell is not.

Runtimes MAY offer a `shell` hook as a generic escape hatch (`hook: shell, args: { cmd: "make test" }`), but flow authors should prefer named hooks for anything reused across multiple flows.

---

### Multiple Flows in One File

A single YAML file can declare multiple flows in a `flows:` array:

```yaml
flows:
  - flow:
      name: feature-development
      version: 1.0.0
      # ...
  - flow:
      name: bug-fix
      version: 1.0.0
      # ...
```

Convenient when a project has many small flows that share context. Runtimes MUST support both `flow:` (single) and `flows:` (array) top-level shapes.

---

### Flow-Load Validation

Runtimes MUST perform the following checks at flow-load time and refuse to load any flow that fails them:

1. **Step ids unique** within the flow.
2. **Routing targets exist** — every `on_*` and `needs:` value either references a defined step id or is the literal `"done"`.
3. **DAG is acyclic** — except for the `on_reject: <self>` self-loop allowance.
4. **Multi-target requires `needs:`** — if step X is the target of routing from more than one source, X must declare `needs:` listing those sources.
5. **`needs:` matches sources** — every step in `X.needs` must route to X via some `on_*` field.
6. **Type/with consistency** — `with` schema matches `type`. For `cli`, the universal fields (`cli`, `input`) are present. For `subflow`, `flow` is present.
7. **Sub-flow inputs satisfied** — if the runtime can resolve the sub-flow at load time, every required input is provided in `with.inputs`.
8. **No cross-flow recursion** — the sub-flow call graph is acyclic.
9. **Output references valid** — `{{ steps.X.output.foo }}` against a `subflow` step is checked against the sub-flow's declared `outputs:` block when statically resolvable.
10. **No cross-sibling references** — a step cannot template `{{ steps.Y.output }}` when Y is a parallel sibling.

These checks shift error reporting left. A flow that loads is structurally sound; failures at runtime are about external conditions (CLI errors, hook failures), not about the flow's wiring.

---

## Part 2 — Execution Protocol

### Conformance and Protocol Bindings

A runtime is FlowSpec-conformant if it implements the flow semantics (Part 1) and the logging schema (Part 3), plus at least one **protocol binding** exposing these operations: trigger, list flows, run status, approve, reject, cancel. The HTTP+SSE interface below is the reference binding. An MCP binding (tools over Streamable HTTP) is equally conformant; operation names MAY differ per binding, semantics MUST NOT.

### The HTTP Interface

```
GET    /flows                         # List all available flows
GET    /flows/{name}                  # Describe a specific flow
POST   /flows/{name}/trigger          # Start a new run
GET    /runs                          # List runs (filterable)
GET    /runs/{run_id}                 # Full run state and step logs
POST   /runs/{run_id}/approve         # Approve a waiting human_loop step
POST   /runs/{run_id}/reject          # Reject a waiting human_loop step
GET    /runs/{run_id}/stream          # Server-Sent Events stream of live events
POST   /runs/{run_id}/cancel          # Cancel a run (terminal)
```

The contract is minimal intentionally. Runtimes MAY add endpoints for their own features; clients targeting portability use only these.

---

### Triggering a Flow

```
POST /flows/feature-development/trigger
Content-Type: application/json

{
  "inputs": {
    "message": "add stripe payment integration"
  },
  "trigger_metadata": {
    "user": "matias",
    "source": "signal"
  }
}
```

Response:

```json
{
  "run_id": "run_20260507_143022_a3f9",
  "phase": "running",
  "active_steps": ["plan"],
  "started_at": "2026-05-07T14:30:22Z"
}
```

The run proceeds asynchronously. The caller polls `/runs/{run_id}` or subscribes to `/runs/{run_id}/stream` for live updates.

---

### Approving and Rejecting

```
POST /runs/run_20260507_143022_a3f9/approve
Content-Type: application/json

{
  "step_id": "plan",
  "comment": "Looks good. Use idempotency keys on the refund endpoint."
}
```

Approval with a comment means: advance through `on_approve`, and pass the comment as `{{ steps.plan.approval_comment }}` to subsequent steps.

```
POST /runs/run_20260507_143022_a3f9/reject
Content-Type: application/json

{
  "step_id": "plan",
  "feedback": "Missing error handling for expired customer tokens. Add a step."
}
```

Rejection with feedback advances through `on_reject` (typically re-runs the same step) with the feedback available as `{{ steps.plan.feedback }}`.

`step_id` is required when more than one step is currently `waiting_approval`. Optional when only one is waiting.

---

### Event Stream

The SSE stream emits events as the run progresses.

```typescript
interface RunEvent {
  type: EventType;
  run_id: string;
  step_id?: string;
  timestamp: string;    // ISO 8601
  sequence: number;     // Monotonic per run
  payload: unknown;     // Type-specific
}

type EventType =
  | "run_started"
  | "step_activated"        // Step transitioned from pending to active
  | "step_started"          // CLI/subflow execution began
  | "step_stream_delta"     // Incremental output from the executor
  | "step_completed"
  | "step_failed"
  | "step_waiting_approval"
  | "step_waiting_on_subflow"  // Subflow step's child is blocked (matches the step status)
  | "approval_resolved"
  | "run_completed"
  | "run_failed"
  | "run_cancelled";
```

The `sequence` field gives total ordering for replay. With parallelism, events from different steps interleave; clients demultiplex on `step_id`. A client reconnecting after a network hiccup can request replay starting from `sequence: N+1`.

---

## Part 3 — Logging Schema

### The FlowRun Record

```typescript
interface FlowRun {
  run_id: string;
  flow: string;
  flow_version: string;
  flowspec_version: string;
  triggered_by: string;            // e.g., "user:matias", "schedule:daily-tests"
  trigger_input: Record<string, unknown>;
  trigger_metadata?: Record<string, unknown>;

  // Sub-flow linkage (set when this run was invoked from a parent step)
  parent_run_id?: string;
  parent_step_id?: string;
  child_run_ids?: string[];        // Denormalized; runtime computes

  // State (see "Run State" below)
  phase: RunPhase;
  active_steps: string[];          // Step ids with non-terminal, non-pending status

  started_at: string;              // ISO 8601
  completed_at?: string;
  duration_seconds?: number;

  steps: StepRun[];
  hook_runs?: HookRun[];

  // Aggregates
  total_tokens: TokenCounts;
  total_cost_usd?: number;

  // Computed outputs (the resolved `outputs:` block; only meaningful for sub-flows)
  outputs?: Record<string, unknown>;
}

type RunPhase =
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

interface TokenCounts {
  input: number;
  output: number;
  total: number;
}
```

---

### Run State: Phase and Active Steps

Run state is two orthogonal pieces of information:

**`phase`** answers: *is this run still alive, and if not, how did it end?* It takes exactly one value at a time. A run is either `running`, or it has terminated as `completed`, `failed`, or `cancelled`.

**`active_steps`** answers: *what is happening right now?* It is a list of step ids whose status is non-terminal and non-pending — that is, currently `running`, `waiting_approval`, or `waiting_on_subflow`. While `phase == "running"`, `active_steps` is non-empty. When the run terminates, `active_steps` empties.

Both fields are *derived* from step-level statuses plus a cancellation flag. The runtime computes them; consumers do not need to maintain them. This guarantees the run-level view can never disagree with the per-step truth.

A consumer asking "what does this run need from me?" iterates `active_steps`, looks up each id in `steps[]`, and inspects each step's `status`. The information lives at one layer (the step) and is summarized at another (the run) without redundancy.

**Examples.**

A run mid-flight with three concurrent steps in different states:

```json
{
  "phase": "running",
  "active_steps": ["unit-tests", "integration-tests", "deploy-staging"]
}
```

A finished run:

```json
{
  "phase": "completed",
  "active_steps": []
}
```

A failed run:

```json
{
  "phase": "failed",
  "active_steps": []
}
```

---

### The StepRun Record

```typescript
interface StepRun {
  step_id: string;
  status: StepStatus;
  type: StepType;                  // "cli" | "subflow"

  // Resolved configuration (what was actually used at runtime)
  with_resolved: Record<string, unknown>;

  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;

  // Tokens and cost (CLI steps only; tracked when the runtime supports it)
  tokens?: TokenCounts;
  cost_usd?: number;

  // I/O
  input_resolved?: string;         // CLI steps: the resolved input
  artifacts?: {                    // Durable outputs, by reference (never content)
    uri: string;
    kind: "file" | "commit";
  }[];
  output_ref?: string;             // Optional URI pointer, typically into artifacts[]
  stream_log_ref?: string;         // CLI steps: stream-json log; optional URI pointer,
                                   // typically into artifacts[]

  // Sub-flow linkage (subflow steps only)
  child_run_id?: string;

  // Human loop
  human_loop: boolean;
  approval_status?: "pending" | "approved" | "rejected";
  approved_by?: string;
  approved_at?: string;
  approval_comment?: string;
  feedback?: string;

  // Hub integration (CLI steps with role)
  hub_bundle_ref?: string;

  // Failure info
  failure_reason?: string;
  failure_details?: Record<string, unknown>;

  // Retry info
  attempt: number;
}

type StepStatus =
  | "pending"
  | "running"
  | "waiting_approval"
  | "waiting_on_subflow"
  | "completed"
  | "failed"
  | "failed_rejected"           // Human rejected; routes via on_reject
  | "skipped"                   // Routing did not reach this step
  | "cancelled";                // Killed mid-execution
```

Notes:

- `failed_rejected` is distinct from `failed` so audit consumers can tell apart "the AI couldn't do it" from "the human said no."
- `waiting_on_subflow` is a step status, not a run status. A run with one step in `waiting_on_subflow` and another in `running` has `phase: running` and both step ids in `active_steps`.
- `with_resolved` captures the post-template `with` block. For `cli` steps this includes the resolved CLI, provider, model, role, and input. For `subflow` steps it includes the resolved sub-flow name, version, and inputs.

---

### The HookRun Record

```typescript
interface HookRun {
  hook: string;
  phase: HookPhase;
  step_id?: string;                // Present for step-scoped hooks

  status: HookStatus;
  started_at: string;
  completed_at?: string;
  duration_seconds?: number;

  args_resolved?: Record<string, unknown>;
  output_ref?: string;

  failure_reason?: string;
  failure_details?: Record<string, unknown>;
}

type HookPhase = "before_run" | "after_run" | "before_step" | "after_step";

type HookStatus = "completed" | "failed" | "skipped";
```

Hook executions are recorded so the audit trail covers the full lifecycle, not just the executor steps. Orphan slots, half-started containers, and missed teardowns are all visible here — these often point to a flow's failure cause faster than the step log does.

---

### Stream Logs

The structured StepRun captures aggregates (duration, tokens, output reference). For debugging and replay you often need the **raw event stream** — every tool call, every intermediate thought, every delta.

Runtimes SHOULD persist the full stream-json output of each `cli` step alongside the structured log. The StepRun's `stream_log_ref` field points to it. Format is newline-delimited JSON, one event per line, matching the streaming format of the underlying CLI.

With the structured log plus the stream log, you can replay an agent's reasoning step by step, find which tool calls consumed the most tokens, diagnose why a plan went off-track, and build agent-performance analytics.

Stream logs are typically the largest data a FlowSpec runtime produces. Runtimes MAY compress them, rotate them, or move them to cold storage, but MUST keep `stream_log_ref` resolvable for the lifetime of the run record.

---

### Hub Bundle Reference

The `hub_bundle_ref` field on each StepRun is worth calling out. When a CLI step uses a role and the agent bootstraps from agent-context-hub, the bundle the hub returned is captured and referenced.

This matters because **the same role can return different bundles over time** — hub content changes, tags evolve, role notes accumulate. If you are debugging a flow that worked last month but does not now, comparing the bundles from then versus now often reveals what changed. Without this reference, you are guessing at what the agent actually knew when it acted.

---

### Sub-Flow Linkage

When a `subflow` step invokes a child run:

- The parent's `StepRun.child_run_id` is set to the child's `run_id`.
- The child's `FlowRun.parent_run_id` is set to the parent's `run_id`.
- The child's `FlowRun.parent_step_id` is set to the parent's step id.

Following these links reconstructs the full call tree. The `GET /runs/{run_id}` endpoint shape does not change; clients that want the full tree do N+1 fetches or use a runtime-specific `?include=children` query parameter.

The call tree is always finite and acyclic — recursion is forbidden at flow-load time, so there is no infinite-depth scenario. Runtimes MAY additionally enforce a maximum sub-flow depth as defense-in-depth for call graphs not fully resolvable at load time.

---

## Part 4 — What Is Out of Scope

FlowSpec is deliberately narrow. These are explicitly not part of the spec:

- **Agent configuration.** How a runtime resolves `claude-code` to a binary path, an API key, and parameters is runtime-specific.
- **Project provisioning.** Creating projects, managing git state, setting up environments — runtime-specific. FlowSpec assumes the project exists and is accessible.
- **Secret management.** Secrets never appear in flow YAML. Runtimes handle resolution.
- **Authentication and authorization.** The HTTP interface assumes some auth mechanism exists; the spec does not dictate which.
- **Scheduling.** "Run this flow every weekday at 9am" is a runtime feature.
- **Dynamic fan-out.** "Run this step once per item in a list" is not in v0.3. Model it as multiple explicit steps, or as a sub-flow invoked multiple times.
- **Custom step types.** The `type` field has a closed set of values in v0.3 (`cli`, `subflow`). Runtimes MUST NOT accept custom types in this version.
- **Loops and conditionals.** Beyond `on_success`/`on_failure` branching and `on_reject` self-loops, FlowSpec has no native loop or `if` primitive. Branching can be modeled as a CLI step that returns success or failure based on internal logic.

This narrowness is the price of portability. A flow you can run on any FlowSpec runtime is more valuable than a flow you can run on one runtime that supports every possible pattern.

---

## Part 5 — Versioning the Spec

FlowSpec itself is versioned, distinct from individual flows.

Current spec version: **0.3 (draft)**.

A flow MAY declare `flowspec_version: "0.3"` at the top level. If omitted, runtimes assume the latest spec version they support. Runtimes SHOULD document which spec versions they implement.

---

## Amendments Applied (2026-07)

1. **Protocol bindings** — Part 2 opens with a conformance definition: HTTP+SSE is the *reference* binding; an MCP binding is equally conformant. (The HTTP binding materializes when the Platform — its natural client — exists; the current MVP runtime exposes MCP only.)
2. **`artifacts[]` on StepRun** — new optional field `artifacts?: { uri, kind: "file" | "commit" }[]`; `output_ref` and `stream_log_ref` redefined as optional URI pointers that typically reference `artifacts[]` entries.
3. **Event naming** — Human Loops prose now says `step_waiting_approval` (the `EventType` name); `step_waiting_subflow` renamed to `step_waiting_on_subflow` to match the `waiting_on_subflow` step status.
4. **Sub-flow depth** — runtimes MAY enforce a maximum sub-flow depth as defense-in-depth.
