# Running devkitd for Phase 4 development

devkitd (`~/work/projects/devaiflow/devkitd`, branch `feat-refactor`) is **not
modified by us** except for one line documented below. This is the manual
setup for running it locally against flowspec's real `DevkitdClient`.

## Starting the server

```bash
cd ~/work/projects/devaiflow/devkitd
cargo run
```

- Binds `127.0.0.1:9000`; MCP endpoint is `http://127.0.0.1:9000/mcp`
  (**the full path, including `/mcp`** — that's what `devkitd_url` must be).
- **cwd matters.** `plugins_dir`/`scripts_dir` in `devkitd.toml` are relative
  to the working directory devkitd is started from. Always `cd` into the
  devkitd repo root first.
- Loopback bind needs no auth token. Set `DEVKITD_MCP_TOKEN=<token>` to
  require bearer auth (mandatory for any non-loopback bind — devkitd refuses
  to start without one in that case). `devkitd_auth_token` in flowspec's
  config is the raw token, no `"Bearer "` prefix.
- Plugins load at boot and on every `tools/list` (full directory rescan).
  **Restart devkitd after editing a plugin manifest.**

## The one required manifest edit

flowspec's scheduler always sends a cli step's prompt as an arg named
`input` (`CliWith.input`, `flowspec-app/src/scheduler.rs`), but devkitd's
`agent-run` plugin only mapped `prompt`. One line bridges them:

```json
// devkitd/plugins/agent-run.json — args_mapping
"input": { "flag": "--prompt", "required": false },
```

(Already applied in this checkout, alongside the existing `prompt` mapping —
either arg name reaches `--prompt`.)

## Operational constraints worth knowing

- **`_timeout_seconds` is authoritative, with no server-side cap.** The
  step's `timeout:` (parsed + `deadline_margin_secs`) is what flowspec sends;
  devkitd's own `[execution] timeout_seconds` (600s) only applies when the
  caller omits an override.
- **Terminal job retention is 24h**, purged opportunistically on the next job
  insert (no background sweeper; running jobs are never purged). Re-attach
  is effectively infallible short of a devkitd restart.
- **`max_parallel = 5`** (a global `tokio::Semaphore`). A 6th concurrent job
  still gets a `job_id` and reports `running` immediately — that state does
  not guarantee a live process, only that the job was accepted.
- **Unmapped args are silently dropped.** A typo'd `with:` key (or an
  optional key the plugin manifest doesn't map) is a silent no-op, not an
  error — check the manifest, not just the fixture, when a step "ignores" an
  argument.
- **`null`-valued args become `""` server-side**, which corrupts bool-flag
  parsing in the shell scripts (e.g. `verbose`). The flowspec adapter omits
  `null`-valued keys before sending rather than passing them through.
- The tool named `containers-up` is what starts feature containers; its
  *executable* is `containers-start.sh` — the tool name and script name
  differ (a footgun when writing fixtures from memory).

## Available plugins used by the Phase 4 fixtures

| tool | used by | notes |
| --- | --- | --- |
| `echo-test` | `tracer.yaml` (hook + cli step) | required arg `id`; ignores unrecognized argv, so no arg-mapping edits were needed |
| `create-feature` | `devkit-chain.yaml` (before_run) | `-p`/`-f` for project/feature; defaults to embedded infra |
| `containers-up` | `devkit-chain.yaml` (before_run) | reads `COMPOSE_FILE`/`COMPOSE_PROFILES`/`ROUTING` from the worktree `.env` |
| `agent-run` | `devkit-chain.yaml` (steps) | `input`/`prompt` → `--prompt`; stdout is the run directory path, not the response text — the actual output lives in `<run_dir>/response.md`, `meta.json`, `stream.jsonl` |

## Running the fixtures

```bash
just tracer   # flows-fixtures/tracer.yaml — cheap, no agent tokens, run first
just chain    # flows-fixtures/devkit-chain.yaml — real worktree + containers + agents
```

Both assume devkitd is already running per the above. `devkit-chain` creates
the `pro-rails/feat-testing-x` worktree and its containers and deliberately
does **not** clean them up — inspect `summary.md` / `blog.md` under
`/workspaces/projects/pro-rails/feat-testing-x` afterward, and tear down
manually (`clean-feature` via devkitd, or the project's own tooling) when
done with it.

## `agent-flow.conf`

`agent-run.sh` sources `/workspaces/agent-flow.conf` for overrides. If it
doesn't exist, the script's built-in defaults apply: image `coding-agent:latest`,
network `homelab-agents`. Both must exist on the host running devkitd (`docker
images` / `docker network ls`) before running `devkit-chain` — this is the
most likely first-run failure, and it's worth checking before spending agent
tokens on a run that can't start its containers.
