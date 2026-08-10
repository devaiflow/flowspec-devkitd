# flowspec-devkitd — Project Spec

**What this project is:** the reference devkitd-backed runtime for the FlowSpec standard (v0.3). It loads flow definitions, executes them as durable state machines, and delegates all real work (agent steps and lifecycle hooks) to **devkitd**, a Rust daemon that exposes a native MCP server over Streamable HTTP. flowspec-devkitd consumes devkitd as an MCP client and, inbound, exposes its own MCP tool surface so an AI host can trigger, supervise, and approve runs.

This document is the project-specific reference. The standard itself lives in [`flowspec-spec.md`](./flowspec-spec.md); the implementation architecture lives in [`architecture.md`](./architecture.md).

---

## Decided Stack

| Layer | Crate / Tool | Role |
| --- | --- | --- |
| Language | Rust, edition 2024 | Implementation language |
| Async runtime | Tokio | Application + server only; never in domain |
| Build / tasks | Cargo + `just` | `just test`, `just lint`, `just run` |
| MCP | `rmcp` 3.1.2 | Inbound server + outbound client to devkitd |
| HTTP / transport | `axum` + `StreamableHttpService` | MCP over Streamable HTTP |
| Config | `figment` | YAML file layered under `FLOWSPEC_*` env vars |
| Persistence | `rusqlite` (bundled) | State store for runs, step runs, hook runs, links |
| Validation snapshots | `insta` | Structured violation output tests |
| YAML | `serde_yaml_ng` | Flow file parsing in the server adapter |

---

## Crate Dependency Policy

| Crate | Allowed dependencies |
| --- | --- |
| `flowspec-domain` | `serde`, `serde_json`, `thiserror`, `indexmap`, `semver`. **No tokio, no rmcp, no IO crates.** |
| `flowspec-app` | `flowspec-domain`, `tokio` (sync primitives only), `async-trait`, `thiserror`, `tracing` |
| `flowspec-server` | Everything: `rmcp`, `axum`, `rusqlite`, `figment`, `serde_yaml_ng`, `tracing-subscriber`, etc. |

This rule is enforced in CI by `./scripts/check-layering.sh` (`cargo tree -i tokio -p flowspec-domain` must fail).

---

## MCP Tool Surface (Phase 0–1)

The runtime exposes these MCP tools. Schemas and additional tools (list flows, run status, cancel, stream) land in later phases.

| Tool | Semantics |
| --- | --- |
| `ping` | Liveness check. Returns `"pong"`. |

Inbound MCP auth is deferred; the intended perimeter is Tailscale.

---

## Configuration

`config.rs` loads `~/.flowspec/config.yaml` and layers `FLOWSPEC_*`-prefixed environment variables on top (env wins). Fields:

- `listen_addr` (default `127.0.0.1:8080`)
- `devkitd_url` (required)
- `devkitd_auth_token` (optional; devkitd requires bearer auth off-loopback)
- `flows_dir` (default `./flows`)
- `db_path` (default `./flowspec.db`)
- `default_step_timeout_secs` (default `3600`)
- `deadline_margin_secs` (default `30`)
- `poll_interval_secs` (default `5`)
- `max_step_output_kb` (default `256`)
- `max_subflow_depth` (default `8`)
- `executor.cli_tool` (default `"agent-run"`) — the devkitd tool name used for `type: cli` steps

Configuration errors fail fast at startup with the figment error pretty-printed.

---

## Scope Cuts (Explicitly Out of Scope)

- `flowspec-app` is an empty stub in Phases 0–1; ports, scheduler, use cases, and recovery land in Phases 2/3.
- Inbound FlowSpec HTTP+SSE interface is a future second adapter.
- Streaming step *output* capture would require a devkitd contract change.
- Git/hub flow sources, cross-runtime subflows, second MCP transport, migration tooling.

See [`architecture.md`](./architecture.md) for the full "explicitly out of scope" list and preserved seams.

---

## Working Agreement

- **Domain purity:** `flowspec-domain` contains no IO, no clock, no randomness, no protocol types. Engine decisions are pure functions tested with plain in/out assertions.
- **Error discipline:** each crate defines its own `thiserror` enums. `anyhow` is only used in `main.rs`.
- **Tracing:** one span per run, child span per step; engine decisions logged at `debug`.
- **Tests:** domain tests are the bulk of coverage; integration tests use fakes and real SQLite persistence.
- **Tooling:** `rust-toolchain.toml` pins stable; `justfile` exposes common tasks; `./scripts/ci.sh` runs fmt, clippy, layering, and tests.
