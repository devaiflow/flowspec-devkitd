# OpenClaw validation — Phase 5.2

Not a coding milestone — an empirical one. The code steps (5.1) are covered by
automated tests against in-memory fakes (`crates/flowspec-server/tests/mcp_tools.rs`);
this script is the only way to validate that a real LLM host actually
understands the tool contract. Run it after 5.1 is merged, against a live
`flowspec-server` (real devkitd, real SQLite) with OpenClaw as the driver.

## Setup

1. Start devkitd (see `docs/devkitd-dev.md`).
2. Start flowspec-server: `cargo run --bin flowspec-server`, pointed at a
   `flows_dir` containing at least one flow with a `human_loop: true` step
   (e.g. `flows-fixtures/human-loop.yaml`, or `tracer.yaml`). `listen_addr`
   must be non-loopback (e.g. `0.0.0.0:8080`) for a remote OpenClaw to reach
   it, and `allowed_hosts` in `~/.flowspec/config.yaml` (or `FLOWSPEC_ALLOWED_HOSTS`)
   must include the **exact authority OpenClaw dials, including the port**
   (e.g. `box.tail1234.ts.net:8080`) — rmcp 3.x's DNS-rebinding protection
   403s any `Host` header not in that list, and the default is loopback-only.
   This is the first thing to check if OpenClaw can't connect at all.
3. Register the server in OpenClaw as an MCP server at
   `http://<host>:8080/mcp` (adjust for `listen_addr`).
4. Drive everything from Signal (or whatever chat surface OpenClaw is
   configured with) — **no terminal**.

## The four-beat lifecycle

1. **Start**: "Run the feature flow for X" → the host should call
   `list_flows`, then `start_flow` with correctly-shaped `inputs` (matching
   the declared input names/types) and an `idempotency_key`.
2. **Approval notification**: OpenClaw polls `pending_approvals` on its
   heartbeat/cron; a message should arrive on Signal containing the waiting
   step's output as context (not just "something needs approval").
3. **Act**: "Approve it" / "Reject: the plan misses the migration step" →
   the host should call the correct tool (`approve_step` / `reject_step`),
   pass the rejection feedback through verbatim, and the run should resume
   accordingly.
4. **Completion**: visible via `get_run_status` showing `phase: completed`
   (or `failed`), surfaced back to the user without another manual prompt.

**Exit criterion** (roadmap Phase 5): the full lifecycle completes from a
single chat message, with zero terminal interaction.

## Mistake log

Every host mistake is a description/schema bug in `mcp_server/server.rs`,
**not** a host bug — log it here, fix the tool contract, and re-run. Budget:
two iteration rounds.

| # | What the host did | Expected | Root cause | Fix applied |
|---|---|---|---|---|
| | | | | |

Fill this table in during the actual run. When a fix is applied, it should
also update:
- The tool's `description` / field `#[schemars(description = ...)]` in
  `crates/flowspec-server/src/mcp_server/server.rs` and the `use_cases`
  request/response structs.
- The `insta` snapshot at
  `crates/flowspec-server/tests/snapshots/mcp_tools__tool_schema.snap`
  (`INSTA_UPDATE=always cargo test -p flowspec-server --test mcp_tools tool_schema_snapshot`).
- `docs/notes.md`'s Phase 5 section, with the outcome.
