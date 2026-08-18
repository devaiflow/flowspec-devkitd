//! The outbound platform connector: an adapter, not a separate service.
//! Same process, same SQLite, same use cases as the MCP surface -- a
//! sidecar would only see `get_run_status` polls and could not emit
//! faithful events. Absent `Config::platform` disables this module
//! entirely; nothing else in `flowspec-server` depends on it.
//!
//! `client` is the typed HTTP wrapper over the five endpoints in
//! `docs/platform-agent-api.md`; `poller` drains the platform's action
//! queue; `pump` drains flowspec's own run-event outbox back to the
//! platform. Both loops poll on independent timers and never block run
//! execution -- a transport failure just gets retried next tick.

pub mod client;
pub mod poller;
pub mod pump;

pub use client::PlatformClient;
