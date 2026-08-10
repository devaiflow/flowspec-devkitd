//! `Devkitd` port implementation over a real (or stub) devkitd MCP server.
//!
//! Every devkitd payload is JSON-encoded text inside `content[0].text` — a
//! double decode. Error mapping is discriminated **by call-site, never by
//! message text**: see `start`, `wait`, `cancel` below and
//! `docs/devkitd-dev.md` for the wire contract this adapter assumes.

use async_trait::async_trait;
use flowspec_app::ports::{
    Devkitd, DevkitdError, JobHandle, LivenessSink, StartRequest, StepOutput,
};
use rmcp::model::{CallToolRequestParams, CallToolResult, ContentBlock};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{RoleClient, ServiceExt};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;

/// Configuration for `DevkitdClient`.
#[derive(Debug, Clone)]
pub struct DevkitdClientConfig {
    /// The full MCP endpoint, including `/mcp`.
    pub url: String,
    /// Raw bearer token (no `"Bearer "` prefix) — required for non-loopback devkitd.
    pub auth_token: Option<String>,
    /// `job-status` poll interval while a job is `received`/`running`.
    pub poll_interval: Duration,
    /// Truncation threshold for step stdout/stderr.
    pub max_step_output_kb: u64,
    /// Backoff delays for transport errors during polling. A failure at index
    /// `i` sleeps `poll_retry_delays[i]` (clamped to the remaining deadline)
    /// before retrying; exhausting the list gives up with `Unreachable`.
    pub poll_retry_delays: Vec<Duration>,
}

impl DevkitdClientConfig {
    /// `poll_retry_delays` defaults to `[1, 2, 4, 8, 16, 30]s` (~61s budget).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            auth_token: None,
            poll_interval: Duration::from_secs(5),
            max_step_output_kb: 256,
            poll_retry_delays: [1, 2, 4, 8, 16, 30]
                .into_iter()
                .map(Duration::from_secs)
                .collect(),
        }
    }
}

/// Real devkitd MCP client adapter. Holds a single cached connection behind a
/// mutex, reconnecting on the next call after any transport failure.
pub struct DevkitdClient {
    config: DevkitdClientConfig,
    conn: Mutex<Option<RunningService<RoleClient, ()>>>,
}

/// Wire shape of a `job-status` response. `exit_code` is nullable — the
/// `cancelled` state carries exactly `{"state":"cancelled"}`, no exit_code.
#[derive(Debug, Deserialize)]
struct JobStatusEnvelope {
    state: String,
    #[serde(default)]
    stdout: Option<String>,
    #[serde(default)]
    stderr: Option<String>,
    #[serde(default)]
    exit_code: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct JobCreatedEnvelope {
    job_id: String,
}

impl DevkitdClient {
    pub fn new(config: DevkitdClientConfig) -> Self {
        Self {
            config,
            conn: Mutex::new(None),
        }
    }

    /// Ensure a connection exists, then issue `tools/call`. Any transport
    /// error (including connect failure) invalidates the cached connection
    /// and maps to `Unreachable`; a synchronous `isError` from devkitd is
    /// returned as `Ok` for the caller to interpret by call-site.
    async fn invoke(
        &self,
        name: &str,
        arguments: Option<Map<String, Value>>,
    ) -> Result<CallToolResult, DevkitdError> {
        let mut guard = self.conn.lock().await;

        if guard.is_none() {
            let mut transport_config =
                StreamableHttpClientTransportConfig::with_uri(self.config.url.clone());
            if let Some(token) = &self.config.auth_token {
                transport_config = transport_config.auth_header(token.clone());
            }
            let transport = StreamableHttpClientTransport::from_config(transport_config);
            match ().serve(transport).await {
                Ok(service) => *guard = Some(service),
                Err(_) => return Err(DevkitdError::Unreachable),
            }
        }

        let service = guard.as_ref().expect("connection just established");
        let mut params = CallToolRequestParams::new(name.to_string());
        if let Some(arguments) = arguments {
            params = params.with_arguments(arguments);
        }
        let result = service.call_tool(params).await;

        match result {
            Ok(r) => Ok(r),
            Err(_) => {
                *guard = None; // invalidate; next call reconnects
                Err(DevkitdError::Unreachable)
            }
        }
    }

    async fn cancel_best_effort(&self, handle: &JobHandle) {
        if let Err(e) = self.cancel(handle).await {
            tracing::warn!(job_id = %handle.0, error = %e, "devkitd: deadline cancel failed");
        }
    }
}

fn first_text(result: &CallToolResult) -> Option<&str> {
    result.content.iter().find_map(|c| match c {
        ContentBlock::Text(t) => Some(t.text.as_str()),
        _ => None,
    })
}

/// Truncate `raw` to `max_kb` KiB at a char boundary, appending an explicit
/// marker. Returns `raw` unchanged if it's already within budget.
fn truncate_str(raw: &str, max_kb: u64) -> String {
    let limit = (max_kb as usize).saturating_mul(1024);
    if raw.len() <= limit {
        return raw.to_string();
    }
    let mut end = limit;
    while end > 0 && !raw.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated at {max_kb} KB by flowspec]", &raw[..end])
}

/// Decode stdout per the adapter's contract: truncate first if oversized
/// (returned as a string, marker included), else try structured JSON, else
/// fall back to the plain string.
fn decode_stdout(raw: &str, max_kb: u64) -> Value {
    let limit = (max_kb as usize).saturating_mul(1024);
    if raw.len() > limit {
        return Value::String(truncate_str(raw, max_kb));
    }
    serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

/// Clamp `delay` so a sleep never runs past `deadline`; `None` deadline means
/// unbounded (hooks without `timeout:`).
fn clamp_to_deadline(delay: Duration, deadline: Option<SystemTime>) -> Duration {
    match deadline {
        None => delay,
        Some(dl) => match dl.duration_since(SystemTime::now()) {
            Ok(remaining) => delay.min(remaining),
            Err(_) => Duration::ZERO,
        },
    }
}

#[async_trait]
impl Devkitd for DevkitdClient {
    async fn start(&self, req: StartRequest) -> Result<JobHandle, DevkitdError> {
        let mut args = match req.args {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            other => {
                // Contract violation on our own side — the scheduler always
                // builds an object. Surface it rather than silently dropping.
                let mut map = Map::new();
                map.insert("value".to_string(), other);
                map
            }
        };
        // devkitd turns `null` into `""` server-side, which corrupts
        // bool-flag args (e.g. `verbose`) — omit rather than send null.
        args.retain(|_, v| !v.is_null());
        if let Some(seconds) = req.timeout_seconds {
            args.insert(
                "_timeout_seconds".to_string(),
                Value::String(seconds.to_string()),
            );
        }

        let result = self.invoke(&req.tool, Some(args)).await?;

        if result.is_error == Some(true) {
            let text = first_text(&result).unwrap_or_default();
            return Err(DevkitdError::ToolError {
                stdout: String::new(),
                stderr: truncate_str(text, self.config.max_step_output_kb),
                exit_code: -1,
            });
        }

        let text = first_text(&result).ok_or_else(|| DevkitdError::ToolError {
            stdout: String::new(),
            stderr: "devkitd: job creation response had no text content".to_string(),
            exit_code: -1,
        })?;
        let envelope: JobCreatedEnvelope =
            serde_json::from_str(text).map_err(|e| DevkitdError::ToolError {
                stdout: String::new(),
                stderr: format!("devkitd: unparseable job creation response: {e}"),
                exit_code: -1,
            })?;
        Ok(JobHandle(envelope.job_id))
    }

    async fn wait(
        &self,
        handle: &JobHandle,
        deadline: Option<SystemTime>,
        liveness: LivenessSink,
    ) -> Result<StepOutput, DevkitdError> {
        let mut args = Map::new();
        args.insert("job_id".to_string(), Value::String(handle.0.clone()));

        let mut consecutive_failures = 0usize;

        loop {
            if deadline.is_some_and(|dl| SystemTime::now() >= dl) {
                self.cancel_best_effort(handle).await;
                return Err(DevkitdError::Timeout);
            }

            match self.invoke("job-status", Some(args.clone())).await {
                Err(DevkitdError::Unreachable) => {
                    if consecutive_failures >= self.config.poll_retry_delays.len() {
                        return Err(DevkitdError::Unreachable);
                    }
                    let delay = clamp_to_deadline(
                        self.config.poll_retry_delays[consecutive_failures],
                        deadline,
                    );
                    consecutive_failures += 1;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    continue;
                }
                Err(other) => return Err(other),
                Ok(result) => {
                    consecutive_failures = 0;

                    if result.is_error == Some(true) {
                        // Only cause on this call-site: unknown job_id (devkitd
                        // restart, retention expiry, or a bogus handle).
                        return Err(DevkitdError::Interrupted);
                    }

                    liveness();

                    let text = first_text(&result).ok_or_else(|| DevkitdError::ToolError {
                        stdout: String::new(),
                        stderr: "devkitd: job-status response had no text content".to_string(),
                        exit_code: -1,
                    })?;
                    let envelope: JobStatusEnvelope =
                        serde_json::from_str(text).map_err(|e| DevkitdError::ToolError {
                            stdout: String::new(),
                            stderr: format!("devkitd: unparseable job-status response: {e}"),
                            exit_code: -1,
                        })?;

                    match envelope.state.as_str() {
                        "received" | "running" => {
                            let delay = clamp_to_deadline(self.config.poll_interval, deadline);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                        }
                        "cancelled" => return Err(DevkitdError::Cancelled),
                        "done" => {
                            let max_kb = self.config.max_step_output_kb;
                            let stdout_raw = envelope.stdout.unwrap_or_default();
                            let stderr_raw = envelope.stderr.unwrap_or_default();
                            return match envelope.exit_code {
                                Some(0) => Ok(StepOutput {
                                    output: decode_stdout(&stdout_raw, max_kb),
                                }),
                                Some(code) if (1..=255).contains(&code) => {
                                    Err(DevkitdError::ToolError {
                                        stdout: truncate_str(&stdout_raw, max_kb),
                                        stderr: truncate_str(&stderr_raw, max_kb),
                                        exit_code: code as i32,
                                    })
                                }
                                Some(-2) => Err(DevkitdError::Timeout),
                                Some(-1) => Err(DevkitdError::ToolError {
                                    stdout: truncate_str(&stdout_raw, max_kb),
                                    stderr: truncate_str(&stderr_raw, max_kb),
                                    exit_code: -1,
                                }),
                                other => Err(DevkitdError::ToolError {
                                    stdout: truncate_str(&stdout_raw, max_kb),
                                    stderr: format!(
                                        "devkitd: contract violation, exit_code={other:?}"
                                    ),
                                    exit_code: -1,
                                }),
                            };
                        }
                        unknown => {
                            tracing::warn!(
                                state = %unknown,
                                job_id = %handle.0,
                                "devkitd: unknown job-status state; continuing to poll"
                            );
                            let delay = clamp_to_deadline(self.config.poll_interval, deadline);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn cancel(&self, handle: &JobHandle) -> Result<(), DevkitdError> {
        let mut args = Map::new();
        args.insert("job_id".to_string(), Value::String(handle.0.clone()));

        let mut attempt = 0u32;
        loop {
            match self.invoke("job-cancel", Some(args.clone())).await {
                // Either a real cancellation or `isError` because the job is
                // already gone — devkitd's cancel is idempotent either way.
                Ok(_) => return Ok(()),
                Err(DevkitdError::Unreachable) => {
                    attempt += 1;
                    if attempt >= 3 {
                        return Err(DevkitdError::Unreachable);
                    }
                    tokio::time::sleep(Duration::from_millis(200 * attempt as u64)).await;
                }
                Err(other) => return Err(other),
            }
        }
    }
}
