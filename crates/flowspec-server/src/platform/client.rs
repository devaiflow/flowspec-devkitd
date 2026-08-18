//! Typed `reqwest` wrapper over the five `devaiflow-platform` agent-facing
//! endpoints (`docs/platform-agent-api.md`, mirrored by
//! `devaiflow-platform/scripts/mock-runtime.mjs`). The runtime always
//! connects outbound to the platform; this client never listens.

use crate::config::PlatformConfig;
use flowspec_app::ports::{RunEventType, StoredRunEvent};
use flowspec_app::use_cases::queries::FlowRunSnapshot;
use flowspec_app::wire::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("platform returned {status}: {body}")]
    Http { status: u16, body: String },
}

/// One entry from `GET /api/agent/v1/actions`. `payload`'s shape depends on
/// `kind` (`TriggerRunActionPayloadSchema` / `Approve...` / `Reject...` /
/// `Cancel...` in `src/lib/run-schemas.ts`) -- left as `Value` and decoded by
/// the poller, which knows which kind it's holding.
#[derive(Debug, Clone, Deserialize)]
pub struct Action {
    pub id: String,
    pub run_id: String,
    pub kind: String,
    pub payload: Value,
    #[allow(dead_code)] // not consumed today; kept for parity with the wire shape
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
struct ActionsResponse {
    actions: Vec<Action>,
}

#[derive(Debug, Serialize)]
struct AckBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_run_id: Option<String>,
}

/// Wire shape of one `RunEventSchema` entry (`src/lib/run-schemas.ts`).
/// `timestamp` is RFC3339 -- the platform's Zod schema takes any string, but
/// RFC3339 is what every other flowspec-devkitd timestamp already uses on
/// the wire (`flowspec_app::wire::Timestamp`).
#[derive(Debug, Serialize)]
struct WireEvent {
    #[serde(rename = "type")]
    event_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    step_id: Option<String>,
    timestamp: Timestamp,
    sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
}

#[derive(Debug, Serialize)]
struct EventsBody {
    events: Vec<WireEvent>,
}

pub struct PlatformClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl PlatformClient {
    pub fn new(config: &PlatformConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: config.url.trim_end_matches('/').to_string(),
            token: config.token.expose().to_string(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn authed(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        builder.bearer_auth(&self.token)
    }

    /// Non-destructive: unacked/undeleted actions redeliver on the next call.
    pub async fn get_actions(&self) -> Result<Vec<Action>, PlatformError> {
        let resp = self
            .authed(self.http.get(self.url("/api/agent/v1/actions")))
            .send()
            .await?;
        let resp = check_status(resp).await?;
        Ok(resp.json::<ActionsResponse>().await?.actions)
    }

    /// `409 action_not_pending` is treated as success: it means we crashed
    /// after acting but before acking on a previous attempt, or a duplicate
    /// delivery landed twice. Never DELETE an action already acked -- for
    /// `trigger_run` that retroactively fails a live run
    /// (`devaiflow-platform/src/pages/api/agent/v1/actions/[id]/index.ts`).
    pub async fn ack_action(
        &self,
        id: &str,
        runtime_run_id: Option<String>,
    ) -> Result<(), PlatformError> {
        let resp = self
            .authed(
                self.http
                    .post(self.url(&format!("/api/agent/v1/actions/{id}/ack")))
                    .json(&AckBody { runtime_run_id }),
            )
            .send()
            .await?;
        if resp.status().as_u16() == 409 {
            return Ok(());
        }
        check_status(resp).await?;
        Ok(())
    }

    /// Reserved for actions that are genuinely un-executable (invalid
    /// `flow_doc`, ambiguous approval target). Never call this on an action
    /// already acked.
    pub async fn delete_action(&self, id: &str) -> Result<(), PlatformError> {
        let resp = self
            .authed(
                self.http
                    .delete(self.url(&format!("/api/agent/v1/actions/{id}"))),
            )
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    pub async fn push_state(
        &self,
        platform_run_id: &str,
        snapshot: &FlowRunSnapshot,
    ) -> Result<(), PlatformError> {
        let resp = self
            .authed(
                self.http
                    .post(self.url(&format!("/api/agent/v1/runs/{platform_run_id}/state")))
                    .json(snapshot),
            )
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }

    /// `events` must be non-empty -- the platform's `BodySchema` requires
    /// `.min(1)`. Ascending `sequence` within the batch, matching how they
    /// were persisted.
    pub async fn push_events(
        &self,
        platform_run_id: &str,
        events: &[StoredRunEvent],
    ) -> Result<(), PlatformError> {
        if events.is_empty() {
            return Ok(());
        }
        let body = EventsBody {
            events: events.iter().map(wire_event).collect(),
        };
        let resp = self
            .authed(
                self.http
                    .post(self.url(&format!("/api/agent/v1/runs/{platform_run_id}/events")))
                    .json(&body),
            )
            .send()
            .await?;
        check_status(resp).await?;
        Ok(())
    }
}

fn wire_event(e: &StoredRunEvent) -> WireEvent {
    WireEvent {
        event_type: event_type_str(e.event_type),
        step_id: e.step_id.clone(),
        timestamp: e.timestamp.into(),
        sequence: e.sequence,
        payload: if e.payload.is_null() {
            None
        } else {
            Some(e.payload.clone())
        },
    }
}

fn event_type_str(t: RunEventType) -> &'static str {
    match t {
        RunEventType::RunStarted => "run_started",
        RunEventType::StepActivated => "step_activated",
        RunEventType::StepStarted => "step_started",
        RunEventType::StepStreamDelta => "step_stream_delta",
        RunEventType::StepCompleted => "step_completed",
        RunEventType::StepFailed => "step_failed",
        RunEventType::StepWaitingApproval => "step_waiting_approval",
        RunEventType::StepWaitingOnSubflow => "step_waiting_on_subflow",
        RunEventType::ApprovalResolved => "approval_resolved",
        RunEventType::RunCompleted => "run_completed",
        RunEventType::RunFailed => "run_failed",
        RunEventType::RunCancelled => "run_cancelled",
    }
}

async fn check_status(resp: reqwest::Response) -> Result<reqwest::Response, PlatformError> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        let status_code = status.as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(PlatformError::Http {
            status: status_code,
            body,
        })
    }
}
