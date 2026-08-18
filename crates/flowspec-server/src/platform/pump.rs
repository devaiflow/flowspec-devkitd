//! Drains the run-event outbox (`unpushed_events`) to the platform: events
//! first (ascending sequence, per run), then a fresh `FlowRun` state
//! snapshot, then marks the batch pushed. A spare snapshot push is harmless
//! (the endpoint is an upsert) -- state is pushed on every run that had
//! events this tick.

use super::client::PlatformClient;
use flowspec_app::ports::{StateStore, StoredRunEvent};
use flowspec_app::use_cases::queries::flow_run_snapshot;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct PumpConfig {
    pub poll_interval: Duration,
    pub event_batch_size: usize,
}

/// Runs until `shutdown` is cancelled. Transport failures are logged and
/// retried next tick -- never fatal, and never block run execution (the
/// scheduler doesn't wait on this loop for anything).
pub async fn run(
    client: Arc<PlatformClient>,
    store: Arc<dyn StateStore>,
    config: PumpConfig,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(config.poll_interval) => {}
        }
        if let Err(e) = drain_once(&client, &store, config.event_batch_size).await {
            tracing::warn!("platform pump: drain failed: {e}");
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum PumpError {
    #[error("store error: {0}")]
    Store(#[from] flowspec_app::ports::StoreError),
}

async fn drain_once(
    client: &PlatformClient,
    store: &Arc<dyn StateStore>,
    batch_size: usize,
) -> Result<(), PumpError> {
    let events = store.unpushed_events(batch_size).await?;
    if events.is_empty() {
        return Ok(());
    }

    // Group by run, preserving ascending sequence within each run (the
    // store already returns them ordered `(run_id, sequence)`).
    let mut by_run: BTreeMap<String, Vec<StoredRunEvent>> = BTreeMap::new();
    for (run_id, event) in events {
        by_run.entry(run_id).or_default().push(event);
    }

    for (run_id, run_events) in by_run {
        let record = match store.load_run(&run_id).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%run_id, "pump: failed to load run: {e}");
                continue;
            }
        };
        let Some(platform_run_id) =
            flowspec_app::use_cases::queries::platform_run_id(record.idempotency_key.as_deref())
        else {
            // Not a platform-originated run (started via MCP): it has no
            // platform mirror. Mark its events pushed anyway so the outbox
            // doesn't grow unboundedly -- there is no destination for them.
            if let Some(max_seq) = run_events.iter().map(|e| e.sequence).max() {
                let _ = store.mark_events_pushed(&run_id, max_seq).await;
            }
            continue;
        };
        let platform_run_id = platform_run_id.to_string();

        if let Err(e) = client.push_events(&platform_run_id, &run_events).await {
            tracing::warn!(%run_id, %platform_run_id, "pump: push_events failed: {e}");
            continue;
        }

        match flow_run_snapshot(store.clone(), &run_id).await {
            Ok(snapshot) => {
                if let Err(e) = client.push_state(&platform_run_id, &snapshot).await {
                    tracing::warn!(%run_id, %platform_run_id, "pump: push_state failed: {e}");
                    // Events already landed; don't mark pushed on a state
                    // failure so the next tick re-pushes both (push_events
                    // is idempotent via (run_id, sequence) INSERT OR IGNORE).
                    continue;
                }
            }
            Err(e) => {
                tracing::warn!(%run_id, "pump: flow_run_snapshot failed: {e}");
                continue;
            }
        }

        if let Some(max_seq) = run_events.iter().map(|e| e.sequence).max()
            && let Err(e) = store.mark_events_pushed(&run_id, max_seq).await
        {
            tracing::warn!(%run_id, "pump: mark_events_pushed failed: {e}");
        }
    }

    Ok(())
}
