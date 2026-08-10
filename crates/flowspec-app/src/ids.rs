use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a run id of the form `run_<epoch-seconds>_<counter>`.
/// Monotonic and unique within a single process; good enough for tests and
/// the single-writer SQLite task.
pub fn new_run_id() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("run_{secs}_{n:06x}")
}
