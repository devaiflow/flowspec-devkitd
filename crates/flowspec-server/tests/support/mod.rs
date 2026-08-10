//! Shared test helper: a `Container` wired entirely from `flowspec-app`'s
//! in-memory fakes, for exercising the MCP tool surface without a live
//! devkitd or SQLite file.

use flowspec_app::ports::{Devkitd, FlowSource, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use flowspec_app::testkit::{FakeDevkitd, InMemoryFlowSource, InMemoryStateStore, Script};
use flowspec_domain::flow::types::FlowDefinition;
use flowspec_server::config::{Config, ExecutorConfig};
use flowspec_server::container::Container;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub fn scheduler_config() -> SchedulerConfig {
    SchedulerConfig {
        poll_interval_secs: 1,
        deadline_margin_secs: 1,
        default_step_timeout_secs: 3600,
        max_step_output_kb: 256,
        executor_cli_tool: "agent-run".to_string(),
    }
}

fn fake_config() -> Config {
    Config {
        listen_addr: "127.0.0.1:0".to_string(),
        allowed_hosts: vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ],
        devkitd_url: "http://127.0.0.1:1/mcp".to_string(),
        devkitd_auth_token: None,
        flows_dir: "./flows".to_string(),
        db_path: ":memory:".to_string(),
        default_step_timeout_secs: 3600,
        deadline_margin_secs: 1,
        poll_interval_secs: 1,
        max_step_output_kb: 256,
        max_subflow_depth: 8,
        executor: ExecutorConfig {
            cli_tool: "agent-run".to_string(),
        },
    }
}

/// A `Container` over `InMemoryStateStore` + `FakeDevkitd` + a fixed set of
/// flow definitions, with `scripts` controlling how each step's `cli`
/// invocation resolves (see `flowspec_app::testkit::Script`).
pub fn fake_container(
    flows: Vec<FlowDefinition>,
    scripts: HashMap<String, Script>,
) -> Arc<Container> {
    let state_store: Arc<dyn StateStore> = Arc::new(InMemoryStateStore::new());
    let devkitd: Arc<dyn Devkitd> =
        Arc::new(FakeDevkitd::new(scripts).with_poll_interval(Duration::from_millis(10)));
    let flow_source: Arc<dyn FlowSource> = Arc::new(InMemoryFlowSource::new(flows));
    let scheduler = Arc::new(Scheduler::new(
        state_store.clone(),
        devkitd.clone(),
        scheduler_config(),
    ));

    Arc::new(Container::from_parts(
        fake_config(),
        state_store,
        devkitd,
        flow_source,
        scheduler,
        scheduler_config(),
    ))
}

pub fn load_fixture(rel: &str) -> FlowDefinition {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../flows-fixtures")
        .join(rel);
    let text = std::fs::read_to_string(path).unwrap();
    let file: flowspec_domain::flow::types::FlowFile = serde_yaml_ng::from_str(&text).unwrap();
    file.into_definitions().into_iter().next().unwrap()
}
