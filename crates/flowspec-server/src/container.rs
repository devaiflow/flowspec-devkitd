use crate::config::Config;
use crate::devkitd::{DevkitdClient, DevkitdClientConfig};
use crate::flows::FsFlowSource;
use crate::platform::PlatformClient;
use crate::state::SqliteStore;
use flowspec_app::ports::{Devkitd, FlowSource, SchedulerConfig, StateStore};
use flowspec_app::scheduler::Scheduler;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Composition root: builds the SQLite state store, the real devkitd MCP
/// client, the filesystem flow source, and the scheduler that ties them
/// together.
pub struct Container {
    pub config: Config,
    pub state_store: Arc<dyn StateStore>,
    pub devkitd: Arc<dyn Devkitd>,
    pub flow_source: Arc<dyn FlowSource>,
    pub scheduler: Arc<Scheduler>,
    pub scheduler_config: SchedulerConfig,
    /// `None` when `config.platform` is absent -- the connector is fully
    /// disabled and nothing changes for a pure-OpenClaw deployment.
    pub platform_client: Option<Arc<PlatformClient>>,
}

impl Container {
    pub fn build(config: Config) -> anyhow::Result<Container> {
        let state_store: Arc<dyn StateStore> = Arc::new(SqliteStore::open(&config.db_path)?);

        let mut devkitd_config = DevkitdClientConfig::new(config.devkitd_url.clone());
        devkitd_config.auth_token = config
            .devkitd_auth_token
            .as_ref()
            .map(|t| t.expose().to_string());
        devkitd_config.poll_interval = Duration::from_secs(config.poll_interval_secs);
        devkitd_config.max_step_output_kb = config.max_step_output_kb;
        let devkitd: Arc<dyn Devkitd> = Arc::new(DevkitdClient::new(devkitd_config));

        let flow_source: Arc<dyn FlowSource> =
            Arc::new(FsFlowSource::load(Path::new(&config.flows_dir))?);

        let scheduler_config = SchedulerConfig {
            poll_interval_secs: config.poll_interval_secs,
            deadline_margin_secs: config.deadline_margin_secs,
            default_step_timeout_secs: config.default_step_timeout_secs,
            max_step_output_kb: config.max_step_output_kb,
            executor_cli_tool: config.executor.cli_tool.clone(),
        };
        let scheduler = Arc::new(Scheduler::new(
            state_store.clone(),
            devkitd.clone(),
            scheduler_config.clone(),
        ));

        let platform_client = config
            .platform
            .as_ref()
            .map(|p| Arc::new(PlatformClient::new(p)));

        Ok(Container {
            config,
            state_store,
            devkitd,
            flow_source,
            scheduler,
            scheduler_config,
            platform_client,
        })
    }

    /// Assemble a `Container` from already-built parts, bypassing config
    /// loading and real adapters. For tests that wire fakes directly.
    #[cfg(any(test, feature = "test-fakes"))]
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        config: Config,
        state_store: Arc<dyn StateStore>,
        devkitd: Arc<dyn Devkitd>,
        flow_source: Arc<dyn FlowSource>,
        scheduler: Arc<Scheduler>,
        scheduler_config: SchedulerConfig,
    ) -> Container {
        let platform_client = config
            .platform
            .as_ref()
            .map(|p| Arc::new(PlatformClient::new(p)));
        Container {
            config,
            state_store,
            devkitd,
            flow_source,
            scheduler,
            scheduler_config,
            platform_client,
        }
    }
}
