use figment::{
    Figment,
    providers::{Env, Format, Yaml},
};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutorConfig {
    #[serde(default = "default_cli_tool")]
    pub cli_tool: String,
}

fn default_cli_tool() -> String {
    "agent-run".to_string()
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        ExecutorConfig {
            cli_tool: default_cli_tool(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    /// Authorities accepted in the inbound `Host` header for the MCP
    /// Streamable HTTP endpoint (DNS-rebinding protection). Must include the
    /// exact `host:port` a remote MCP client (e.g. OpenClaw over Tailscale)
    /// dials, or rmcp returns 403 Forbidden. Defaults to loopback only.
    #[serde(default = "default_allowed_hosts")]
    pub allowed_hosts: Vec<String>,
    pub devkitd_url: String,
    pub devkitd_auth_token: Option<String>,
    #[serde(default = "default_flows_dir")]
    pub flows_dir: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default = "default_step_timeout_secs")]
    pub default_step_timeout_secs: u64,
    #[serde(default = "default_deadline_margin_secs")]
    pub deadline_margin_secs: u64,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_max_step_output_kb")]
    pub max_step_output_kb: u64,
    #[serde(default = "default_max_subflow_depth")]
    pub max_subflow_depth: u32,
    #[serde(default)]
    pub executor: ExecutorConfig,
}

fn default_listen_addr() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_allowed_hosts() -> Vec<String> {
    vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ]
}
fn default_flows_dir() -> String {
    "./flows".to_string()
}
fn default_db_path() -> String {
    "./flowspec.db".to_string()
}
fn default_step_timeout_secs() -> u64 {
    3600
}
fn default_deadline_margin_secs() -> u64 {
    30
}
fn default_poll_interval_secs() -> u64 {
    5
}
fn default_max_step_output_kb() -> u64 {
    256
}
fn default_max_subflow_depth() -> u32 {
    8
}

#[derive(Debug, thiserror::Error)]
#[error("configuration error: {0}")]
pub struct ConfigError(Box<figment::Error>);

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        ConfigError(Box::new(e))
    }
}

impl Config {
    /// Layers `~/.flowspec/config.yaml` under `FLOWSPEC_`-prefixed environment
    /// variables (env wins). Fails fast with the figment error pretty-printed.
    pub fn load() -> Result<Config, ConfigError> {
        Self::from_yaml_file(&dirs_home_config_path())
    }

    /// Same layering as `load`, but against an explicit YAML file path. Exists so
    /// tests can exercise the file-then-env layering without touching `$HOME`.
    pub fn from_yaml_file(path: &str) -> Result<Config, ConfigError> {
        Figment::new()
            .merge(Yaml::file(path))
            .merge(Env::prefixed("FLOWSPEC_"))
            .extract()
            .map_err(ConfigError::from)
    }
}

fn dirs_home_config_path() -> String {
    match std::env::var_os("HOME") {
        Some(home) => format!("{}/.flowspec/config.yaml", home.to_string_lossy()),
        None => ".flowspec/config.yaml".to_string(),
    }
}
