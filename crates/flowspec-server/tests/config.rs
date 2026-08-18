use flowspec_server::config::Config;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

// `FLOWSPEC_DEVKITD_URL` is process-global env state; serialize every test in
// this file so one test's env mutation can't leak into another running in a
// parallel thread.
static ENV_LOCK: Mutex<()> = Mutex::new(());

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn write_temp_yaml(contents: &str) -> std::path::PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "flowspec-config-test-{}-{n}.yaml",
        std::process::id()
    ));
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    path
}

#[test]
fn env_var_overrides_file_value() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml(
        r#"
devkitd_url: "http://file-configured:9000"
poll_interval_secs: 7
"#,
    );

    // SAFETY: this test owns the FLOWSPEC_DEVKITD_URL env var for its duration.
    unsafe { std::env::set_var("FLOWSPEC_DEVKITD_URL", "http://env-configured:9000") };
    let config = Config::from_yaml_file(path.to_str().unwrap()).expect("config loads");
    unsafe { std::env::remove_var("FLOWSPEC_DEVKITD_URL") };
    std::fs::remove_file(&path).ok();

    assert_eq!(config.devkitd_url, "http://env-configured:9000");
    assert_eq!(config.poll_interval_secs, 7); // untouched by env, comes from the file
}

#[test]
fn missing_optional_fields_fall_back_to_defaults() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml("devkitd_url: \"http://localhost:9000\"\n");
    let config = Config::from_yaml_file(path.to_str().unwrap()).expect("config loads");
    std::fs::remove_file(&path).ok();

    assert_eq!(config.listen_addr, "127.0.0.1:8080");
    assert_eq!(config.flows_dir, "./flows");
    assert_eq!(config.executor.cli_tool, "agent-run");
}

#[test]
fn missing_required_field_fails_fast() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml("listen_addr: \"127.0.0.1:9999\"\n");
    let result = Config::from_yaml_file(path.to_str().unwrap());
    std::fs::remove_file(&path).ok();

    assert!(
        result.is_err(),
        "devkitd_url is required and has no default"
    );
}

#[test]
fn platform_block_absent_disables_the_connector() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml("devkitd_url: \"http://localhost:9000\"\n");
    let config = Config::from_yaml_file(path.to_str().unwrap()).expect("config loads");
    std::fs::remove_file(&path).ok();

    assert!(
        config.platform.is_none(),
        "no platform: block in the file must leave the connector disabled"
    );
}

#[test]
fn platform_block_loads_from_yaml() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml(
        r#"
devkitd_url: "http://localhost:9000"
platform:
  url: "https://devaiflow.example"
  token: "dvf_abc123"
  poll_interval_secs: 5
"#,
    );
    let config = Config::from_yaml_file(path.to_str().unwrap()).expect("config loads");
    std::fs::remove_file(&path).ok();

    let platform = config.platform.expect("platform: block must be parsed");
    assert_eq!(platform.url, "https://devaiflow.example");
    assert_eq!(platform.token.expose(), "dvf_abc123");
    assert_eq!(platform.poll_interval_secs, 5);
    assert_eq!(platform.event_batch_size, 100, "must default to 100");
}

#[test]
fn platform_env_vars_nest_with_double_underscore() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml("devkitd_url: \"http://localhost:9000\"\n");

    // SAFETY: this test owns these env vars for its duration, under ENV_LOCK.
    unsafe {
        std::env::set_var("FLOWSPEC_PLATFORM__URL", "https://env-platform.example");
        std::env::set_var("FLOWSPEC_PLATFORM__TOKEN", "dvf_env_token");
    }
    let config = Config::from_yaml_file(path.to_str().unwrap()).expect("config loads");
    unsafe {
        std::env::remove_var("FLOWSPEC_PLATFORM__URL");
        std::env::remove_var("FLOWSPEC_PLATFORM__TOKEN");
    }
    std::fs::remove_file(&path).ok();

    let platform = config
        .platform
        .expect("FLOWSPEC_PLATFORM__* must nest into platform:");
    assert_eq!(platform.url, "https://env-platform.example");
    assert_eq!(platform.token.expose(), "dvf_env_token");

    // Single-underscore keys (e.g. FLOWSPEC_DEVKITD_URL) must stay
    // unaffected by adding `.split("__")` -- confirmed by every other test
    // in this file still passing, but assert it directly here too.
    assert_eq!(config.devkitd_url, "http://localhost:9000");
}

#[test]
fn devkitd_auth_token_never_appears_in_debug_output() {
    let _guard = ENV_LOCK.lock().unwrap();
    let path = write_temp_yaml(
        "devkitd_url: \"http://localhost:9000\"\ndevkitd_auth_token: \"super-secret-token\"\n",
    );
    let config = Config::from_yaml_file(path.to_str().unwrap()).expect("config loads");
    std::fs::remove_file(&path).ok();

    let debug_output = format!("{config:?}");
    assert!(
        !debug_output.contains("super-secret-token"),
        "Debug output must never leak devkitd_auth_token: {debug_output}"
    );
}
