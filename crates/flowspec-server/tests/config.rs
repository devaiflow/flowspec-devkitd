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
