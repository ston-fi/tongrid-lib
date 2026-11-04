use crate::config::{read_config, resolve_secret};
use log::{LevelFilter, Log, Metadata, Record};
use serde::Deserialize;
use std::sync::Mutex;

struct CapturedLog(Mutex<Vec<String>>);

impl Log for CapturedLog {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        if let Ok(mut messages) = self.0.lock() {
            messages.push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

static LOGGER: CapturedLog = CapturedLog(Mutex::new(Vec::new()));

#[test]
fn test_secret_environment_resolution_preserves_literal_fallback() -> anyhow::Result<()> {
    let name = format!("TONGRID_SECRET_RESOLUTION_TEST_{}", std::process::id());
    let previous = std::env::var_os(&name);
    std::env::remove_var(&name);
    assert_eq!(resolve_secret(&name)?, name);
    assert_eq!(
        resolve_secret("literal-canary-value")?,
        "literal-canary-value"
    );
    std::env::set_var(&name, "resolved-canary-value");
    assert_eq!(resolve_secret(&name)?, "resolved-canary-value");
    let next_name = format!("{name}_NEXT");
    std::env::set_var(&next_name, "recursive-canary-value");
    std::env::set_var(&name, &next_name);
    assert_eq!(resolve_secret(&name)?, next_name);
    std::env::remove_var(&next_name);
    // An existing empty variable takes precedence; it must not fall back.
    std::env::set_var(&name, "");
    assert_eq!(resolve_secret(&name)?, "");
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        std::env::set_var(&name, std::ffi::OsString::from_vec(vec![0xff, 0xfe]));
        let error = match resolve_secret(&name) {
            Ok(_) => anyhow::bail!("non-Unicode secret was accepted"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "Secret environment value is not valid Unicode"
        );
    }
    if let Some(previous) = previous {
        std::env::set_var(&name, previous);
    } else {
        std::env::remove_var(&name);
    }
    Ok(())
}

// Deliberately no Debug: reading configuration must not require printing it.
#[derive(Deserialize)]
struct TestConfig {
    file_manager: Credentials,
    count: u32,
}

#[derive(Deserialize)]
struct Credentials {
    s3_access_key: String,
    s3_secret_key: String,
}

#[test]
fn test_config_precedence_defaults_then_file_then_environment() -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct CountConfig {
        count: u32,
        nested: Nested,
    }
    #[derive(Deserialize)]
    struct Nested {
        value: u32,
    }
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("config.yaml");
    let path = file
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid test path"))?;
    let prefix = format!("TONGRID_CORE_PRECEDENCE_{}", std::process::id());
    let env_name = format!("{prefix}_COUNT");
    let nested_name = format!("{prefix}_NESTED__VALUE");
    let previous = std::env::var_os(&env_name);
    let previous_nested = std::env::var_os(&nested_name);
    std::env::remove_var(&env_name);
    std::env::remove_var(&nested_name);
    let defaults_yaml = "count: 1\nnested:\n  value: 10";
    let defaults: CountConfig = read_config(path, defaults_yaml, &prefix)?;
    assert_eq!(defaults.count, 1);
    std::fs::write(path, "count: 2")?;
    let file_value: CountConfig = read_config(path, defaults_yaml, &prefix)?;
    assert_eq!(file_value.count, 2);
    assert_eq!(file_value.nested.value, 10);
    std::env::set_var(&env_name, "3");
    std::env::set_var(&nested_name, "30");
    let env_value = read_config::<CountConfig>(path, defaults_yaml, &prefix);
    if let Some(previous) = previous {
        std::env::set_var(&env_name, previous);
    } else {
        std::env::remove_var(&env_name);
    }
    if let Some(previous) = previous_nested {
        std::env::set_var(&nested_name, previous);
    } else {
        std::env::remove_var(&nested_name);
    }
    let env_value = env_value?;
    assert_eq!(env_value.count, 3);
    assert_eq!(env_value.nested.value, 30);
    Ok(())
}

#[test]
fn test_config_logs_and_errors_exclude_values() -> anyhow::Result<()> {
    log::set_logger(&LOGGER).map_err(|_| anyhow::anyhow!("test logger already installed"))?;
    log::set_max_level(LevelFilter::Trace);
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.yaml");
    let path = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid test path"))?;
    let defaults =
        "file_manager:\n  s3_access_key: inert-access-canary\n  s3_secret_key: inert-secret-canary\ncount: 1";
    let config: TestConfig = read_config(path, defaults, "TONGRID_CONFIG_LOG_TEST")?;
    assert_eq!(config.count, 1);
    assert_eq!(config.file_manager.s3_access_key, "inert-access-canary");
    assert_eq!(config.file_manager.s3_secret_key, "inert-secret-canary");

    std::fs::write(path, "count: 2")?;
    let config: TestConfig = read_config(path, defaults, "TONGRID_CONFIG_LOG_TEST")?;
    assert_eq!(config.count, 2);

    for (contents, expected_phase) in [
        ("count: inert-rejected-canary", "deserialize"),
        ("count: [inert-parser-canary", "build"),
    ] {
        std::fs::write(path, contents)?;
        let error = match read_config::<TestConfig>(path, defaults, "TONGRID_CONFIG_LOG_TEST") {
            Ok(_) => anyhow::bail!("invalid configuration was accepted"),
            Err(error) => error,
        };
        let diagnostic = format!("{error} {error:?}");
        assert!(diagnostic.contains(expected_phase));
        assert!(!diagnostic.contains("canary"));
        assert!(std::error::Error::source(&error).is_none());
    }

    let messages = LOGGER
        .0
        .lock()
        .map_err(|_| anyhow::anyhow!("test log lock poisoned"))?;
    assert!(messages
        .iter()
        .any(|message| message.contains("Loaded configuration")));
    assert!(messages.iter().all(|message| !message.contains("canary")));
    Ok(())
}
