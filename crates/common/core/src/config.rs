use config::{ConfigError, FileFormat};
use std::path;
use std::path::Path;

/// Resolve a secret's configured value as an environment variable name first.
/// Missing variables preserve literal-value compatibility; resolution is one hop.
pub fn resolve_secret(value: &str) -> anyhow::Result<String> {
    match std::env::var(value) {
        Ok(secret) => Ok(secret),
        Err(std::env::VarError::NotPresent) => Ok(value.to_owned()),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "Secret environment value is not valid Unicode"
        )),
    }
}

/// Merge YAML defaults, an optional file and prefixed environment overrides.
/// Errors suppress parser/deserialization payloads to avoid leaking secret values.
pub fn read_config<'a, T: serde::de::Deserialize<'a>>(
    config_path: &str,
    default_config: &str,
    env_prefix: &str,
) -> anyhow::Result<T, ConfigError> {
    let config_path = Path::new(config_path);
    log::info!(
        "Loading {:?} from {:?}",
        std::any::type_name::<T>(),
        path::absolute(config_path).map_err(|e| ConfigError::Foreign(e.into()))?,
    );

    let default_src = config::File::from_str(default_config, FileFormat::Yaml);
    let file_src = config::File::from(config_path).required(false);
    let env_src = config::Environment::with_prefix(env_prefix)
        .prefix_separator("_")
        .separator("__")
        .try_parsing(true);

    let builder = config::Config::builder()
        .add_source(default_src)
        .add_source(file_src)
        .add_source(env_src)
        .build()
        .map_err(|_| {
            ConfigError::Message(
                "Failed to build configuration; check YAML and environment overrides".into(),
            )
        })?;
    // Parser and deserialization errors can contain secret values from the input.
    // Do not retain the original error, including as an error source.
    let config = builder.try_deserialize().map_err(|_| {
        ConfigError::Message(
            "Failed to deserialize configuration; check required fields and value types".into(),
        )
    })?;
    log::info!("Loaded configuration for {}", std::any::type_name::<T>());
    Ok(config)
}
