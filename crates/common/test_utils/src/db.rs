use anyhow::Context;
use std::fs::{File, OpenOptions};
use std::time::Duration;
use stonfi_scylla_client::client::ScyllaClient;

/// Compose Scylla host address; no environment/config override is accepted.
pub const LOCAL_SCYLLA_URL: &str = "127.0.0.1:19042";
const KEYSPACE_PREFIX: &str = "tongrid_test_";
const SCHEMA_LOCK_FILE: &str = "tongrid-scylla-test-schema.lock";
const CLEANUP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Owns the isolated Scylla keyspace used by one integration test.
///
/// Tests share the local Scylla instance at `127.0.0.1:19042`, while each
/// environment gets a random keyspace that is dropped before setup and when
/// the environment is dropped. Schema changes are serialized across test
/// processes to keep concurrent migrations from overloading the shared node.
#[must_use = "keep the environment alive until the test no longer uses its database"]
pub struct ScyllaTestEnv {
    keyspace: String,
}

impl ScyllaTestEnv {
    /// Prepares a unique keyspace name for a test using the local Scylla instance.
    ///
    /// # Errors
    ///
    /// Returns an error when the local Scylla instance cannot be reached or the
    /// keyspace cannot be cleaned before setup.
    pub async fn new() -> anyhow::Result<Self> {
        let keyspace = format!("{KEYSPACE_PREFIX}{:032x}", rand::random::<u128>());
        Self::prepare(keyspace).await
    }

    async fn prepare(keyspace: String) -> anyhow::Result<Self> {
        drop_keyspace(&keyspace).await?;
        Ok(Self { keyspace })
    }

    /// Returns the unquoted CQL identifier reserved for this test.
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Drops the test keyspace and reports cleanup failures to the caller.
    ///
    /// Dropping the environment also performs this cleanup. Call this method
    /// when a test needs cleanup errors to fail the test explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when the local Scylla instance cannot be reached or the
    /// keyspace cannot be dropped.
    pub async fn cleanup(mut self) -> anyhow::Result<()> {
        let keyspace = std::mem::take(&mut self.keyspace);
        drop_keyspace(&keyspace).await
    }
}

impl Drop for ScyllaTestEnv {
    fn drop(&mut self) {
        if self.keyspace.is_empty() {
            return;
        }

        let keyspace = std::mem::take(&mut self.keyspace);
        let cleanup_thread = std::thread::Builder::new()
            .name("tongrid-scylla-test-cleanup".to_owned())
            .spawn(move || cleanup_keyspace_blocking(&keyspace));

        match cleanup_thread {
            Ok(thread) => match thread.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => log::error!("Failed to clean Scylla test keyspace: {error:#}"),
                Err(_) => log::error!("Scylla test keyspace cleanup thread panicked"),
            },
            Err(error) => {
                log::error!("Failed to start Scylla test keyspace cleanup thread: {error}")
            }
        }
    }
}

async fn drop_keyspace(keyspace: &str) -> anyhow::Result<()> {
    let _schema_lock = acquire_schema_lock().await?;
    let client = ScyllaClient::builder(LOCAL_SCYLLA_URL, keyspace)
        .with_max_parallel_queries(1)
        .with_request_timeout(CLEANUP_REQUEST_TIMEOUT)
        .with_retry_count(0)
        .build()
        .await
        .with_context(|| format!("failed to connect to local Scylla at {LOCAL_SCYLLA_URL}"))?;
    client
        .execute_unprepared(
            &format!("DROP KEYSPACE IF EXISTS {keyspace}"),
            "scylla_test_env_cleanup",
        )
        .await
        .with_context(|| format!("failed to drop Scylla test keyspace {keyspace}"))?;
    Ok(())
}

fn cleanup_keyspace_blocking(keyspace: &str) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build Scylla test cleanup runtime")?;
    runtime.block_on(drop_keyspace(keyspace))
}

/// Serializes schema creation and cleanup across test processes.
/// Returns an error if the lock cannot be acquired within 30 seconds.
pub async fn acquire_schema_lock() -> anyhow::Result<File> {
    tokio::task::spawn_blocking(acquire_schema_lock_blocking)
        .await
        .context("Scylla test schema lock task failed")?
}

fn acquire_schema_lock_blocking() -> anyhow::Result<File> {
    let path = std::env::temp_dir().join(SCHEMA_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| {
            format!(
                "failed to open Scylla test schema lock at {}",
                path.display()
            )
        })?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "Scylla test schema lock timed out"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error).context("failed to acquire Scylla test schema lock"),
        }
    }
    Ok(file)
}
