use anyhow::{anyhow, bail};
use include_dir::{include_dir, Dir};
use std::error::Error;
use stonfi_scylla_client::simple_migrator::SimpleMigrator;

static MIGRATIONS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations");

static ERROR_COLUMN_EXISTS: &str =
    "Database returned an error: The query is syntactically correct but invalid, Error message: Invalid column name";
static UNKNOWN_PROPERTY_TABLETS: &str =
    "Database returned an error: The submitted query has a syntax error, Error message: Unknown property 'tablets'";

pub(super) async fn apply_migrations(migrator: &SimpleMigrator) -> anyhow::Result<()> {
    let mut files = MIGRATIONS_DIR.find("*.cql")?.collect::<Vec<_>>();
    files.sort_by_key(|entry| entry.path());
    log::info!("Applying {} migrations...", files.len());

    for file in files {
        let path_str = file.path().display();
        log::info!("Applying: {path_str}");
        let migration = MIGRATIONS_DIR
            .get_file(file.path())
            .ok_or_else(|| anyhow!("File not found for path: {path_str}"))?
            .contents_utf8()
            .ok_or_else(|| anyhow!("contents_utf8 returns None for path: {path_str}"))?
            .to_string();

        for stmt in split_statements(&migration) {
            if let Err(err) = migrator.apply(&stmt).await {
                if error_chain_contains(&err, UNKNOWN_PROPERTY_TABLETS) {
                    log::warn!("Scylla version doesn't support 'tablets' property on keyspace creation. Please crate keyspace manually.");
                    continue;
                }
                if !error_chain_contains(&err, ERROR_COLUMN_EXISTS) {
                    bail!("Fail to apply migration from {path_str} (query: {stmt}): {err}");
                }
            }
        }
        log::info!("Applied: {path_str}");
    }
    log::info!("All migrations applied");
    Ok(())
}

fn error_chain_contains(error: &(dyn Error + 'static), expected: &str) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.to_string().contains(expected) {
            return true;
        }
        current = error.source();
    }
    false
}

fn split_statements(source: &str) -> Vec<String> {
    source
        .split(";\n")
        .filter_map(|statement| {
            let statement = statement
                .lines()
                .filter(|line| {
                    !line.starts_with("//") && !line.starts_with("--") && !line.is_empty()
                })
                .collect::<Vec<_>>()
                .join("\n");
            let statement = statement.trim();
            (!statement.is_empty()).then(|| statement.to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{error_chain_contains, UNKNOWN_PROPERTY_TABLETS};
    use anyhow::anyhow;

    #[test]
    fn test_error_chain_contains_nested_driver_message() {
        let error = anyhow!(UNKNOWN_PROPERTY_TABLETS).context("query failed");

        assert!(error_chain_contains(
            error.as_ref(),
            UNKNOWN_PROPERTY_TABLETS
        ));
        assert!(!error_chain_contains(error.as_ref(), "unrelated error"));
    }
}
