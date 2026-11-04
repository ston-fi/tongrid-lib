use anyhow::Context;
use stonfi_scylla_client::client::ScyllaClient;
use tongrid_db::test_support::init_test_db;
use tongrid_test_utils::db::LOCAL_SCYLLA_URL;

#[ignore = "requires explicitly provisioned disposable loopback Scylla"]
#[tokio::test]
async fn test_databases_are_isolated_and_explicit_cleanup_drops_only_owned_keyspace(
) -> anyhow::Result<()> {
    let (first_env, first) = init_test_db().await.context("initialize first DB")?;
    let (second_env, second) = init_test_db().await.context("initialize second DB")?;
    first.progress().save_initial_height(11).await?;
    second.progress().save_initial_height(22).await?;
    assert_eq!(first.progress().load_initial_height().await?, Some(11));
    assert_eq!(second.progress().load_initial_height().await?, Some(22));

    let first_keyspace = first_env.keyspace().to_owned();
    drop(first);
    first_env.cleanup().await.context("cleanup first DB")?;
    let client = ScyllaClient::builder(LOCAL_SCYLLA_URL, "system_schema")
        .with_retry_count(0)
        .build()
        .await
        .context("connect inspection client")?;
    let rows: Vec<(String,)> = client
        .select(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
            (first_keyspace,),
            "test_keyspace_cleanup",
        )
        .await?;
    assert!(rows.is_empty());
    assert_eq!(second.progress().load_initial_height().await?, Some(22));
    drop(second);
    second_env.cleanup().await?;
    Ok(())
}
