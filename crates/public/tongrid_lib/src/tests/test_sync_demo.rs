use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn test_preflight_rejects_tcp_without_cql_with_bounded_wait() -> anyhow::Result<()> {
    // A TCP listener alone must not authorize migrations or a mainnet connection.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?.to_string();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        super::setup::connect_scylla(&endpoint, Duration::from_millis(50)),
    )
    .await?;
    let error = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("non-CQL endpoint was accepted"))?;
    assert!(error.to_string().contains("docker compose up"));
    Ok(())
}
