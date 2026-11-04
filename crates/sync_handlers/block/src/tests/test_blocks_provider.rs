use super::BlocksProvider;
use std::time::Duration;
use tokio::sync::oneshot;
use tongrid_ton::tg_ton::block_stream::{BlockStream, BlockStreamItem};

struct UnavailableStream {
    fail: bool,
    started: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
}
#[async_trait::async_trait]
impl BlockStream for UnavailableStream {
    async fn next(&mut self) -> anyhow::Result<Option<BlockStreamItem>> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        if self.fail {
            anyhow::bail!("provider unavailable")
        }
        std::future::pending().await
    }
}
impl Drop for UnavailableStream {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

#[tokio::test]
async fn test_drop_releases_block_stream_during_failure_or_pending_fetch() -> anyhow::Result<()> {
    for fail in [true, false] {
        let (started, ready) = oneshot::channel();
        let (dropped, released) = oneshot::channel();
        let stream = UnavailableStream {
            fail,
            started: Some(started),
            dropped: Some(dropped),
        };
        let provider = BlocksProvider::new(stream, 1, 1).await?;
        tokio::time::timeout(Duration::from_secs(1), ready).await??;
        drop(provider);
        tokio::time::timeout(Duration::from_secs(1), released).await??;
    }
    Ok(())
}

#[tokio::test]
async fn test_zero_stream_capacity_or_batch_limit_returns_error() {
    for (buffer, batch) in [(0, 1), (1, 0)] {
        let stream = UnavailableStream {
            fail: true,
            started: None,
            dropped: None,
        };
        assert!(BlocksProvider::new(stream, buffer, batch).await.is_err());
    }
}
