use super::{adjust_path_to_s3, PrefetchedFile, S3FsOps};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};

#[test]
fn test_adjust_path_to_s3() {
    for path in [
        "/tmp/test",
        "./tmp/test",
        "tmp/test",
        ".//tmp/test",
        ".///tmp/test",
        ".///tmp/test/",
        ".///tmp/test//",
    ] {
        assert_eq!(adjust_path_to_s3(path), "tmp/test");
    }
}

fn pending_prefetch() -> (PrefetchedFile, oneshot::Receiver<()>) {
    let (finished, receiver) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _finished = finished;
        std::future::pending::<anyhow::Result<Vec<u8>>>().await
    });
    (
        PrefetchedFile {
            path: "next".into(),
            handle,
        },
        receiver,
    )
}

fn filesystem(prefetched: PrefetchedFile) -> anyhow::Result<S3FsOps> {
    // Construction only: these tests never contact an S3 endpoint.
    let credentials =
        ::s3::creds::Credentials::new(Some("inert"), Some("inert"), None, None, None)?;
    let client = ::s3::Bucket::new(
        "test",
        ::s3::Region::Custom {
            region: "test".into(),
            endpoint: "http://127.0.0.1:1".into(),
        },
        credentials,
    )?;
    Ok(S3FsOps {
        client,
        bucket_name: "test".into(),
        prefetched: Arc::new(Mutex::new(Some(prefetched))),
    })
}

#[tokio::test]
async fn test_discard_and_owner_drop_cancel_prefetch() -> anyhow::Result<()> {
    let (prefetched, stopped) = pending_prefetch();
    let fs = filesystem(prefetched)?;
    fs.discard_prefetch("unrelated").await;
    assert!(fs.prefetched.lock().await.is_some());
    fs.discard_prefetch("next").await;
    assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
        .await?
        .is_err());

    let (prefetched, stopped) = pending_prefetch();
    let fs = filesystem(prefetched)?;
    drop(fs);
    assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
        .await?
        .is_err());
    Ok(())
}

#[tokio::test]
async fn test_cancelled_wait_cancels_prefetch() -> anyhow::Result<()> {
    let (prefetched, stopped) = pending_prefetch();
    let fs = filesystem(prefetched)?;
    {
        let wait = fs.wait_for_prefetch("next");
        tokio::pin!(wait);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(wait.as_mut().poll(cx).is_pending()))
                .await
        );
        assert!(fs.prefetched.lock().await.is_none());
    }
    assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
        .await?
        .is_err());
    Ok(())
}

#[tokio::test]
async fn test_completed_prefetch_returns_bytes() -> anyhow::Result<()> {
    let fs = filesystem(PrefetchedFile {
        path: "next".into(),
        handle: tokio::spawn(async { Ok(vec![1, 2, 3]) }),
    })?;
    assert_eq!(fs.wait_for_prefetch("other").await, None);
    assert_eq!(fs.wait_for_prefetch("next").await, Some(vec![1, 2, 3]));
    assert!(fs.prefetched.lock().await.is_none());
    Ok(())
}
