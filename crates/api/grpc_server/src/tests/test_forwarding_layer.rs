use super::{add_forward_header, ForwardingLayer};
use http::Request;
use tonic::transport::Channel;

#[tokio::test]
async fn test_invalid_forwarding_percentage_is_rejected() {
    for percentage in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.01, 1.01] {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        assert!(ForwardingLayer::new(channel, percentage).is_err());
    }
}

#[tokio::test]
async fn test_forwarding_endpoints_and_loop_prevention() -> anyhow::Result<()> {
    for (percentage, expected) in [(0.0, false), (1.0, true)] {
        let layer = ForwardingLayer::new(
            Channel::from_static("http://127.0.0.1:1").connect_lazy(),
            percentage,
        )?;
        let mut request = Request::new(());
        assert_eq!(layer.should_forward(&request), expected);
        add_forward_header(&mut request);
        assert!(!layer.should_forward(&request));
    }
    Ok(())
}

fn pending_forwarding_task() -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (finished, receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _finished = finished;
        std::future::pending::<()>().await;
    });
    (task, receiver)
}

#[tokio::test]
async fn test_full_shadow_queue_does_not_block_or_truncate_primary() -> anyhow::Result<()> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, StreamBody};
    use hyper::body::Frame;
    use std::time::Duration;

    let frames = (0..32).map(|index| Ok::<_, tonic::Status>(Frame::data(Bytes::from(vec![index]))));
    let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
    let (task, stopped) = pending_forwarding_task();
    let mut body = super::ForwardingBody {
        body: tonic::body::Body::new(StreamBody::new(tokio_stream::iter(frames))),
        sender: Some(sender),
        task: task.abort_handle(),
        complete: false,
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        for index in 0..32 {
            let frame = body
                .frame()
                .await
                .ok_or_else(|| anyhow::anyhow!("missing primary frame"))??;
            assert_eq!(frame.data_ref(), Some(&Bytes::from(vec![index])));
        }
        assert!(body.frame().await.is_none());
        anyhow::Ok(())
    })
    .await??;
    // Saturation cancels the shadow, rather than completing a truncated request.
    assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
        .await?
        .is_err());
    for _ in 0..16 {
        assert!(receiver.recv().await.is_some());
    }
    assert!(receiver.recv().await.is_none());
    Ok(())
}

#[tokio::test]
async fn test_forwarding_preserves_trailers_errors_and_cancels_on_drop() -> anyhow::Result<()> {
    use http_body_util::{BodyExt, StreamBody};
    use hyper::body::Frame;
    use std::time::Duration;
    let mut trailers = http::HeaderMap::new();
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    let frames = vec![
        Ok(Frame::trailers(trailers.clone())),
        Err(tonic::Status::cancelled("test")),
    ];
    let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
    let (task, stopped) = pending_forwarding_task();
    let mut body = super::ForwardingBody {
        body: tonic::body::Body::new(StreamBody::new(tokio_stream::iter(frames))),
        sender: Some(sender),
        task: task.abort_handle(),
        complete: false,
    };
    let primary = body
        .frame()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing trailers"))??;
    let shadow = receiver
        .recv()
        .await
        .ok_or_else(|| anyhow::anyhow!("missing shadow trailers"))??;
    assert_eq!(primary.trailers_ref(), Some(&trailers));
    assert_eq!(shadow.trailers_ref(), Some(&trailers));
    assert!(
        matches!(body.frame().await, Some(Err(status)) if status.code() == tonic::Code::Cancelled)
    );
    assert!(
        matches!(receiver.recv().await, Some(Err(status)) if status.code() == tonic::Code::Cancelled)
    );
    drop(body);
    assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
        .await?
        .is_err());
    assert!(receiver.recv().await.is_none());
    Ok(())
}

#[tokio::test]
async fn test_completed_primary_keeps_shadow_alive_until_it_finishes() -> anyhow::Result<()> {
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use std::time::Duration;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, tonic::Status>>(16);
    let mut tasks = tokio::task::JoinSet::new();
    let task = tasks.spawn(async move {
        let frame = receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing shadow body"))??;
        assert_eq!(frame.data_ref(), Some(&Bytes::from_static(b"payload")));
        assert!(receiver.recv().await.is_none());
        anyhow::Ok(())
    });
    let mut body = super::ForwardingBody {
        body: tonic::body::Body::new(Full::new(Bytes::from_static(b"payload"))),
        sender: Some(sender),
        task,
        complete: false,
    };
    assert!(body.frame().await.is_some());
    drop(body);
    // Full marks EOS with its last data frame, without requiring another poll.
    tokio::time::timeout(Duration::from_secs(1), tasks.join_next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing forwarding task"))???;
    Ok(())
}

#[tokio::test]
async fn test_forwarding_task_limit_and_last_owner_cancellation() -> anyhow::Result<()> {
    use std::time::Duration;
    use tonic_middleware::RequestInterceptor;
    let layer = ForwardingLayer::new(
        Channel::from_static("http://127.0.0.1:1").connect_lazy(),
        1.0,
    )?;
    let (finished, stopped) = tokio::sync::oneshot::channel::<()>();
    {
        let mut tasks = layer.tasks.lock();
        tasks.spawn(async move {
            let _finished = finished;
            std::future::pending::<()>().await;
        });
        for _ in 1..super::MAX_FORWARDING_TASKS {
            tasks.spawn(std::future::pending::<()>());
        }
    }
    let primary = layer
        .intercept(Request::new(tonic::body::Body::empty()))
        .await?;
    assert_eq!(layer.tasks.lock().len(), super::MAX_FORWARDING_TASKS);
    drop(primary);
    let clone = layer.clone();
    drop(layer);
    assert_eq!(clone.tasks.lock().len(), super::MAX_FORWARDING_TASKS);
    drop(clone);
    assert!(tokio::time::timeout(Duration::from_secs(1), stopped)
        .await?
        .is_err());
    Ok(())
}
