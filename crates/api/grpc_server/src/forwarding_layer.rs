use bytes::Bytes;
use http::Request;
use http_body_util::StreamBody;
use hyper::body::Frame;
use parking_lot::Mutex;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::{AbortHandle, JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body;
use tonic::client::GrpcService;
use tonic::transport::Channel;
use tonic::{async_trait, Status};
use tonic_middleware::RequestInterceptor;
use tower::ServiceExt;

const FORWARDED_HEADER_KEY: &str = "ForwardedBy";
const FORWARDED_HEADER_VALUE: &str = "tongrid";
const MAX_FORWARDING_TASKS: usize = 128;

/// Best-effort request mirroring that never backpressures the primary body.
///
/// Clones share up to 128 forwarding tasks with a 30-second timeout and 16-frame
/// queues. Saturation or early primary-body cancellation aborts the shadow;
/// dropping the last layer cancels all outstanding forwarding tasks.
#[derive(Clone)]
pub struct ForwardingLayer {
    channel: Channel,
    percentage: f64,
    tasks: Arc<Mutex<JoinSet<()>>>,
}

impl ForwardingLayer {
    pub fn new(channel: Channel, percentage: f64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            percentage.is_finite() && (0.0..=1.0).contains(&percentage),
            "forwarding percentage must be a finite number in [0, 1], got {}",
            percentage
        );
        Ok(Self {
            channel,
            percentage,
            tasks: Arc::new(Mutex::new(JoinSet::new())),
        })
    }

    pub fn should_forward<T>(&self, req: &Request<T>) -> bool {
        !is_forwarded(req) && rand::random_bool(self.percentage)
    }
}

#[async_trait]
impl RequestInterceptor for ForwardingLayer {
    async fn intercept(&self, req: Request<Body>) -> Result<Request<Body>, Status> {
        if !self.should_forward(&req) {
            return Ok(req);
        }

        let mut tasks = self.tasks.lock();
        while tasks.try_join_next().is_some() {}
        if tasks.len() >= MAX_FORWARDING_TASKS {
            return Ok(req);
        }
        let (parts, body) = req.into_parts();
        let complete = hyper::body::Body::is_end_stream(&body);
        let (forward_sender, forward_rcv) = mpsc::channel(16);
        let forward_body = Body::new(StreamBody::new(ReceiverStream::new(forward_rcv)));

        let parts_for_primary = parts.clone();
        let mut forward_req = Request::from_parts(parts, forward_body);
        add_forward_header(&mut forward_req);
        forward_req.headers_mut().insert(
            "x-client-id",
            http::HeaderValue::from_static("traffic-forward"),
        );

        let channel = self.channel.clone();
        let task = tasks.spawn(async move {
            match tokio::time::timeout(Duration::from_secs(30), send_req(channel, forward_req))
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(_)) => log::warn!("forwarding request failed"),
                Err(_) => log::warn!("forwarding request timed out"),
            }
        });
        let orig_body = Body::new(ForwardingBody {
            body,
            sender: if complete { None } else { Some(forward_sender) },
            task,
            complete,
        });
        Ok(Request::from_parts(parts_for_primary, orig_body))
    }
}

fn is_forwarded<T>(req: &Request<T>) -> bool {
    req.headers().contains_key(FORWARDED_HEADER_KEY)
}

fn add_forward_header<T>(req: &mut Request<T>) {
    req.headers_mut().insert(
        FORWARDED_HEADER_KEY,
        http::HeaderValue::from_static(FORWARDED_HEADER_VALUE),
    );
}

// The primary request drives reads. A slow shadow never applies backpressure to it.
// An early body drop cancels forwarding. After EOF the layer owns the bounded task.
struct ForwardingBody {
    body: Body,
    sender: Option<Sender<Result<Frame<Bytes>, Status>>>,
    task: AbortHandle,
    complete: bool,
}

impl hyper::body::Body for ForwardingBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let frame = std::task::ready!(Pin::new(&mut self.body).poll_frame(cx));
        if let Some(sender) = &self.sender {
            let copy = match &frame {
                Some(Ok(frame)) => clone_frame(frame).map(Ok),
                Some(Err(status)) => Some(Err(status.clone())),
                None => None,
            };
            if let Some(copy) = copy {
                if sender.try_send(copy).is_err() {
                    // Abort the whole shadow request; never send a truncated request
                    // as if it completed successfully after dropping a frame.
                    self.task.abort();
                    self.sender = None;
                }
            } else {
                self.sender = None;
            }
        }
        if frame.is_none() || self.body.is_end_stream() {
            self.complete = true;
            self.sender = None;
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

impl Drop for ForwardingBody {
    fn drop(&mut self) {
        if !self.complete {
            self.task.abort();
        }
    }
}

fn clone_frame(frame: &Frame<Bytes>) -> Option<Frame<Bytes>> {
    if let Some(data) = frame.data_ref() {
        Some(Frame::data(data.clone()))
    } else {
        frame
            .trailers_ref()
            .map(|trailers| Frame::trailers(trailers.clone()))
    }
}

async fn send_req(mut channel: Channel, req: Request<Body>) -> anyhow::Result<()> {
    channel.ready().await?.call(req).await?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/test_forwarding_layer.rs"]
mod tests;
