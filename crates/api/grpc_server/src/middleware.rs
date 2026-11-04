use crate::metrics;
use bytes::Bytes;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;
use tonic::async_trait;
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::{Code, Status};
use tonic_middleware::{Middleware, ServiceBound};

#[derive(Clone)]
pub(crate) struct GrpcMiddleware;

impl GrpcMiddleware {
    const UNKNOWN_NAME: &'static str = "unknown";
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl<S> Middleware<S> for GrpcMiddleware
where
    S: ServiceBound,
    S::Future: Send,
    S::Error: Debug,
{
    async fn call(&self, req: Request<Body>, mut service: S) -> Result<Response<Body>, S::Error> {
        let start_time = Instant::now();

        let client_id = match req.headers().get("x-client-id") {
            Some(v) => v.to_str().unwrap_or("parse_failed").to_string(),
            None => "empty".to_string(),
        };
        // uri format examples:
        // http://localhost:14291/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
        // http://localhost:14291/tongrid.v1.BlockGrpc/GetBlockShards
        let mut parts = req.uri().path().split('/').rev();

        // collect in reverse order
        let method_name = parts.next().unwrap_or(Self::UNKNOWN_NAME).to_string();
        let service_name = parts.next().unwrap_or(Self::UNKNOWN_NAME).to_string();
        log::debug!("[GRPC] request received: service={service_name}, method={method_name}");
        let mut result = service.call(req).await;
        let duration = start_time.elapsed();
        match &mut result {
            Ok(rsp) => {
                let wait_duration = rsp
                    .extensions()
                    .get::<Duration>()
                    .copied()
                    .unwrap_or(Duration::ZERO);
                let execution_duration = duration.saturating_sub(wait_duration);

                log::debug!(
                    "[GRPC] request completed: service={service_name}, method={method_name}"
                );
                let code = rsp
                    .headers()
                    .get("grpc-status")
                    .map(|value| Code::from_bytes(value.as_bytes()));
                let is_ok = code.is_none_or(|code| code == Code::Ok);
                if !is_ok {
                    log::warn!("[GRPC] request error: service={service_name}, method={method_name}, rsp={rsp:?}");
                }
                metrics::update(
                    &service_name,
                    &method_name,
                    is_ok,
                    wait_duration,
                    execution_duration,
                    &client_id,
                );
                let mut body = ErrorMetricsBody {
                    body: std::mem::replace(rsp.body_mut(), Body::empty()),
                    service_name,
                    method_name,
                    client_id,
                    error_recorded: false,
                };
                if let Some(code) = code {
                    body.record_error(code);
                }
                *rsp.body_mut() = Body::new(body);
            }
            Err(err) => {
                log::warn!("[GRPC] request error: service={service_name}, method={method_name}, err={err:?}");
                metrics::update(
                    &service_name,
                    &method_name,
                    false,
                    Duration::ZERO,
                    duration,
                    &client_id,
                );
                metrics::record_error(&service_name, &method_name, Code::Unknown, &client_id);
            }
        }
        result
    }
}

/// Observe terminal errors without buffering, consuming, or changing response frames.
struct ErrorMetricsBody {
    body: Body,
    service_name: String,
    method_name: String,
    client_id: String,
    error_recorded: bool,
}

impl ErrorMetricsBody {
    fn record_error(&mut self, code: Code) {
        if code != Code::Ok && !self.error_recorded {
            metrics::record_error(&self.service_name, &self.method_name, code, &self.client_id);
            self.error_recorded = true;
        }
    }
}

impl HttpBody for ErrorMetricsBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.body).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(code) = frame
                    .trailers_ref()
                    .and_then(|trailers| trailers.get("grpc-status"))
                    .map(|value| Code::from_bytes(value.as_bytes()))
                {
                    this.record_error(code);
                }
            }
            Poll::Ready(Some(Err(status))) => this.record_error(status.code()),
            _ => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, HeaderValue};
    use http_body_util::{BodyExt, StreamBody};
    use hyper::body::Frame;
    use std::convert::Infallible;
    use tonic::Status;

    fn counter(name: &str, method: &str, label: &str, value: &str) -> f64 {
        prometheus::gather()
            .into_iter()
            .filter(|family| family.name() == name)
            .flat_map(|family| family.get_metric().to_vec())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|item| item.name() == "method_name" && item.value() == method)
                    && metric
                        .get_label()
                        .iter()
                        .any(|item| item.name() == label && item.value() == value)
            })
            .map(|metric| {
                metric
                    .get_counter()
                    .as_ref()
                    .map(|counter| counter.value())
                    .unwrap_or_default()
            })
            .sum()
    }

    fn request(method: &str) -> anyhow::Result<Request<Body>> {
        Ok(Request::builder()
            .uri(format!("/tongrid.v3.Test/{method}"))
            .body(Body::empty())?)
    }

    fn trailers(code: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("grpc-status", HeaderValue::from_static(code));
        headers
    }

    async fn response(method: &str, rsp: Response<Body>) -> anyhow::Result<Response<Body>> {
        let rsp = std::sync::Arc::new(parking_lot::Mutex::new(Some(rsp)));
        let service = tower::service_fn(move |_| {
            let result = rsp
                .lock()
                .take()
                .ok_or_else(|| std::io::Error::other("service called twice"));
            std::future::ready(result)
        });
        Ok(GrpcMiddleware::new()
            .call(request(method)?, service)
            .await?)
    }

    #[tokio::test]
    async fn test_explicit_ok_header_is_success() -> anyhow::Result<()> {
        let method = "ExplicitOkHeader";
        let mut rsp = Response::new(Body::empty());
        *rsp.headers_mut() = trailers("0");
        let rsp = response(method, rsp).await?;
        assert_eq!(rsp.headers()["grpc-status"], "0");
        assert_eq!(counter("grpc_requests", method, "status", "ok"), 1.0);
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "Unknown"),
            0.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_unary_status_error_is_counted() -> anyhow::Result<()> {
        let method = "UnaryInvalidArgument";
        let rsp = response(
            method,
            Status::invalid_argument("bad input").into_http::<Body>(),
        )
        .await?;
        assert_eq!(rsp.headers()["grpc-status"], "3");
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "InvalidArgument"),
            1.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_trailer_error_preserves_stream_and_counts_once() -> anyhow::Result<()> {
        let method = "TrailerUnavailable";
        let frames: Vec<Result<Frame<Bytes>, Status>> = vec![
            Ok(Frame::data(Bytes::from_static(b"payload"))),
            Ok(Frame::trailers(trailers("14"))),
        ];
        let body = Body::new(StreamBody::new(tokio_stream::iter(frames)));
        let mut rsp = response(method, Response::new(body)).await?;
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "Unavailable"),
            0.0
        );
        let frame = rsp
            .body_mut()
            .frame()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing data"))??;
        assert_eq!(frame.data_ref(), Some(&Bytes::from_static(b"payload")));
        let frame = rsp
            .body_mut()
            .frame()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing trailers"))??;
        assert_eq!(frame.trailers_ref(), Some(&trailers("14")));
        assert!(rsp.body_mut().frame().await.is_none());
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "Unavailable"),
            1.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_body_error_is_forwarded_and_counted() -> anyhow::Result<()> {
        let method = "BodyInternal";
        let frames: Vec<Result<Frame<Bytes>, Status>> = vec![Err(Status::internal("broken body"))];
        let body = Body::new(StreamBody::new(tokio_stream::iter(frames)));
        let mut rsp = response(method, Response::new(body)).await?;
        let result = rsp
            .body_mut()
            .frame()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing body error"))?;
        let err = result
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected body error"))?;
        assert_eq!(err.code(), tonic::Code::Internal);
        assert_eq!(err.message(), "broken body");
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "Internal"),
            1.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_header_and_trailer_do_not_double_count() -> anyhow::Result<()> {
        let method = "RepeatedNotFound";
        let frames: Vec<Result<Frame<Bytes>, Status>> = vec![Ok(Frame::trailers(trailers("5")))];
        let body = Body::new(StreamBody::new(tokio_stream::iter(frames)));
        let mut rsp = Response::new(body);
        *rsp.headers_mut() = trailers("5");
        let mut rsp = response(method, rsp).await?;
        let _ = rsp
            .body_mut()
            .frame()
            .await
            .ok_or_else(|| anyhow::anyhow!("missing trailers"))??;
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "NotFound"),
            1.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_service_error_is_preserved_and_counted_as_unknown() -> anyhow::Result<()> {
        let method = "ServiceFailure";
        let service = tower::service_fn(|_| async {
            Err::<Response<Body>, _>(std::io::Error::other("service failed"))
        });
        let result = GrpcMiddleware::new().call(request(method)?, service).await;
        let err = result
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected service error"))?;
        assert_eq!(err.to_string(), "service failed");
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "Unknown"),
            1.0
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_dropped_body_does_not_invent_status() -> anyhow::Result<()> {
        let method = "DroppedBody";
        let frames: Vec<Result<Frame<Bytes>, Status>> = vec![Ok(Frame::trailers(trailers("13")))];
        let body = Body::new(StreamBody::new(tokio_stream::iter(frames)));
        drop(response(method, Response::new(body)).await?);
        assert_eq!(
            counter("grpc_errors_total", method, "grpc_code", "Internal"),
            0.0
        );
        // Keep a success path with an explicitly infallible service callable as before.
        let service =
            tower::service_fn(|_| async { Ok::<_, Infallible>(Response::new(Body::empty())) });
        let _ = GrpcMiddleware::new()
            .call(request("InfallibleService")?, service)
            .await?;
        Ok(())
    }
}
