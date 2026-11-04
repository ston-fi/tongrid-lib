//! Shared, cancellation-safe rate limiting above Tongrid backend selection.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::transport::Channel;
use tower::{Service, ServiceExt};

/// Transport returned by the generated service accessors. Configure rate limiting on the builder.
/// Clones share a single quota, including startup and sync-status stream establishment.
#[derive(Clone)]
pub struct RateLimitedChannel {
    channel: Channel,
    rate_limiter: Arc<RateLimiter>,
    timeout: Duration,
}

struct RateLimiter {
    next: Mutex<Instant>,
    interval: Duration,
}

impl RateLimitedChannel {
    pub(super) fn new(channel: Channel, rate: u32, timeout: Duration) -> Self {
        Self {
            channel,
            rate_limiter: Arc::new(RateLimiter::new(rate)),
            timeout,
        }
    }
}

impl RateLimiter {
    fn new(rate: u32) -> Self {
        const MAX_RPS: u32 = 5000;
        if rate > MAX_RPS {
            log::warn!("[tongrid_client][rate_limiter] Requested {rate} RPS exceeds maximum {MAX_RPS}; using {MAX_RPS} RPS");
        }
        let rate = if rate == 0 {
            MAX_RPS
        } else {
            rate.min(MAX_RPS)
        };
        Self {
            next: Mutex::new(Instant::now()),
            interval: Duration::from_nanos(1_000_000_000u64.div_ceil(u64::from(rate))),
        }
    }

    async fn acquire(&self) {
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        // Cancellation while queued or sleeping does not reserve a future slot.
        *next = Instant::now() + self.interval;
    }
}

impl Service<Request<Body>> for RateLimitedChannel {
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let mut channel = self.channel.clone();
        let rate_limiter = self.rate_limiter.clone();
        let timeout = self.timeout;
        Box::pin(async move {
            let operation = async move {
                let ready = channel.ready().await?;
                rate_limiter.acquire().await;
                ready.call(request).await.map_err(Into::into)
            };
            tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| {
                    tonic::Status::deadline_exceeded(
                        "Tongrid rate limiter or transport deadline exceeded",
                    )
                })?
        })
    }
}

#[cfg(test)]
#[path = "../tests/test_rate_limiter.rs"]
mod tests;
