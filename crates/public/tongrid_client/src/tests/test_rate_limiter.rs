use super::*;

#[tokio::test(start_paused = true)]
async fn test_cancelled_wait_does_not_reserve_slots_or_burst() -> anyhow::Result<()> {
    let rate_limiter = Arc::new(RateLimiter {
        next: Mutex::new(Instant::now()),
        interval: Duration::from_secs(1),
    });
    let started = Instant::now();
    rate_limiter.acquire().await;
    for _ in 0..5 {
        let waiting = rate_limiter.clone();
        let task = tokio::spawn(async move { waiting.acquire().await });
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.is_err());
    }
    rate_limiter.acquire().await;
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    let a = rate_limiter.clone();
    let b = rate_limiter.clone();
    tokio::join!(a.acquire(), b.acquire());
    assert_eq!(started.elapsed(), Duration::from_secs(3));
    Ok(())
}

struct RateLimitWarnings(std::sync::Mutex<Vec<String>>);

impl log::Log for RateLimitWarnings {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() == log::Level::Warn
            && metadata.target() == "tongrid_client::tongrid_client::rate_limiter"
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            if let Ok(mut messages) = self.0.lock() {
                messages.push(record.args().to_string());
            }
        }
    }

    fn flush(&self) {}
}

#[tokio::test(start_paused = true)]
async fn test_default_zero_and_maximum_rate_with_warning() -> anyhow::Result<()> {
    static WARNINGS: RateLimitWarnings = RateLimitWarnings(std::sync::Mutex::new(Vec::new()));
    log::set_logger(&WARNINGS).map_err(|error| anyhow::anyhow!("set test logger: {error}"))?;
    log::set_max_level(log::LevelFilter::Warn);

    let builder = super::super::builder::Builder::new("default_rate_limit");
    assert_eq!(builder.rate_limit, 1000);
    let channel = Channel::from_static("http://localhost:8080").connect_lazy();
    let transport = RateLimitedChannel::new(channel, builder.rate_limit, Duration::from_secs(10));
    let clone = transport.clone();
    transport.rate_limiter.acquire().await;
    let started = Instant::now();
    clone.rate_limiter.acquire().await;
    assert!(started.elapsed() >= Duration::from_millis(1));

    for (requested, expected_interval) in [
        (0, Duration::from_micros(200)),
        (1, Duration::from_secs(1)),
        (5000, Duration::from_micros(200)),
        (5001, Duration::from_micros(200)),
        (u32::MAX, Duration::from_micros(200)),
    ] {
        let configured =
            super::super::builder::Builder::new("configured_rate_limit").with_rate_limit(requested);
        let limiter = RateLimiter::new(configured.rate_limit);
        assert_eq!(limiter.interval, expected_interval);
    }
    let warnings = WARNINGS
        .0
        .lock()
        .map_err(|_| anyhow::anyhow!("warning capture lock"))?;
    assert_eq!(warnings.len(), 2, "only over-limit settings should warn");
    for (warning, requested) in warnings.iter().zip([5001, u32::MAX]) {
        assert!(warning.contains(&format!("Requested {requested} RPS")));
        assert!(warning.contains("using 5000 RPS"));
    }
    Ok(())
}
