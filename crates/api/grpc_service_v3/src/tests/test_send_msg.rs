use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Barrier;
use ton::block_tlb::{CommonMsgInfo, CommonMsgInfoExtIn};
use ton::ton_core::types::tlb_core::{MsgAddressExt, TLBCoins};
use ton::ton_core::types::TonAddress;

fn request(timeout_ms: u32) -> anyhow::Result<SendMsgReturnHashRequest> {
    let msg = Msg::new(
        CommonMsgInfo::ExtIn(CommonMsgInfoExtIn {
            src: MsgAddressExt::NONE,
            dst: TonAddress::ZERO.to_msg_address_int(),
            import_fee: TLBCoins::ZERO,
        }),
        TonCell::empty().clone(),
    );
    Ok(SendMsgReturnHashRequest {
        body: msg.to_boc()?,
        timeout_ms,
    })
}

struct Sender {
    calls: AtomicUsize,
    // Number of failures before a successful call; usize::MAX never succeeds.
    fail_count: usize,
    delay: Duration,
    expected_body: Vec<u8>,
    completed: AtomicUsize,
    dropped: AtomicUsize,
    barrier: Option<Arc<Barrier>>,
}

impl Sender {
    fn new(body: &[u8], fail_count: usize, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            fail_count,
            delay,
            expected_body: body.to_vec(),
            completed: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            barrier: None,
        })
    }
}

struct InFlight<'a>(&'a AtomicUsize);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl TonMsgSender for Sender {
    async fn send_msg(&self, boc: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(boc == self.expected_body, "Sender received a changed BOC");
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let _guard = InFlight(&self.dropped);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        tokio::time::sleep(self.delay).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(call >= self.fail_count, "synthetic rejection");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn test_concurrent_round_keeps_every_sender_and_normalized_hash() -> anyhow::Result<()> {
    let req = request(1000)?;
    let expected = Msg::<TonCell>::from_boc(req.body.clone())?
        .cell_hash_normalized()?
        .into_vec();
    let barrier = Arc::new(Barrier::new(2));
    let first = Arc::new(Sender {
        barrier: Some(barrier.clone()),
        ..Sender {
            calls: AtomicUsize::new(0),
            fail_count: 0,
            delay: Duration::ZERO,
            expected_body: req.body.clone(),
            completed: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            barrier: None,
        }
    });
    let second = Arc::new(Sender {
        barrier: Some(barrier),
        delay: Duration::from_millis(150),
        calls: AtomicUsize::new(0),
        fail_count: 0,
        expected_body: req.body.clone(),
        completed: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
    });
    let start = Instant::now();
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![first.clone(), second.clone()];
    let response = send_with_senders(&senders, &SendMsgServiceConfig::default(), req).await?;
    assert_eq!(response.msg_hash, expected);
    assert_eq!(Instant::now() - start, Duration::from_millis(150));
    for sender in [first, second] {
        assert_eq!(sender.calls.load(Ordering::SeqCst), 1);
        assert_eq!(sender.completed.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_mixed_success_failure_and_timeout_succeeds() -> anyhow::Result<()> {
    let req = request(1000)?;
    let success = Sender::new(&req.body, 0, Duration::ZERO);
    let failure = Sender::new(&req.body, usize::MAX, Duration::ZERO);
    let pending = Sender::new(&req.body, 0, Duration::from_secs(10));
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![success.clone(), failure, pending.clone()];
    let start = Instant::now();
    send_with_senders(
        &senders,
        &SendMsgServiceConfig {
            attempt_timeout: Duration::from_millis(50),
        },
        req,
    )
    .await?;
    assert_eq!(Instant::now() - start, Duration::from_millis(50));
    assert_eq!(pending.completed.load(Ordering::SeqCst), 0);
    assert_eq!(pending.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(success.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_all_failed_round_retries_then_stops_after_success() -> anyhow::Result<()> {
    let req = request(1000)?;
    let sender = Sender::new(&req.body, 1, Duration::ZERO);
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![sender.clone()];
    let start = Instant::now();
    send_with_senders(&senders, &SendMsgServiceConfig::default(), req).await?;
    assert_eq!(sender.calls.load(Ordering::SeqCst), 2);
    assert_eq!(Instant::now() - start, RETRY_INTERVAL);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_all_failures_stop_at_deadline_including_retry_sleep() -> anyhow::Result<()> {
    let req = request(250)?;
    let sender = Sender::new(&req.body, usize::MAX, Duration::ZERO);
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![sender.clone()];
    let start = Instant::now();
    assert!(matches!(
        send_with_senders(&senders, &SendMsgServiceConfig::default(), req).await,
        Err(GrpcServiceError::TimedOut(_))
    ));
    assert_eq!(Instant::now() - start, Duration::from_millis(250));
    assert_eq!(sender.calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_attempt_is_clipped_to_remaining_request_deadline() -> anyhow::Result<()> {
    let req = request(125)?;
    let sender = Sender::new(&req.body, 0, Duration::from_secs(10));
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![sender.clone()];
    let start = Instant::now();
    assert!(matches!(
        send_with_senders(
            &senders,
            &SendMsgServiceConfig {
                attempt_timeout: Duration::from_secs(2)
            },
            req
        )
        .await,
        Err(GrpcServiceError::TimedOut(_))
    ));
    assert_eq!(Instant::now() - start, Duration::from_millis(125));
    assert_eq!(sender.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_cancellation_drops_outstanding_calls_without_retry() -> anyhow::Result<()> {
    let req = request(10_000)?;
    let sender = Sender::new(&req.body, 0, Duration::from_secs(20));
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![sender.clone()];
    let config = SendMsgServiceConfig::default();
    let mut call = Box::pin(send_with_senders(&senders, &config, req));
    assert!(futures::poll!(call.as_mut()).is_pending());
    assert_eq!(sender.calls.load(Ordering::SeqCst), 1);
    drop(call);
    tokio::time::advance(Duration::from_secs(30)).await;
    assert_eq!(sender.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(sender.calls.load(Ordering::SeqCst), 1);
    assert_eq!(sender.completed.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_invalid_boc_and_zero_deadline_never_broadcast() -> anyhow::Result<()> {
    let req = request(0)?;
    let sender = Sender::new(&req.body, 0, Duration::ZERO);
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![sender.clone()];
    assert!(matches!(
        send_with_senders(
            &senders,
            &SendMsgServiceConfig::default(),
            SendMsgReturnHashRequest {
                body: vec![0, 1, 2],
                timeout_ms: 1000
            }
        )
        .await,
        Err(GrpcServiceError::InvalidArgument(_))
    ));
    assert!(matches!(
        send_with_senders(&senders, &SendMsgServiceConfig::default(), req).await,
        Err(GrpcServiceError::TimedOut(_))
    ));
    assert_eq!(sender.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn test_context_rejects_empty_senders_and_zero_attempt_timeout() -> anyhow::Result<()> {
    assert!(validate_senders(&[], &SendMsgServiceConfig::default()).is_err());
    let sender: Arc<dyn TonMsgSender> = Sender::new(&[], 0, Duration::ZERO);
    assert!(validate_senders(
        &[sender],
        &SendMsgServiceConfig {
            attempt_timeout: Duration::ZERO
        }
    )
    .is_err());
    Ok(())
}

#[test]
fn test_service_config_keeps_existing_shape_and_adds_broadcast_bound() -> anyhow::Result<()> {
    let config: crate::config::ServicesConfig = serde_json::from_str(
        r#"{
        "tx_service": { "get_initial_tx_from_tx_chain": true, "tree_limits": { "max_depth": 7 } },
        "jetton_service": { "max_batch_size": 23 },
        "send_msg_service": { "attempt_timeout": "250ms" }
    }"#,
    )?;
    assert!(config.tx_service.get_initial_tx_from_tx_chain);
    assert_eq!(config.tx_service.tree_limits.max_depth, 7);
    assert_eq!(config.tx_service.tree_limits.max_width, 30);
    assert_eq!(config.jetton_service.max_batch_size, 23);
    assert_eq!(
        config.send_msg_service.attempt_timeout,
        Duration::from_millis(250)
    );
    let omitted: crate::config::ServicesConfig = serde_json::from_str("{}")?;
    assert_eq!(
        omitted.send_msg_service.attempt_timeout,
        Duration::from_secs(2)
    );
    assert!(serde_json::from_str::<crate::config::ServicesConfig>(
        r#"{"jetton_service":{"concurrency":0}}"#
    )
    .is_err());
    let zero: crate::config::ServicesConfig =
        serde_json::from_str(r#"{"send_msg_service":{"attempt_timeout":"0s"}}"#)?;
    assert!(zero.send_msg_service.validate().is_err());
    Ok(())
}

struct TestLogger;
thread_local! {
    static LOG_LEVELS: std::cell::RefCell<Vec<log::Level>> = const { std::cell::RefCell::new(Vec::new()) };
}
impl log::Log for TestLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        if record.target().ends_with("services::send_msg") {
            LOG_LEVELS.with(|levels| levels.borrow_mut().push(record.level()));
        }
    }
    fn flush(&self) {}
}

#[tokio::test(start_paused = true)]
async fn test_each_failure_warns_and_only_terminal_failure_errors() -> anyhow::Result<()> {
    static LOGGER: TestLogger = TestLogger;
    log::set_logger(&LOGGER).map_err(|_| anyhow::anyhow!("Test logger already installed"))?;
    log::set_max_level(log::LevelFilter::Trace);
    LOG_LEVELS.with(|levels| levels.borrow_mut().clear());
    let req = request(10)?;
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![
        Sender::new(&req.body, usize::MAX, Duration::ZERO),
        Sender::new(&req.body, 0, Duration::from_secs(10)),
    ];
    assert!(
        send_with_senders(&senders, &SendMsgServiceConfig::default(), req)
            .await
            .is_err()
    );
    LOG_LEVELS.with(|levels| {
        assert_eq!(
            *levels.borrow(),
            vec![log::Level::Warn, log::Level::Warn, log::Level::Error]
        )
    });
    LOG_LEVELS.with(|levels| levels.borrow_mut().clear());
    let req = request(100)?;
    let senders: Vec<Arc<dyn TonMsgSender>> = vec![
        Sender::new(&req.body, usize::MAX, Duration::ZERO),
        Sender::new(&req.body, 0, Duration::ZERO),
    ];
    send_with_senders(&senders, &SendMsgServiceConfig::default(), req).await?;
    LOG_LEVELS.with(|levels| assert_eq!(*levels.borrow(), vec![log::Level::Warn]));
    Ok(())
}
