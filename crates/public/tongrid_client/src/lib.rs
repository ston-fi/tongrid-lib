//! Async client for the Tongrid gRPC APIs.
//!
//! Tests use generated loopback tonic services without native backend dependencies.
//! Historical live provider comparisons are maintained in private app tests.
//!
//! [`TongridClient`](tongrid_client::TongridClient) implements both
//! `ton::ton_core::traits::state_provider::StateProvider` and
//! `ton::ton_core::traits::emulation_provider::EmulationProvider`. Pass clones
//! of the client as the two explicit capabilities required by
//! `ton::contracts::ContractClient::builder`.
//!
//! The endpoint is caller supplied: call `with_tongrid_url` before `build`.
//! Omitting it returns [`errors::TongridApiError::InvalidArguments`] before DNS.
//! `build` obtains an initial status; the last client clone's drop aborts its
//! status and DNS refresh workers. Generated RPC handles do not own those workers.
//!
//! Rate limiting is always enabled and shared by all services and clones, including `ContractClient`
//! calls and sync-status stream establishment/reconnection. Streaming messages and cached reads
//! consume no additional slots. An unset limit defaults to 1000 RPS; values above 5000 RPS log a
//! warning and are capped at 5000. Explicit zero also selects 5000 RPS; it does not disable the limiter.
//! `client_timeout_ms` also bounds quota waits; expiration returns
//! gRPC `DeadlineExceeded`. Cancelling a queued call does not reserve future capacity.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use tongrid_client::tongrid_client::TongridClient;
//! let client = TongridClient::builder("my-client")?
//!     .with_tongrid_url("http://localhost:8080".to_string())
//!     .with_rate_limit(20)
//!     .build().await?;
//! let _status = client.last_sync_status();
//! let _contracts = ton::contracts::ContractClient::builder(client.clone(), client)?.build()?;
//! # Ok(())
//! # }
//! ```
//!
//! Generated accessor transport types now contain `RateLimitedChannel`. Prefer inferred
//! service types; callers explicitly naming the old `Channel` transport must update that type.

pub use tongrid_proto; // re-export

pub mod errors;
mod interceptor;
pub mod tongrid_client;
