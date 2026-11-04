# Repository contract

The workspace owns 17 library packages with explicit Cargo membership.
Production application composition, deployment, runtime topology and credentials
stay outside this repository. The explicit live `tongrid-sync-demo`
example under root `examples/` and the loopback-only Compose Scylla/S3 services are a
development-only exception. Cargo registers the example on `tongrid_lib`, the
umbrella package at `crates/public/tongrid_lib`;
there is no additional package or release unit. Libraries own their resources, reviewed
synthetic fixtures and third-party notices. Encoded resources require semantic
and provenance review before export.

Packages inherit Apache-2.0, the shared version, authors, MSRV and disabled
publishing from `Cargo.toml`. Development uses the pinned `rust-toolchain.toml`.
Git source distribution is separate from registry publication. Supported platform
claims require actual build and downstream consumer evidence.

CI runs one Ubuntu job on the development compiler: workspace-wide strict Clippy,
build and library unit tests. Cargo workspace membership defines coverage.
The GitHub-only release workflow runs after this job succeeds on main.

Review documentation, inherited metadata and dependency changes locally. Do not
include credentials or private values in source or diagnostics. Build/release
helpers live in `.ci/`; tool configuration lives in `.config/`. The
[testing guide](../testing.md) documents additional local checks.

Core uses public dependencies and stays
independent of protocol/backend crates. Lightweight clients must not acquire native
emulation or backend development dependencies. Test support remains a leaf.

Default and all-feature tests use offline data or loopback only. Native compilation
may fetch pinned public sources; service-dependent tests require a separately
provisioned disposable harness. No public workflow deploys the application or receives private application credentials.
The release workflow uses the built-in GitHub token only after successful main CI.

Cache keys use stable compiler, platform, dependency, feature and build inputs,
not run IDs or source revisions. Hosted execution and warmed cache reuse are
verified separately from local validation. Merging a release-plz version PR authorizes the GitHub-only release after CI.
Registry publication remains disabled; visibility changes and deployment are separate.
