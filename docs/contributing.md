# Contributing

Start with the [architecture](architecture.md), [repository contract](specs/repository.md)
and the contract for the affected [client](specs/client.md) or
[backend](specs/backend.md) component. Keep changes focused and preserve
public APIs, wire/storage formats and resource ownership outside the requested change.

## Checks

CI runs Clippy, build and unit tests. See [testing](testing.md) for the exact
commands and native prerequisites. For local formatting and workflow changes:

```sh
cargo fmt --all --check
actionlint
git diff --check
```

Review documentation links, dependency changes and the final diff. Build/release
helpers use Python 3.12's standard library and live in [.ci](../.ci/README.md).

## Rust prerequisites

Install Rust 1.97.1. Extended checks also use MSRV 1.95.0, cargo-nextest 0.9.132
and cargo-deny 0.19.0.
Install checksum-verified protoc for Linux x86_64 or macOS Apple Silicon:

```sh
python3 .ci/install_protoc.py
export PROTOC="$PWD/.tools/protoc/bin/protoc"
export PROTOC_INCLUDE="$PWD/.tools/protoc/include"
```

Run the [Rust checks](testing.md) selected for your
change. Core/protocol/client checks need no native TON. Full workspace checks
require native prerequisites but never application credentials or live services.

## Adding or changing a crate

- Keep packages in their responsibility group under `crates/`. Add exact paths
  to Cargo workspace membership; the CI commands cover the whole workspace.
- Inherit workspace version, edition, MSRV, license, authors, repository and
  `publish = false`. Keep the shared version coherent across all packages.
- Crates own their schemas, proto inputs, examples and reviewed fixtures. Review
  provenance and notices; no include/path dependency may escape the repository.
- Default and all-feature tests must be offline or loopback-only. Keep explicit
  disposable-service suites ignored and document their setup and cleanup.
- Update existing specifications, crate docs and the crate map when behavior or
  ownership changes. API, feature, dependency, MSRV, lifecycle and packaged-resource
  changes also require downstream consumer and artifact checks.

Review the final diff and report the exact checks run and remaining gaps.
Publication, tags, repository visibility and deployment follow the separate
[release policy](releasing.md); local validation does not authorize them.
