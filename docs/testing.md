# Testing and CI

## Continuous integration

[Build-Test](../.github/workflows/build.yml) runs one job on Ubuntu 24.04 x86_64
with Rust **1.97.1**. After installing native dependencies and protoc **29.3**, it
runs exactly these checks, sharing one build cache:

```sh
cargo +1.97.1 clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo +1.97.1 build --workspace --all-targets --all-features --locked
cargo +1.97.1 test --workspace --lib --all-features --locked
```

The build compiles examples and integration tests; only library unit tests execute
in CI. Live service tests remain ignored. There is no OS/MSRV matrix or separate
repository, dependency, documentation or consumer job. On main, the successful job
allows the [GitHub-only release workflow](releasing.md) to run.

## Local maintenance and extended checks

Use `cargo fmt --all --check`, `actionlint` and `git diff --check` for formatting
and workflow changes. Review local documentation links when moving files.

For changes affecting public APIs, dependencies, resources or lifecycle, select
these additional checks. They are not automatic CI jobs. Use MSRV **1.95.0** and
other platforms when validating those compatibility claims:

```sh
RUST_VERSION=1.97.1
cargo +"$RUST_VERSION" check --workspace --all-targets --locked --no-default-features
cargo +"$RUST_VERSION" nextest run --workspace --all-targets --locked --all-features --profile ci
cargo +"$RUST_VERSION" test --workspace --doc --all-features --locked
cargo +"$RUST_VERSION" run --locked -p tongrid_client --example loopback
cargo +"$RUST_VERSION" run --locked -p tongrid_core --all-features --example configuration
rm -rf target/doc
RUSTDOCFLAGS="-D warnings" cargo +"$RUST_VERSION" doc --workspace --all-features --no-deps --locked
cargo deny --locked check --config .config/deny.toml
```

The optional test runner is cargo-nextest **0.9.132**; dependency checks use
cargo-deny **0.19.0**. Clearing generated docs prevents cached source links from
surviving a rebuild. Core, protocol and client have no crate features. Check
lightweight client dependency changes from a downstream project: they must not
introduce native/backend dependencies.

## Native prerequisites

Full workspace and umbrella checks compile native TON. Direct client/core builds
do not. The lockfile selects tonlib-sys 2026.10.0; `.ci/native.json` pins TON
v2026.10 to its reviewed revision. Prepare and select those sources with:

```sh
python3 .ci/prepare_native.py
export TON_MONOREPO_DIR="$PWD/target/native-ton"
```

Linux requires Clang 21, CMake, pkg-config, libssl-dev, libsodium-dev,
libsecp256k1-dev, liblz4-dev, zlib1g-dev, automake, autoconf and libtool. CI installs
Clang 21 through the LLVM Noble archive with a checksum-pinned signing key.
macOS requires CMake, Clang and Homebrew openssl@3, lz4, pkgconf, libsodium,
secp256k1, automake, autoconf and libtool. macOS builds are checked locally.
Native compilation may download public sources; it does not need live TON nodes.

Ubuntu results do not establish support for every Debian version. Additional
platform claims require independent build/consumer evidence with the OS, compiler
and native dependencies recorded.

## Test tiers

- **Default/all features:** unit, replay, doc and loopback transport tests, plus
  offline examples. No live TON, signing, broadcasting, Scylla or S3. Tests use
  synthetic inputs, temporary directories and ephemeral ports. The large
  loopback balancing experiment remains explicitly ignored.
- **Disposable services:** ignored tests selected explicitly against disposable
  loopback Scylla/S3. Missing services are failures, never successful skips.
- **Live demo:** [tongrid-sync-demo](../examples/tongrid-sync-demo/README.md)
  is compiled in CI and executed only manually. Its offline preflight regression
  runs with `cargo test -p tongrid_lib --example tongrid-sync-demo`. Its
  dedicated Compose project owns disposable Scylla; mainnet uses ton-rs public nodes.
- **Live application checks:** owned and orchestrated by the application, outside
  library CI.

`.config/nextest.toml` limits ordinary tests to 60 seconds and disposable-service
tests to 180 seconds, with no retries. Whole-job CI timeouts are separate. Record
cold/native compilation separately from warm test execution. To establish offline
behavior, run with outbound access denied after acquiring build dependencies;
ordinary successful execution alone is not proof of network isolation.

Compose binds Scylla to `127.0.0.1:19042` and S3 to `127.0.0.1:19000`, using
nonstandard host ports. The demo and service tests use these addresses.

For the backup file-manager tests, Compose provides an in-memory Moto S3 emulator
at `http://127.0.0.1:19000`. Its healthcheck creates the `tongrid-test` bucket if
missing. Use the inert credentials `tongrid-local-test` and
`tongrid-local-test-password`. Objects are discarded when the emulator restarts;
this service is only for disposable local data.

```sh
docker compose up -d --wait --wait-timeout 90 s3
cargo nextest run -p tongrid_backup_handler --test test_file_manager --profile local-services --run-ignored only
docker compose stop s3
docker compose rm -f s3
```

Port `19000` must be free for the tests above. For other local use,
`S3_PORT=19001 docker compose up -d --wait s3` selects another loopback port;
the tests still expect `19000`. The sync demo itself does not use S3.

DB integration tests use the same Compose Scylla at `127.0.0.1:19042` as the
demo. Run them only against a disposable local instance. Library test support
creates unique test keyspaces and drops only those keyspaces during cleanup; it
does not use the demo keyspace.

```sh
docker compose up -d --wait --wait-timeout 180 scylla
cargo nextest run -p tongrid_db --all-features --profile local-services --run-ignored only
```

## Distribution

Before releasing, review source archives and generated docs for complete resources,
license notices and sensitive data. Verify the selected remote revision from a
consumer project. Consumers resolve their own lockfiles; see the
[dependency policy](dependencies.md).

CI cache keys include platform, compiler, native toolchain, lockfiles, manifests
and protoc configuration. They
exclude run IDs and source commit IDs. Verify cache reuse with a warmed hosted
run; local builds cannot establish a hosted cache hit.
