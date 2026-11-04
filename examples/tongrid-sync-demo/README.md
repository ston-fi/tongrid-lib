# Tongrid setup example

Read [main.rs](main.rs) for the setup sequence: connect Scylla, initialize TON,
wire **block → account → classifier → jetton**, then run sync and the v3 API.
[setup.rs](setup.rs) holds hardcoded component settings, checkpoint initialization
and API service registration. There is no separate application framework or CLI.

Install the [native Rust prerequisites](../../docs/contributing.md#rust-prerequisites).
Run from the repository root:

```sh
docker compose up -d --wait scylla
cargo run --locked -p tongrid_lib --example tongrid-sync-demo
```

The example checks CQL readiness before contacting TON. If Scylla is unavailable,
it exits with instructions to start Compose (`docker-compose up -d scylla` also
works). The dedicated database listens on `127.0.0.1:19042`; the API uses
`127.0.0.1:50051` and supports gRPC reflection. Ctrl-C stops the demo. Stop Scylla
with `docker compose stop scylla`. `docker compose down` stops all services; add
`-v` only to delete the demo data and checkpoints.

Compose also provides an optional disposable S3 emulator at
`http://127.0.0.1:19000` for the
[backup file-manager tests](../../docs/testing.md#test-tiers). The sync demo does
not use it.

It uses **mainnet public nodes shipped by ton-rs**, starts at the current
masterchain block on first run, and resumes saved checkpoints on restart.
There is no historical backfill, backup, verifier, resync or account-fix stage.
Classification uses dynamic detection without private code presets. The coordinator
and worker run inside the binary on an ephemeral loopback port. Worker concurrency
and emulator threads are both limited to two. Only one demo instance may run.

The API includes SendMsg, which broadcasts submitted messages to mainnet;
the demo itself never signs or broadcasts. Watch `JETTON_SYNC` progress for the
fully completed pipeline. Public-node availability and throughput are not guaranteed;
this small setup is for learning, not production deployment.

CI compiles the example without Docker or TON access. Run its offline preflight
regression locally:

```sh
cargo test --locked -p tongrid_lib --example tongrid-sync-demo
```
