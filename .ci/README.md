# Build helpers

These three Python standard-library scripts support the build and GitHub release
workflows. Run them from the repository root:

- `install_protoc.py` installs checksum-verified protoc using `protoc.json`.
- `prepare_native.py` prepares TON sources at the revision in `native.json`.
- `install_tool.py release-plz-linux-x86_64` installs checksum-verified release-plz
  using `tools.json`. Use `release-plz-darwin-arm64` for local macOS validation.

Downloaded tools live in `.tools/`; native sources live in `target/native-ton/`.
Both directories are ignored by Git. Rust checks run directly in the
[build workflow](../.github/workflows/build.yml).
