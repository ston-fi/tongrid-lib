# Dependency policy

All library dependencies resolve from public registries or reviewed public HTTPS
Git repositories. Every project-owned Git declaration uses a full commit revision.
Libraries do not depend on application crates. Workspace validation uses Cargo.lock
and --locked; dependency updates require review and the normal source/artifact checks.

The legacy TON block codec is retained to preserve stripped-block storage bytes and
transaction identity. Its ton_types and ed25519-dalek upstream manifests omit explicit
revisions. Cargo.lock records the resolved commits; review source changes when
updating it. Cargo-deny restricts Git dependencies to these three repositories. An unlocked consumer
must review its generated lockfile; the library's own lockfile is not inherited by
Cargo consumers, which must commit and validate their own lockfiles.
Replacing this codec requires its own storage-compatibility evidence.

Three narrowly documented RustSec maintenance notices remain: bincode 2 task-wire
encoding, and derivative/yaml-rust inherited through the legacy codec. These are
maintenance advisories, not suppressed vulnerability findings. Existing encoding is
preserved; replacements require compatibility review. No vulnerability advisory
is ignored.

Registry dependencies use the broadest SemVer-compatible requirements: a major
version for 1.x and later (`"1"`), major/minor for 0.x (`"0.4"`), and the patch
for 0.0.x (`"0.0.3"`). Cargo.lock records the reviewed resolved versions; updates
preserve the workspace MSRV and all reviewed Git source identities.

The dependency-specific MPL-2.0 permission for option-ext and
CDLA-Permissive-2.0 permission for webpki-root-certs do not change the workspace's
Apache-2.0 license. Preserve dependency notices if distributing their source or
native artifacts. The TON Core unfreezer adaptation retains its full MIT notice.
Native TON itself is built from public upstream source into ignored caches; those
sources and binaries are not vendored in this repository.
