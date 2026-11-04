# Releases and compatibility

All 17 packages inherit one workspace version and Apache-2.0. Distribution is
GitHub-only: release-plz uses Git tags as its baseline, creates one `vX.Y.Z` tag
and GitHub release, and never publishes to crates.io. Cargo and release-plz both
disable registry publishing. Only `tongrid_lib` owns the tag and root changelog.

## Automation

[Build-Test](../.github/workflows/build.yml) runs one Linux job with Clippy, build
and unit tests on pushes to `main`, pull requests and manual dispatch. Release runs
only after this job succeeds, on this repository's `main` branch. Failed,
cancelled or skipped checks cannot authorize a release.

The [release workflow](../.github/workflows/release.yml), using checksum-pinned
release-plz 0.4.2, runs these steps:

1. Release the current version only if the tested commit belongs to a merged
   `release-plz-` PR and its shared tag does not already exist.
2. Refresh remote tags, then open/update a separate version/changelog PR for
   changes since the last tag. All packages inherit the same next version; the
   lockfile and internal dependency requirements are updated together.
3. Explicitly dispatch Build-Test on the release PR branch. GitHub's built-in
   token does not trigger normal PR workflows for PRs it creates.

Without an existing tag, the first PR prepares the current version's changelog;
it need not increment the version. Subsequent changes produce a version bump.
Merging the reviewed release PR is the release decision. Re-running an already
released version does not create another tag. Main/release runs are serialized
and are not cancelled midway by newer pushes.

Enable GitHub Actions to create pull requests in repository settings. The workflow
uses only the built-in GitHub token with contents, pull-request and workflow-dispatch
permissions; no personal token, registry token or OIDC publishing permission is
required. Release jobs use no caches shared with PR builds. API compatibility
review is manual (`semver_check = false`); Git-only versioning does not certify
that a change is backward compatible.

## Release validation

Before tagging or distributing a revision:

- Pass the [CI and relevant extended checks](testing.md), downstream builds and native builds on
  claimed platforms. Verify hosted results and warmed cache reuse separately
  from local checks. Broader Debian or architecture claims need their own evidence.
- Review source archives, generated docs, fetched refs and workflow artifacts
  for secrets, private data, complete resources and license notices. A clean
  scanner result alone is insufficient.
- Verify installation and examples from the exact remote revision without
  private credentials.
- Document compatibility changes and the previous coherent version pin.

Maintainers review version PRs before merging; visibility changes remain separate.
Application owners validate and pin the selected revision; deployment remains
an application operation.

## Compatibility

During 0.x, incompatible public API or MSRV changes require a minor-version
change. Preserve v3 service paths, protobuf tags/reserved names, stored formats
and transaction LT/hash identity. Bug fixes preserve supported contracts.

Use one canonical Git URL and immutable revision for all Tongrid packages and
commit the consumer lockfile. Review resolved TON dependencies for a single type
identity. The library lockfile is not inherited by downstream Cargo consumers;
see [dependency policy](dependencies.md). Restore the consumer's prior coherent
pin and lockfile for source rollback; this does not imply a data migration.
