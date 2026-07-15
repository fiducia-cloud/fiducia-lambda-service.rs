# Workflows

- `ci.yml` checks formatting, Clippy, tests, the release build, and dependency
  advisories with locked Rust dependencies. GitHub Actions are commit-pinned;
  `fiducia-clients`, `fiducia-interfaces`, `fiducia-messaging.rs`, and
  `fiducia-telemetry.rs` are checked out at the same exact revisions verified
  by the Dockerfile.
- `cli-flags.yml` audits the non-secret flag schema and proves that secret-shaped
  or invalid flags fail closed.

When a sibling contract changes, update its `ref` in `ci.yml`, the corresponding
Docker build argument, and the root README in one reviewed commit.

## Security baseline

Every executable workflow uses explicit least-privilege permissions, immutable
third-party action or container references, non-persisted checkout credentials,
concurrency control, and a job timeout. The main CI workflow validates this
directory with the digest-pinned actionlint container. Environment mutation is
forbidden unless this README documents a repository-specific platform exception.
