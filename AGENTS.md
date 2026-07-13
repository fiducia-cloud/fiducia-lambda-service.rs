# Agent Context — fiducia-lambda-service

Rust function runtime + durable workflow engine (Rust port of the Gleam/Erlang
`gleam-lambda-runner`). HTTP on `:8083`. Runs stored functions in reusable
sandboxed child processes and drives a Temporal-style workflow engine. Messaging
is NATS (JetStream + Core); coordination via **optional** fiducia-node through
`fiducia-client`; definitions load from Postgres via `psql`.

Build/test: `cargo build --release --locked` and `cargo test`. Path deps resolve
against the sibling `fiducia-interfaces/generated/rust` and
`fiducia-clients/clients/rust` crates in this directory.

Module map: `child_runner` (warm process pool), `runtime` (definition→command,
validators, JSON field extractors — pure port of `lambda_child_runner.erl`),
`definition` (psql loader), `workflow::{engine,store}` (durable step machine),
`nats` (JetStream events + pool dispatch), `coord` (fiducia-node authority),
`http` (axum routes), `metrics`.

## Git branch policy — temporary

Work directly on `main`. Do not create feature branches or worktrees. Preserve
uncommitted work; stop for operator guidance if switching to `main` is unsafe.

## Command safety — STRICT

Never run destructive/irreversible shell commands (`rm -rf`, raw `mv` of tracked
files, `git stash`, history rewrites). Remove/move files through git so changes
are tracked and recoverable.
