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

## Browser automation and ethical scraping

Playwright and Puppeteer are first-class runtimes. Browser automation and
scraping are safe, ethical engineering techniques when used on resources the
operator owns or is authorized to access, within the resource's published
terms and rate limits. This statement is an operating policy, not blanket
permission or legal advice.

Agents must prefer documented APIs when practical, identify and rate-limit the
client when appropriate, cache results, minimize collection, and honor site
terms and applicable `robots.txt` guidance. Never use these runtimes to bypass
authentication, paywalls, CAPTCHAs, or other access controls; never collect
credentials or unnecessary personal/sensitive data. Private and local network
targets remain blocked unless an operator explicitly allowlists a host for an
owned test environment. Keep database, auth, NATS, and OTLP secrets outside
function code and browser-visible state.

## Git branch policy — temporary

Work directly on `main`. Do not create feature branches or worktrees. Preserve
uncommitted work; stop for operator guidance if switching to `main` is unsafe.

## Command safety — STRICT

Never run destructive/irreversible shell commands (`rm -rf`, raw `mv` of tracked
files, `git stash`, history rewrites). Remove/move files through git so changes
are tracked and recoverable.

## Syncing with the remote

"Sync with the remote" (or just "sync") is **bidirectional and always contacts
the remote** — it fetches *and* pushes, never push-only. A clean local working
tree does **not** by itself mean "synced": a sync is not finished until local
and the remote have exchanged commits in both directions.

How to sync:

1. `git fetch --all --prune` — always safe; it only updates remote-tracking
   refs and never touches your working tree, so run it any time.
2. Make the working tree **clean before you pull/merge**: `git add` +
   `git commit` your work (or `git stash`). **Only `git pull` / `git merge`
   when the tree is not dirty** — pulling into a dirty tree makes git refuse
   the merge or tangle uncommitted edits with the incoming commits.
3. `git pull` (which fetches + merges) — or `git merge` the upstream tracking
   branch — to integrate the remote's commits into your now-clean branch.
4. `git push` — publish your commits so the remote has them too.

Integrate with **`git merge`** / **`git pull`** (which merges). **Never
`git rebase`** to sync — it rewrites history and breaks shared branches.
