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

"Sync with the remote" (or just "sync") is a **two-way** exchange — pull the
remote's commits down **and** push yours up. It is never push-only, and a clean
local tree does not by itself mean "synced": you are done only once local and
the remote hold the same commits.

To sync:

1. **Commit your work first** (`git add` + `git commit`) so the tree is clean —
   pull/merge only into a clean tree. `git pull` / `git merge` aborts when an
   incoming change touches a file you have edited, and even when it doesn't it
   buries the merge in your uncommitted work. (Can't commit yet? `git stash`,
   then `git stash pop` after step 3.)
2. `git fetch --all --prune` — safe any time; it only updates tracking refs.
3. `git pull` (fetch + merge) — or `git merge` the upstream branch — to
   integrate the remote's commits.
4. `git push` to publish yours.

Integrate with **`git merge` / `git pull`**. **Never `git rebase` to sync** — it
rewrites history and breaks shared branches.
