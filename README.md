# fiducia-lambda-service

Function runtime + durable workflow engine for fiducia.cloud. A Rust port of the
Gleam/Erlang `gleam-lambda-runner`.

Two subsystems share one process:

- **Child runner** — runs stored functions (nodejs / python3 / ruby / bash /
  containerized runtimes) in **reusable, sandboxed child processes**. Warm
  workers are keyed by reuse key, reaped when idle, and can be dispatched to
  `dd-container-pool` over NATS instead of running locally. Each local child
  returns one newline-framed stdout result capped at 1 MiB (including the
  newline), so an untrusted function cannot force unbounded result buffering.
- **Workflow engine** — a Temporal-style **durable step machine** (`activity` /
  `sleep` / `waitSignal`) over a persistent store. A scheduler polls for due
  runs and advances each by one step per tick; a crash resumes from the store.

## Architecture

| Concern | Mechanism |
| --- | --- |
| Messaging | **NATS** — JetStream for durable workflow lifecycle events, Core NATS request/reply for container-pool dispatch |
| Coordination (optional) | **fiducia-node** via `fiducia-client` — run leases + fencing tokens, idempotency claims, service registration |
| Function definitions | **Postgres** via `psql` (`LAMBDA_DATABASE_URL`) |
| Shared types | `fiducia-interfaces` |

fiducia-node is **optional**: with no `FIDUCIA_BASE_URL` the engine runs
single-node with permissive leases. See the repo's `messaging-architecture`
guidance — NATS is delivery, fiducia-node is authority.

## HTTP API

All mutating routes require one of `X-Server-Auth` / `X-Lambda-Runner-Auth` /
`X-Agent-Auth` matching the configured secret.

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/invoke/{function_id}` | Invoke a stored function by UUID or slug |
| POST | `/check` | Validate a definition (check-only run) |
| POST | `/destroy/{reuse_key}` | Tear down a warm child worker |
| POST | `/workflows/start` | Start a workflow run |
| GET | `/workflows/runs` | List runs (`?definition=&limit=`) |
| GET | `/workflows/runs/{run_id}` | Get a run with its steps |
| POST | `/workflows/runs/{run_id}/signal` | Deliver a signal |
| POST | `/workflows/runs/{run_id}/cancel` | Cancel a run |
| GET | `/healthz`, `/metrics`, `/docs/api` | Ops surfaces (public) |

## Build & run

```sh
cargo build --release --locked
PORT=8083 LAMBDA_SERVER_AUTH_SECRET=… NATS_URL=nats://… \
  LAMBDA_DATABASE_URL=postgres://… ./target/release/fiducia-lambda-service
```

## Configuration

Every knob is read once at boot from the environment (`src/config.rs`,
`src/workflow/engine.rs`). Secrets are marked; never log them.

| Env var | Type | Default | Description |
| --- | --- | --- | --- |
| `HOST` | string | `0.0.0.0` | Bind address |
| `PORT` | integer | `8083` | HTTP port |
| `LAMBDA_MAX_BODY_BYTES` | integer | `5242880` | Max invoke/check/workflow body |
| `NATS_URL` | string | — | NATS server; absent → publisher/dispatcher no-op |
| `NATS_WORKFLOW_EVENT_SUBJECT` | string | `dd.remote.workflows.events` | Workflow lifecycle event subject |
| `FIDUCIA_BASE_URL` / `FIDUCIA_EDGE_URL` | string | — | Optional fiducia-node coordination endpoint |
| `WORKFLOW_ENGINE_ENABLED` | string | enabled | Toggle the durable workflow scheduler |
| `LAMBDA_CHILD_IDLE_MS` | integer | `300000` | Warm child idle-reap window |
| `LAMBDA_CHILD_TIMEOUT_MS` | integer | `30000` | Hard per-invocation timeout |
| `LOG_FORMAT` | string | human | `json` for structured logs |
| `LAMBDA_DATABASE_URL` | string (**secret**) | — | Postgres URL for definition loading (psql) |
| `LAMBDA_SERVER_AUTH_SECRET` | string (**secret**) | — | Shared secret on mutating routes; also `SERVER_AUTH_SECRET` / `REMOTE_DEV_SERVER_SECRET` |

### flags-2-env

CLI flags map to these env vars through the pinned
[`flags-2-env`](https://github.com/ORESoftware/flags-2-env) parser
(`vendor/flags-2-env` submodule, schema in `.cli-flags.toml`, audited in CI by
`.github/workflows/cli-flags.yml`):

```sh
git submodule update --init --recursive
make -C vendor/flags-2-env all
scripts/with-flags2env.sh --port 8083 --nats-url nats://localhost:4222 -- \
  ./target/release/fiducia-lambda-service
```

## Security

- **Audit:** `cargo audit` is green (`cargo audit` exits 0). See
  `.cargo/audit.toml` for four accepted `rustls-webpki` 0.102.8 advisories
  (RUSTSEC-2026-0104 / 0098 / 0099 / 0049). They are reached only through
  `async-nats v0.38.0` (which hard-pins `rustls-webpki ^0.102`); the fix requires
  async-nats ≥ 0.49, a breaking major bump, so it is accepted with rationale
  rather than forced. The residual webpki only verifies the trusted internal
  NATS broker's TLS certificate. `rustls-pemfile` (RUSTSEC-2025-0134) is an
  informational "unmaintained" warning, not a vulnerability.
- **Auth:** every mutating route requires one of `X-Server-Auth` /
  `X-Lambda-Runner-Auth` / `X-Agent-Auth` matching the configured secret; the
  guard is **fail-closed** — requests are rejected when the secret is
  unconfigured or mismatched.
- **Input handling:** no `unwrap`/`panic` on request-derived input; request
  bodies are size-limited (`LAMBDA_MAX_BODY_BYTES`) and parsed fallibly. Secrets
  are never written to logs. Child stdout is read through a `MAX_RESULT_BYTES`
  (1 MiB) bounded view before it is converted into the invocation result.
