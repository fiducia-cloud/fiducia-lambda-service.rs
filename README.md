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
| Coordination (optional) | **fiducia-node** via an authenticated, tenant-scoped `fiducia-client` — run leases + fencing tokens, idempotency claims, service registration |
| Function definitions | **Postgres** via `psql` (`LAMBDA_DATABASE_URL`) |
| Shared types | `fiducia-interfaces` |

fiducia-node is optional only as an explicit single-process mode: with neither
`FIDUCIA_NODE_URL` nor legacy `FIDUCIA_BASE_URL`, the engine uses local synthetic
leases. Once a node URL is configured, its internal secret is mandatory,
startup registration must succeed, and idempotency/lease errors stop the
operation rather than degrading to local authority. A configured deployment
also supplies the non-secret `FIDUCIA_SERVICE_ADDRESS` that peers can resolve.
NATS is delivery; fiducia-node is authority.

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

For the direct-node Compose path, both API and node secrets are required:

```sh
LAMBDA_SERVER_AUTH_SECRET='replace-with-an-api-secret' \
FIDUCIA_NODE_INTERNAL_SECRET='replace-with-the-node-cluster-secret' \
docker compose up --build
```

## Configuration

Every knob is read once at boot from the environment (`src/config.rs`,
`src/workflow/engine.rs`). Secrets are marked, normalized, and excluded from the
CLI flag surface; never log them.

| Env var | Type | Default | Description |
| --- | --- | --- | --- |
| `HOST` | string | `0.0.0.0` | Bind address |
| `PORT` | integer | `8083` | HTTP port |
| `LAMBDA_MAX_BODY_BYTES` | integer | `5242880` | Max invoke/check/workflow body |
| `NATS_URL` | string | — | NATS server; absent → publisher/dispatcher no-op |
| `NATS_WORKFLOW_EVENT_SUBJECT` | string | `dd.remote.workflows.events` | Workflow lifecycle event subject |
| `FIDUCIA_NODE_URL` / `FIDUCIA_BASE_URL` | string | — | Optional direct fiducia-node endpoint; `FIDUCIA_BASE_URL` is a compatibility alias |
| `FIDUCIA_NODE_INTERNAL_SECRET` / `FIDUCIA_INTERNAL_SECRET` | string (**secret**) | — | Required internal-hop secret whenever a node URL is configured; environment-only |
| `FIDUCIA_NODE_ORG_ID` | string | `fiducia-lambda-service` | Distinct `x-fiducia-org-id` namespace for lambda workflow authority |
| `FIDUCIA_SERVICE_ADDRESS` | string | — | Required reachable address registered in fiducia-node service discovery whenever coordination is configured |
| `WORKFLOW_ENGINE_ENABLED` | string | enabled | Toggle the durable workflow scheduler |
| `LAMBDA_CHILD_IDLE_MS` | integer | `300000` | Warm child idle-reap window |
| `LAMBDA_CHILD_TIMEOUT_MS` | integer | `30000` | Hard per-invocation timeout |
| `LOG_FORMAT` | string | human | `json` for structured logs |
| `LAMBDA_DATABASE_URL` | string (**secret**) | — | Postgres URL for definition loading (psql) |
| `LAMBDA_SERVER_AUTH_SECRET` | string (**secret**) | — | Shared secret on mutating routes; also `SERVER_AUTH_SECRET` / `REMOTE_DEV_SERVER_SECRET` |

### flags-2-env

Non-secret operational flags map to environment variables through the pinned
[`flags-2-env`](https://github.com/ORESoftware/flags-2-env) parser
(`vendor/flags-2-env` submodule, schema in `.cli-flags.toml`, audited in CI by
`.github/workflows/cli-flags.yml`). Database, NATS, and authentication secrets
remain environment-only so they cannot leak through process arguments:

```sh
git submodule update --init --recursive
make -C vendor/flags-2-env all
scripts/with-flags2env.sh --port 8083 --log-format json -- \
  ./target/release/fiducia-lambda-service
```

`LAMBDA_DATABASE_URL`, API-auth secrets, and the fiducia-node cluster secret are
accepted only through environment variables, never command-line flags.

## Security

- **Audit:** `cargo audit` runs without advisory exceptions. NATS uses the
  current `async-nats` TLS stack with `rustls-webpki` 0.103.x.
- **Auth:** every mutating route requires one of `X-Server-Auth` /
  `X-Lambda-Runner-Auth` / `X-Agent-Auth` matching the configured secret; the
  guard is **fail-closed** — requests are rejected when the secret is
  unconfigured or mismatched.
- **Coordination authority:** direct node calls attach both
  `x-fiducia-internal-auth` and the distinct `fiducia-lambda-service` org scope.
  Configured-node registration, idempotency, or lease failures never mint token
  `0` and never fall back to single-process execution; malformed authority
  envelopes are rejected as errors.
- **Input handling:** no `unwrap`/`panic` on request-derived input; request
  bodies are size-limited (`LAMBDA_MAX_BODY_BYTES`) and parsed fallibly. Secrets
  are never written to logs. Child stdout is read through a `MAX_RESULT_BYTES`
  (1 MiB) bounded view before it is converted into the invocation result.
- **Container identity:** the shipped image uses the audited
  `tool-runner-nonroot` profile and runs as numeric uid/gid `65532:65532`.
  Unlike the single-binary services it intentionally retains `psql` and
  `/bin/sh`; direct local container execution should use a derived image with
  the selected runner, while the default deployment dispatches through the
  remote container pool.
