# fiducia-lambda-service

Function runtime + durable workflow engine for fiducia.cloud. A Rust port of the
Gleam/Erlang `gleam-lambda-runner`.

Two subsystems share one process:

- **Child runner** — runs stored functions (nodejs / Playwright / Puppeteer /
  python3 / ruby / bash / containerized runtimes) in **reusable, sandboxed
  child processes**. Warm
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
| Messaging | `fiducia-messaging` envelope; **NATS** JetStream with `Nats-Msg-Id` dedup for lifecycle events, Core request/reply for container-pool dispatch |
| Coordination (optional) | **fiducia-node** via an authenticated, tenant-scoped `fiducia-client` — run leases + fencing tokens, idempotency claims, service registration |
| Function definitions | **Postgres** via `psql` (`LAMBDA_DATABASE_URL`) |
| Shared types | `fiducia-interfaces` |
| Telemetry | `fiducia-telemetry` structured logs + optional OTLP traces; Prometheus counters at `/metrics` |

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
| `NATS_URL` | string | — | NATS server; absent → publisher/dispatcher no-op; initial failures retry every five seconds |
| `NATS_WORKFLOW_EVENT_SUBJECT` | string | `dd.remote.workflows.events` | Workflow lifecycle event subject |
| `FIDUCIA_NODE_URL` / `FIDUCIA_BASE_URL` | string | — | Optional direct fiducia-node endpoint; `FIDUCIA_BASE_URL` is a compatibility alias |
| `FIDUCIA_NODE_INTERNAL_SECRET` / `FIDUCIA_INTERNAL_SECRET` | string (**secret**) | — | Required internal-hop secret whenever a node URL is configured; environment-only |
| `FIDUCIA_NODE_ORG_ID` | string | `fiducia-lambda-service` | Distinct `x-fiducia-org-id` namespace for lambda workflow authority |
| `FIDUCIA_SERVICE_ADDRESS` | string | — | Required reachable address registered in fiducia-node service discovery whenever coordination is configured |
| `WORKFLOW_ENGINE_ENABLED` | string | enabled | Toggle the durable workflow scheduler |
| `LAMBDA_CHILD_IDLE_MS` | integer | `300000` | Warm child idle-reap window |
| `LAMBDA_CHILD_TIMEOUT_MS` | integer | `30000` | Hard per-invocation timeout |
| `LAMBDA_ALLOW_HOST_RUNTIMES` | CSV | `nodejs,playwright,puppeteer` | Runtimes permitted to execute in the host child pool; other runtimes require container-pool dispatch |
| `LAMBDA_BROWSER_ALLOW_PRIVATE_NETWORKS` | boolean | `false` | Explicit operator override for browser access to local/private targets; keep disabled outside owned test networks |
| `LAMBDA_BROWSER_ALLOWED_HOSTS` | CSV | — | Exact private/local hostnames explicitly authorized for browser access |
| `FIDUCIA_LOG_FORMAT` | string | `json` | Logging/tracing comes from the shared `fiducia-telemetry` crate; `text` for compact local logs (`OTEL_LOG_FORMAT` then legacy `LOG_FORMAT` are fallbacks) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | string | — | Optional local collector OTLP gRPC endpoint; exporter failure falls back to stdout |
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

## Reproducible build inputs

CI and the container build consume immutable, verified sibling revisions:

- `fiducia-clients` at `1446b254b4bfd57b2df75c3c451a663313f19eb9`
- `fiducia-interfaces` at `3072e824e4e10f4a392a5851ea155ab5693ff206`
- `fiducia-messaging.rs` at `cec4ea4f54162758858c6c284324c34a42f3f3d7`
- `fiducia-telemetry.rs` at `724844e62ba35f409917d72343e7804c199878a9`

The Dockerfile shallow-fetches those exact commits, verifies each detached
`HEAD`, and compiles with `Cargo.lock`. Update both the CI checkout and matching
Docker build argument together whenever a shared dependency changes.

## Security

### Browser automation and ethical scraping

Playwright and Puppeteer browser automation is a normal, safe, and ethical
engineering capability when it is used on resources you own or are authorized
to access, within published terms and rate limits. This project policy does not
grant permission to scrape a third-party service and is not legal advice.

Prefer a documented API when it provides the needed data. For authorized
browser work, identify and rate-limit the client when appropriate, cache and
minimize collection, and follow the site's terms and applicable `robots.txt`
guidance. Do not bypass authentication, paywalls, CAPTCHAs, or other access
controls, and do not collect credentials or unnecessary personal/sensitive
data. The browser child blocks local/private targets by default, rejects URL
credentials, isolates each invocation in a new browser context, closes that
context after use, and receives no database, auth, NATS, or OTLP secrets.

Use runtime `playwright` or `puppeteer`; the function body receives `request`,
`context`, `page`, `browser`, and a stderr-only `console`. Both engines use the
same image-pinned Chromium build. Compile-only checks avoid launching a browser,
while normal invocations reuse the warm browser process and isolate page state
per invocation.

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
- **Visible delivery degradation:** the standard message envelope supplies the
  producer identity and idempotency key, JetStream publishes carry a dedup
  header, and reconnect, serialization, fallback, and final publish outcomes are
  exposed as structured logs and Prometheus counters. These delivery paths never
  replace fiducia-node leases or the durable workflow store.
