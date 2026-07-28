# Cron function control-plane contract

Customer-authenticated web applications do not call this service directly. A trusted customer or admin backend verifies organization membership, then forwards only its service credential and the canonical organization ID.

## Required headers

- `X-Server-Auth`: service-to-service credential; requests fail closed when the server secret is missing or mismatched.
- `X-Fiducia-Org-Id`: canonical tenant ID. Every definition read and mutation includes the tenant predicate in PostgreSQL.
- `traceparent` and `tracestate`: optional W3C trace context propagated by the caller.
- `Idempotency-Key`: stable cron-run delivery key on `/invoke/{function_id}`.

Browser `Cookie` and `Authorization` headers must never be forwarded.

## Definition lifecycle

1. `POST /v1/functions` creates a tenant-owned draft.
2. `PUT /v1/functions/{id}` replaces the draft after strict validation.
3. `POST /v1/functions/{id}/check` runs a bounded sandbox check and atomically activates the exact checked revision.
4. `POST /v1/functions/{id}/pause` returns the definition to draft status without deleting source.
5. `DELETE /v1/functions/{id}` soft-deletes it for that tenant.
6. `POST /invoke/{id}` loads only an active, non-deleted definition owned by the supplied tenant.

Source code remains in PostgreSQL. `fiducia-node` stores only the opaque function UUID in replicated schedule state.

## Customer-code policy

The first customer-facing release supports managed Node.js code only. It caps source at 256 KiB, rejects customer-supplied entry commands, host shell runtimes, browser runtimes, container selection, reserved tenancy metadata, excessive labels, and excessive runtime limits. Invocation output remains subject to the child runner’s one-MiB ceiling.

## Observability

CRUD, check, pause, delete, and invocation operations create OpenTelemetry spans and low-cardinality Prometheus counters. Function source, service credentials, and database URLs are never span attributes or log fields. W3C trace context links the cron delivery span in `fiducia-node` to the invocation span here.
