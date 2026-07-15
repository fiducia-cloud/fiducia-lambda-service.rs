# Lambda service source

This service combines the HTTP API, reusable child processes, a durable
workflow state machine, fiducia-node authority, and optional NATS delivery.
State and fencing remain authoritative outside NATS. New failure paths must be
logged and represented in `/metrics` rather than silently discarded.
