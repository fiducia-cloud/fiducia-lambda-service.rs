# src

The lambda service: HTTP API (`http.rs`), workflow engine + durable store
(`workflow/`), child-process/pool execution (`child_runner.rs`, `runtime.rs`),
NATS publishing with dedup + core-fallback (`nats.rs`, `messaging.rs` — the
enveloped event contract), config (`config.rs`), Prometheus metrics
(`metrics.rs`), and coordination hooks (`coord.rs`). Logging/tracing comes from
the shared `fiducia-telemetry` init in `main.rs`.
