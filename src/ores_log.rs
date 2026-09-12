//! Process-wide `next-loggers/v1` logger for call-site trace instrumentation.
//!
//! Records carry a static `ores-trace-*` literal per call site and the
//! `ores-routine-*` constant of the enclosing function, plus static outcome
//! messages only. Never pass NATS URLs (they may carry userinfo credentials),
//! subjects, run, tenant or instance identifiers, payloads, tokens, or
//! transport and coordination error text. The existing `tracing` events remain
//! the detailed record.

use std::sync::OnceLock;

use next_loggers::{json, JsonObject, LogLevel, Logger, Options};

static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Shared logger, constructed on first use so no startup ordering is required.
pub fn logger() -> &'static Logger {
    LOGGER.get_or_init(|| Logger::new(options()))
}

#[allow(clippy::needless_update)] // The canonical package may add Options fields.
fn options() -> Options {
    let mut fields = JsonObject::new();
    fields.insert("service.namespace".into(), json!("fiducia-cloud"));
    Options {
        app_name: "fiducia-lambda-service".into(),
        name: Some("runtime".into()),
        runtime: "rust".into(),
        max_level: LogLevel::Info,
        fields,
        console: true,
        ..Options::default()
    }
}
