//! Environment-driven configuration. Mirrors the `env_get`/`env_binary`
//! helpers of the Gleam/Erlang runner: every knob is an env var with a
//! documented default, read once at boot.

use std::net::IpAddr;

/// Static server configuration read from the environment at startup.
#[derive(Debug, Clone)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    /// Max request body accepted on invoke/check/workflow endpoints.
    pub max_body_bytes: usize,
    /// Postgres URL used to load function definitions (LAMBDA_DATABASE_URL).
    pub database_url: Option<String>,
    /// Shared secret required on every mutating endpoint. Resolved from
    /// LAMBDA_SERVER_AUTH_SECRET → SERVER_AUTH_SECRET → REMOTE_DEV_SERVER_SECRET.
    pub server_auth_secret: Option<String>,
    /// NATS connection URL. Absent → the publisher/dispatcher no-op.
    pub nats_url: Option<String>,
    /// Subject workflow lifecycle events are published to.
    pub workflow_event_subject: String,
    /// Optional fiducia coordination endpoint (edge / load balancer). When set,
    /// this instance registers as a service and uses idempotency claims.
    pub fiducia_base_url: Option<String>,
    /// Default idle window before a warm child process is reaped (ms).
    pub child_idle_ms: u64,
    /// Default hard per-invocation timeout (ms).
    pub child_timeout_ms: u64,
}

fn env_opt(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

fn env_or(key: &str, default: &str) -> String {
    env_opt(key).unwrap_or_else(|| default.to_string())
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    env_opt(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        // Auth secret resolution order matches server_auth_secret/0 in
        // http_server.gleam.
        let server_auth_secret = env_opt("LAMBDA_SERVER_AUTH_SECRET")
            .or_else(|| env_opt("SERVER_AUTH_SECRET"))
            .or_else(|| env_opt("REMOTE_DEV_SERVER_SECRET"));

        let host = env_or("HOST", "0.0.0.0")
            .parse()
            .unwrap_or_else(|_| "0.0.0.0".parse().unwrap());

        let port = {
            let p: i64 = env_num("PORT", 8083);
            if (1..=65535).contains(&p) {
                p as u16
            } else {
                8083
            }
        };

        Config {
            host,
            port,
            max_body_bytes: env_num("LAMBDA_MAX_BODY_BYTES", 5_242_880),
            database_url: env_opt("LAMBDA_DATABASE_URL"),
            server_auth_secret,
            nats_url: env_opt("NATS_URL"),
            workflow_event_subject: env_or(
                "NATS_WORKFLOW_EVENT_SUBJECT",
                "dd.remote.workflows.events",
            ),
            fiducia_base_url: env_opt("FIDUCIA_BASE_URL").or_else(|| env_opt("FIDUCIA_EDGE_URL")),
            child_idle_ms: env_num("LAMBDA_CHILD_IDLE_MS", 300_000),
            child_timeout_ms: env_num("LAMBDA_CHILD_TIMEOUT_MS", 30_000),
        }
    }

    pub fn server_auth_configured(&self) -> bool {
        self.server_auth_secret.is_some()
    }
}

/// The default host command used when a definition does not resolve its own.
/// Byte-for-byte the `default_command` constant from `http_server.gleam`.
pub const DEFAULT_NODEJS_HOST_COMMAND: &str = "env -i PATH=\"$PATH\" NODE_ENV=production NODE_NO_WARNINGS=1 NATS_URL=\"${NATS_URL:-}\" CONTAINER_POOL_NATS_URL=\"${CONTAINER_POOL_NATS_URL:-}\" CONTAINER_POOL_NATS_SUBJECT_PREFIX=\"${CONTAINER_POOL_NATS_SUBJECT_PREFIX:-dd.remote.container_pool}\" CONTAINER_POOL_NATS_TIMEOUT_MS=\"${CONTAINER_POOL_NATS_TIMEOUT_MS:-30000}\" node --permission --allow-net child-runtimes/js-function-runner.mjs";
