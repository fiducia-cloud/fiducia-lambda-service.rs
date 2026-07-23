//! NATS integration. Durable lifecycle/workflow events go out over **JetStream**
//! (persistent, replayable); container-pool dispatch is a Core NATS
//! request/reply. An external NATS instance is assumed to exist; when `NATS_URL`
//! is unset every method degrades to a logged no-op so the service still runs.
//! Failed initial connects are retried on a bounded cadence; they are not cached
//! for the lifetime of the process. Connections require TLS for any
//! non-loopback broker (see [`tls_policy`]); credentials come from
//! `NATS_CREDS_FILE`, never the URL.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use fiducia_messaging::{tenant_scoped_dedup_id, NatsPublisher, Publisher};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::Config;
use crate::messaging::MessageEnvelope;
use crate::metrics::Metrics;

/// Lazily-connected NATS handle shared across the service.
pub struct Nats {
    url: Option<String>,
    strict_publish: bool,
    connection: Mutex<ConnectionState>,
    metrics: Arc<Metrics>,
}

#[derive(Default)]
struct ConnectionState {
    client: Option<async_nats::Client>,
    last_attempt: Option<Instant>,
}

/// Transport policy for the configured broker. TLS is mandatory for any
/// non-loopback host; a loopback development broker may stay plaintext.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsPolicy {
    Required,
    LoopbackPlaintext,
    ExplicitPlaintext,
}

/// Follow-up when JetStream does not acknowledge a durable event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FallbackDecision {
    /// Default: degrade to at-most-once Core NATS so live subscribers still
    /// receive the event.
    CoreNats,
    /// `FIDUCIA_NATS_STRICT_PUBLISH=1`: surface the failure loudly instead of
    /// silently downgrading delivery guarantees.
    SurfaceError,
}

fn fallback_after_unacked(strict_publish: bool) -> FallbackDecision {
    if strict_publish {
        FallbackDecision::SurfaceError
    } else {
        FallbackDecision::CoreNats
    }
}

/// `1`/`true` (trimmed, case-insensitive) enables a boolean environment flag.
fn flag_enabled(value: Option<&str>) -> bool {
    value
        .map(str::trim)
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn env_flag(key: &str) -> bool {
    flag_enabled(std::env::var(key).ok().as_deref())
}

/// Host portion of one server URL. Hand-rolled so userinfo credentials never
/// pass through a parser whose errors might echo them.
fn url_host(server: &str) -> &str {
    let rest = server.split_once("://").map_or(server, |(_, rest)| rest);
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let rest = rest.split(['/', '?']).next().unwrap_or(rest);
    if let Some(bracketed) = rest.strip_prefix('[') {
        return bracketed.split(']').next().unwrap_or(bracketed);
    }
    match rest.split_once(':') {
        // A non-numeric remainder means an unbracketed IPv6 literal, not a port.
        Some((host, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => rest,
    }
}

fn host_is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Pure policy decision, split out for unit testing. A comma-separated server
/// list is plaintext-eligible only when every host is loopback.
/// `FIDUCIA_NATS_REQUIRE_TLS=1` forces TLS everywhere and wins over
/// `FIDUCIA_NATS_ALLOW_PLAINTEXT=1`, the loudly-logged explicit opt-out.
fn tls_policy(url: &str, require_tls: bool, allow_plaintext: bool) -> TlsPolicy {
    if require_tls {
        return TlsPolicy::Required;
    }
    if url
        .split(',')
        .all(|server| host_is_loopback(url_host(server.trim())))
    {
        return TlsPolicy::LoopbackPlaintext;
    }
    if allow_plaintext {
        return TlsPolicy::ExplicitPlaintext;
    }
    TlsPolicy::Required
}

/// Fixed label per connection event. Some `Event` variants embed
/// server-provided text in their `Display`, so never format the event itself.
fn connection_event_label(event: &async_nats::Event) -> &'static str {
    match event {
        async_nats::Event::Connected => "connected",
        async_nats::Event::Disconnected => "disconnected",
        async_nats::Event::LameDuckMode => "lame_duck_mode",
        async_nats::Event::Draining => "draining",
        async_nats::Event::Closed => "closed",
        async_nats::Event::SlowConsumer(_) => "slow_consumer",
        async_nats::Event::ServerError(_) => "server_error",
        async_nats::Event::ClientError(_) => "client_error",
    }
}

/// Connect with the TLS policy applied and connection-state callbacks so
/// operators can see flaps. Credentials come from `NATS_CREDS_FILE` when set
/// so they do not have to ride the URL userinfo.
async fn connect_to_nats(url: &str) -> io::Result<async_nats::Client> {
    let policy = tls_policy(
        url,
        env_flag("FIDUCIA_NATS_REQUIRE_TLS"),
        env_flag("FIDUCIA_NATS_ALLOW_PLAINTEXT"),
    );
    if policy == TlsPolicy::ExplicitPlaintext {
        tracing::warn!(
            "FIDUCIA_NATS_ALLOW_PLAINTEXT=1: NATS traffic to a non-loopback broker is UNENCRYPTED"
        );
    }
    let mut options = async_nats::ConnectOptions::new().require_tls(policy == TlsPolicy::Required);
    if let Some(path) = std::env::var_os("NATS_CREDS_FILE") {
        options = options.credentials_file(path).await?;
    }
    options = options.event_callback(|event| async move {
        let state = connection_event_label(&event);
        match event {
            async_nats::Event::Connected => tracing::info!(state, "NATS connection state changed"),
            _ => tracing::warn!(state, "NATS connection state changed"),
        }
    });
    options.connect(url).await.map_err(io::Error::other)
}

impl Nats {
    pub fn new(config: &Config, metrics: Arc<Metrics>) -> Self {
        Nats {
            url: config.nats_url.clone(),
            strict_publish: env_flag("FIDUCIA_NATS_STRICT_PUBLISH"),
            connection: Mutex::new(ConnectionState::default()),
            metrics,
        }
    }

    async fn client(&self) -> Option<async_nats::Client> {
        let url = self.url.as_ref()?;
        let mut connection = self.connection.lock().await;
        if let Some(client) = connection.client.as_ref() {
            return Some(client.clone());
        }
        if connection
            .last_attempt
            .is_some_and(|attempt| attempt.elapsed() < Duration::from_secs(5))
        {
            return None;
        }

        connection.last_attempt = Some(Instant::now());
        self.metrics.nats_connect_attempts_total(1);
        match connect_to_nats(url).await {
            Ok(client) => {
                tracing::info!("connected to NATS");
                connection.client = Some(client.clone());
                Some(client)
            }
            Err(_) => {
                self.metrics.nats_connect_failures_total(1);
                // NATS URLs may contain userinfo credentials; never emit the
                // configured URL or transport error text.
                tracing::warn!(
                    retry_after_seconds = 5,
                    "NATS connect failed; delivery is degraded"
                );
                None
            }
        }
    }

    async fn invalidate_client(&self) {
        self.connection.lock().await.client = None;
    }

    /// Publish a durable, enveloped event to a `fiducia.<class>.<event>.v1`
    /// subject over JetStream. Best-effort: a publish/ack failure is logged, not
    /// propagated (lifecycle events must never break the request path). With
    /// `FIDUCIA_NATS_STRICT_PUBLISH=1` an unacknowledged JetStream publish is
    /// an error — never quietly downgraded to Core NATS.
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        let Some(client) = self.client().await else {
            if self.url.is_some() {
                self.metrics.nats_unavailable_drops_total(1);
            } else {
                self.metrics.nats_unconfigured_skips_total(1);
            }
            return;
        };
        let bytes = match envelope.encode() {
            Ok(b) => b,
            Err(e) => {
                self.metrics.nats_serialization_failures_total(1);
                tracing::warn!(error = %e, "failed to serialize NATS envelope");
                return;
            }
        };
        let dedup_id = tenant_scoped_dedup_id(envelope.tenant_id, &envelope.idempotency_key);
        let publisher = NatsPublisher::new(jetstream::new(client.clone()));
        if publisher.publish(subject, &dedup_id, &bytes).await.is_ok() {
            self.metrics.nats_event_published_total(1);
            return;
        }

        if fallback_after_unacked(self.strict_publish) == FallbackDecision::SurfaceError {
            self.metrics.nats_publish_failures_total(1);
            tracing::error!(
                subject,
                "JetStream publish was not acknowledged; strict mode forbids the Core NATS fallback"
            );
            self.invalidate_client().await;
            return;
        }

        // No stream bound to the subject (or JS disabled) — fall back to Core
        // NATS so live subscribers still receive the event. Dashboards can tell
        // a degraded delivery (`nats_core_fallback_total`) from a dropped one
        // (`nats_publish_failures_total`).
        tracing::warn!(
            subject,
            "JetStream publish was not acknowledged; using Core NATS fallback"
        );
        match client.publish(subject.to_string(), bytes.into()).await {
            Ok(()) => self.metrics.nats_core_fallback_total(1),
            Err(_) => {
                self.metrics.nats_publish_failures_total(1);
                tracing::error!(
                    subject,
                    "Core NATS fallback publish failed; event was not delivered"
                );
                self.invalidate_client().await;
            }
        }
    }

    /// Core NATS request/reply to dd-container-pool: lease a warm worker, post
    /// the invocation envelope, return the worker's response body. Errors carry a
    /// human-readable reason so the caller can decide on local fallback.
    pub async fn pool_dispatch(
        &self,
        subject: &str,
        pool_slug: &str,
        identifier: &str,
        payload: &str,
        timeout_ms: u64,
    ) -> Result<String, String> {
        let client = self.client().await.ok_or_else(|| {
            if self.url.is_some() {
                "NATS is temporarily unavailable".to_string()
            } else {
                "NATS is not configured".to_string()
            }
        })?;

        // Envelope carries routing metadata; the request payload is the lambda
        // invocation body the pooled worker expects.
        let request = serde_json::json!({
            "poolSlug": pool_slug,
            "identifier": identifier,
            "payload": payload,
        });
        let body = serde_json::to_vec(&request).map_err(|e| e.to_string())?;

        let fut = client.request(subject.to_string(), body.into());
        match tokio::time::timeout(Duration::from_millis(timeout_ms.max(1000)), fut).await {
            Ok(Ok(msg)) => String::from_utf8(msg.payload.to_vec())
                .map_err(|_| "pool response was not utf8".to_string()),
            // Keep broker endpoint/credential details out of errors that may be
            // returned through HTTP or logged by a caller.
            Ok(Err(_)) => Err("pool request failed".into()),
            Err(_) => Err("pool dispatch timed out".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn unconfigured_nats_is_observable_and_pool_dispatch_is_explicit() {
        let metrics = Arc::new(Metrics::default());
        let nats = Nats {
            url: None,
            strict_publish: false,
            connection: Mutex::new(ConnectionState::default()),
            metrics: metrics.clone(),
        };
        let envelope = MessageEnvelope::new(
            "workflow.completed",
            serde_json::json!({ "workflow_id": "wf-1" }),
            "workflow:wf-1:completed",
        );

        nats.publish_event("fiducia.workflows.completed.v1", &envelope)
            .await;
        let error = nats
            .pool_dispatch("pool.invoke", "js", "fn-1", "{}", 5)
            .await
            .unwrap_err();

        assert_eq!(error, "NATS is not configured");
        assert_eq!(
            metrics
                .nats_unconfigured_skips_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics.nats_connect_attempts_total.load(Ordering::Relaxed),
            0,
            "an intentionally absent broker must not cause reconnect churn"
        );
    }

    #[test]
    fn tls_is_required_for_any_non_loopback_host() {
        assert_eq!(
            tls_policy("nats://broker.internal:4222", false, false),
            TlsPolicy::Required
        );
        assert_eq!(
            tls_policy("nats://user:pass@broker.internal:4222", false, false),
            TlsPolicy::Required
        );
        // One remote host in a server list taints the whole list.
        assert_eq!(
            tls_policy(
                "nats://127.0.0.1:4222,nats://broker.internal:4222",
                false,
                false
            ),
            TlsPolicy::Required
        );
    }

    #[test]
    fn loopback_hosts_may_stay_plaintext() {
        for url in [
            "nats://localhost:4222",
            "nats://127.0.0.1:4222",
            "nats://[::1]:4222",
            "localhost",
            "nats://user:pass@127.0.0.1:4222/",
        ] {
            assert_eq!(
                tls_policy(url, false, false),
                TlsPolicy::LoopbackPlaintext,
                "{url}"
            );
        }
    }

    #[test]
    fn environment_overrides_follow_the_documented_precedence() {
        // FIDUCIA_NATS_REQUIRE_TLS wins over everything, including loopback.
        assert_eq!(
            tls_policy("nats://localhost:4222", true, true),
            TlsPolicy::Required
        );
        // FIDUCIA_NATS_ALLOW_PLAINTEXT is an explicit opt-out for remote hosts.
        assert_eq!(
            tls_policy("nats://broker.internal:4222", false, true),
            TlsPolicy::ExplicitPlaintext
        );
        // Loopback plaintext needs no opt-out and is not the loud variant.
        assert_eq!(
            tls_policy("nats://localhost:4222", false, true),
            TlsPolicy::LoopbackPlaintext
        );
    }

    #[test]
    fn url_host_extraction_ignores_userinfo_ports_and_paths() {
        assert_eq!(
            url_host("nats://user:pass@broker.internal:4222/x?y"),
            "broker.internal"
        );
        assert_eq!(url_host("nats://[::1]:4222"), "::1");
        assert_eq!(url_host("tls://10.0.0.7"), "10.0.0.7");
        assert_eq!(url_host("localhost:4222"), "localhost");
        assert!(host_is_loopback("127.0.0.53"));
        assert!(host_is_loopback("LOCALHOST"));
        assert!(!host_is_loopback("broker.internal"));
    }

    #[test]
    fn environment_flags_accept_only_explicit_truthy_values() {
        assert!(flag_enabled(Some("1")));
        assert!(flag_enabled(Some("true")));
        assert!(flag_enabled(Some(" 1 ")));
        assert!(!flag_enabled(Some("0")));
        assert!(!flag_enabled(Some("")));
        assert!(!flag_enabled(Some("yes")));
        assert!(!flag_enabled(None));
    }

    #[test]
    fn strict_publish_mode_disables_the_core_nats_fallback() {
        assert_eq!(fallback_after_unacked(false), FallbackDecision::CoreNats);
        assert_eq!(
            fallback_after_unacked(true),
            FallbackDecision::SurfaceError,
            "FIDUCIA_NATS_STRICT_PUBLISH=1 must surface the failure, not degrade"
        );
    }
}
