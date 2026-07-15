//! NATS integration. Durable lifecycle/workflow events go out over **JetStream**
//! (persistent, replayable); container-pool dispatch is a Core NATS
//! request/reply. An external NATS instance is assumed to exist; when `NATS_URL`
//! is unset every method degrades to a logged no-op so the service still runs.
//! Failed initial connects are retried on a bounded cadence; they are not cached
//! for the lifetime of the process.

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
    connection: Mutex<ConnectionState>,
    metrics: Arc<Metrics>,
}

#[derive(Default)]
struct ConnectionState {
    client: Option<async_nats::Client>,
    last_attempt: Option<Instant>,
}

impl Nats {
    pub fn new(config: &Config, metrics: Arc<Metrics>) -> Self {
        Nats {
            url: config.nats_url.clone(),
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
        match async_nats::connect(url).await {
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
    /// propagated (lifecycle events must never break the request path).
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

        // No stream bound to the subject (or JS disabled) — fall back to Core
        // NATS so live subscribers still receive the event.
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
