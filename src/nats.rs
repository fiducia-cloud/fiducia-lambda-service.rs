//! NATS integration. Durable lifecycle/workflow events go out over **JetStream**
//! (persistent, replayable); container-pool dispatch is a Core NATS
//! request/reply. An external NATS instance is assumed to exist; when `NATS_URL`
//! is unset every method degrades to a logged no-op so the service still runs.
//!
//! Reliability contract:
//!   * the connection self-heals — `retry_on_initial_connect` keeps dialing in
//!     the background, and an outright construction failure is retried on the
//!     next call instead of being cached for the process lifetime;
//!   * every JetStream publish carries a tenant-scoped `Nats-Msg-Id`, so an
//!     ack-timeout retry or crash-window republish is deduplicated server-side
//!     within the stream's dedup window;
//!   * a durability downgrade (JetStream → Core fallback) is logged at `warn`,
//!     and a fallback that ALSO fails is logged too — an event can no longer
//!     vanish without a trace.
//!
//! NATS URLs may contain userinfo credentials; transport error text can echo
//! them, so connection-failure logs name the failure class and never the body.

use std::time::Duration;

use async_nats::jetstream;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::messaging::MessageEnvelope;

/// Upper bound on waiting for the JetStream publish acknowledgement before
/// treating the publish as failed and taking the fallback path.
const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Lazily-connected NATS handle shared across the service.
pub struct Nats {
    url: Option<String>,
    client: Mutex<Option<async_nats::Client>>,
}

impl Nats {
    pub fn new(config: &Config) -> Self {
        Nats {
            url: config.nats_url.clone(),
            client: Mutex::new(None),
        }
    }

    /// The shared client, (re)connecting if none is cached. Once constructed,
    /// async-nats reconnects internally, so the cached client stays valid across
    /// broker restarts; if construction itself fails, the next call retries
    /// instead of inheriting a permanently-dead publisher.
    async fn client(&self) -> Option<async_nats::Client> {
        let url = self.url.as_ref()?;
        let mut cached = self.client.lock().await;
        if let Some(client) = cached.as_ref() {
            return Some(client.clone());
        }
        match async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(url)
            .await
        {
            Ok(client) => {
                tracing::info!("connected to NATS");
                *cached = Some(client.clone());
                Some(client)
            }
            Err(_) => {
                tracing::warn!("NATS client construction failed; will retry on the next call");
                None
            }
        }
    }

    /// Publish a durable, enveloped event to a `fiducia.<class>.<event>.v1`
    /// subject over JetStream. Best-effort: a publish/ack failure is logged, not
    /// propagated (lifecycle events must never break the request path).
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        let Some(client) = self.client().await else {
            return; // NATS_URL unset (documented no-op) or construction failed (logged)
        };
        let bytes = match serde_json::to_vec(envelope) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, subject, "failed to serialize NATS envelope; event dropped");
                return;
            }
        };

        // Tenant-scoped idempotent publish: JetStream drops a duplicate
        // `Nats-Msg-Id` within the stream's dedup window, so the ack-timeout
        // retry below (and any crash-window republish) collapses to one stored
        // message. Scoped by tenant so two tenants reusing the same business
        // key can never suppress each other's events.
        let dedup_id = format!(
            "{}:{}",
            envelope
                .tenant_id
                .map(|t| t.to_string())
                .unwrap_or_else(|| "global".to_string()),
            envelope.idempotency_key
        );
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", dedup_id.as_str());

        let js = jetstream::new(client.clone());
        let attempt = async {
            js.publish_with_headers(subject.to_string(), headers, bytes.clone().into())
                .await?
                .await
        };
        let failure_class = match tokio::time::timeout(ACK_TIMEOUT, attempt).await {
            Ok(Ok(_ack)) => return, // durably stored by the broker
            Ok(Err(_)) => "jetstream publish/ack failed (no stream bound?)",
            Err(_) => "jetstream ack timed out",
        };

        // Durability downgrade: deliver at-most-once over Core NATS so the event
        // still reaches live subscribers — but never silently.
        tracing::warn!(
            subject,
            message_id = %envelope.message_id,
            failure_class,
            "durable JetStream publish failed; falling back to core NATS (at-most-once)"
        );
        if client
            .publish(subject.to_string(), bytes.into())
            .await
            .is_err()
        {
            tracing::warn!(
                subject,
                message_id = %envelope.message_id,
                "core NATS fallback publish also failed; event dropped"
            );
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
        let client = self
            .client()
            .await
            .ok_or_else(|| "NATS is not configured".to_string())?;

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
            Ok(Err(e)) => Err(format!("pool request failed: {e}")),
            Err(_) => Err("pool dispatch timed out".into()),
        }
    }
}
