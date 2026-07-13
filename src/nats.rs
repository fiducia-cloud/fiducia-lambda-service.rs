//! NATS integration. Durable lifecycle/workflow events go out over **JetStream**
//! (persistent, replayable); container-pool dispatch is a Core NATS
//! request/reply. An external NATS instance is assumed to exist; when `NATS_URL`
//! is unset every method degrades to a logged no-op so the service still runs.

use std::time::Duration;

use async_nats::jetstream;
use serde::Serialize;
use tokio::sync::OnceCell;

use crate::config::Config;
use crate::messaging::MessageEnvelope;

/// Lazily-connected NATS handle shared across the service.
pub struct Nats {
    url: Option<String>,
    client: OnceCell<Option<async_nats::Client>>,
}

impl Nats {
    pub fn new(config: &Config) -> Self {
        Nats {
            url: config.nats_url.clone(),
            client: OnceCell::new(),
        }
    }

    /// Connect once; cache the result (including a failed/absent connection as
    /// `None` so we don't reconnect on a hot path every call).
    async fn client(&self) -> Option<&async_nats::Client> {
        self.client
            .get_or_init(|| async {
                let url = self.url.clone()?;
                match async_nats::connect(&url).await {
                    Ok(c) => {
                        // NATS URLs may contain userinfo credentials; never emit
                        // the configured URL or transport error text.
                        tracing::info!("connected to NATS");
                        Some(c)
                    }
                    Err(_) => {
                        tracing::warn!("NATS connect failed; events will no-op");
                        None
                    }
                }
            })
            .await
            .as_ref()
    }

    /// Publish a durable, enveloped event to a `fiducia.<class>.<event>.v1`
    /// subject over JetStream. Best-effort: a publish/ack failure is logged, not
    /// propagated (lifecycle events must never break the request path).
    pub async fn publish_event<T: Serialize>(&self, subject: &str, envelope: &MessageEnvelope<T>) {
        let Some(client) = self.client().await else {
            return;
        };
        let bytes = match serde_json::to_vec(envelope) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "failed to serialize NATS envelope");
                return;
            }
        };
        let js = jetstream::new(client.clone());
        match js.publish(subject.to_string(), bytes.clone().into()).await {
            Ok(ack) => {
                if let Err(e) = ack.await {
                    tracing::debug!(error = %e, subject, "JetStream ack failed; falling back to core publish");
                    let _ = client.publish(subject.to_string(), bytes.into()).await;
                }
            }
            Err(e) => {
                // No stream bound to the subject (or JS disabled) — fall back to
                // Core NATS so the event still reaches live subscribers.
                tracing::debug!(error = %e, subject, "JetStream publish failed; using core publish");
                let _ = client.publish(subject.to_string(), bytes.into()).await;
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
