//! Coordination via **fiducia-node** (through the blocking `fiducia-client`).
//!
//! Per the messaging-architecture decision, NATS handles *delivery* and
//! fiducia-node handles *authority*. This module is the authority side:
//!
//!   * **service registration** — announce this runner instance so peers can
//!     discover it and detect staleness;
//!   * **idempotency** — dedup workflow starts by `idempotencyKey`;
//!   * **run leases + fencing tokens** — the workflow scheduler leases a run
//!     before advancing it, so only one replica (holding the current fencing
//!     token) ever drives a given run.
//!
//! `fiducia-client` is blocking (ureq); every call is dispatched to the blocking
//! pool so it never stalls the async runtime. When no `FIDUCIA_BASE_URL` is
//! configured the coordinator degrades to permissive in-process behaviour so a
//! single-node deployment still works.

use std::sync::Arc;

use fiducia_client::FiduciaClient;
use fiducia_interfaces as types;

/// A held run lease: the fiducia-node lock id plus its fencing token. The token
/// is monotonic — a stale holder's token is always lower than the current one,
/// so downstream mutations can reject it.
#[derive(Debug, Clone)]
pub struct RunLease {
    pub key: String,
    pub lock_id: String,
    pub fencing_token: u64,
}

/// Thin async wrapper over the blocking fiducia client.
#[derive(Clone)]
pub struct Coordinator {
    inner: Option<Arc<FiduciaClient>>,
    instance_id: String,
}

impl Coordinator {
    pub fn new(base_url: Option<&str>, instance_id: impl Into<String>) -> Self {
        Coordinator {
            inner: base_url.map(|b| Arc::new(FiduciaClient::new(b))),
            instance_id: instance_id.into(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Register this process as a `fiducia-lambda-service` instance (best-effort).
    pub async fn register_service(&self) {
        let Some(client) = self.inner.clone() else {
            return;
        };
        let instance = self.instance_id.clone();
        let res = tokio::task::spawn_blocking(move || {
            client.service_register("fiducia-lambda-service", &instance, "", 30_000, None)
        })
        .await;
        match res {
            Ok(Ok(v)) => {
                // Deserialize into the shared contract type so we fail loudly if
                // the wire shape ever drifts from fiducia-interfaces.
                if let Some(inst) = v.get("instance").cloned() {
                    match serde_json::from_value::<types::ServiceInstance>(inst) {
                        Ok(_) => {
                            tracing::info!(instance = %self.instance_id, "registered with fiducia-node")
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "service_register returned unexpected shape")
                        }
                    }
                }
            }
            Ok(Err(e)) => tracing::warn!(error = ?e, "service_register failed"),
            Err(e) => tracing::warn!(error = %e, "service_register task panicked"),
        }
    }

    /// Claim an idempotency key for a workflow start. Returns:
    ///   * `Ok(true)`  — freshly claimed, caller should do the work;
    ///   * `Ok(false)` — already claimed/completed, caller should treat as dup;
    ///   * `Err`       — coordination unavailable; caller decides fail-open.
    ///
    /// With no coordinator configured this always claims (`Ok(true)`).
    pub async fn claim_idempotency(&self, key: &str) -> Result<bool, String> {
        let Some(client) = self.inner.clone() else {
            return Ok(true);
        };
        let key = key.to_string();
        let holder = self.instance_id.clone();
        tokio::task::spawn_blocking(move || {
            match client.idempotency_claim(
                &key,
                Some(&holder),
                Some(60_000),
                None,
                serde_json::Value::Null,
            ) {
                Ok(v) => {
                    // The contract reports whether this call created the claim.
                    let claimed = v
                        .get("claimed")
                        .and_then(|c| c.as_bool())
                        .or_else(|| {
                            v.get("record")
                                .and_then(|r| r.get("status"))
                                .and_then(|s| s.as_str())
                                .map(|s| s == "claimed")
                        })
                        .unwrap_or(true);
                    Ok(claimed)
                }
                Err(e) => Err(format!("{e:?}")),
            }
        })
        .await
        .map_err(|e| e.to_string())?
    }

    /// Try to acquire the exclusive lease that authorizes advancing a run. The
    /// returned fencing token stamps any external mutation the run performs.
    /// With no coordinator, a synthetic monotonic-ish lease is granted so a
    /// single node still functions.
    pub async fn try_lease_run(&self, run_id: &str) -> Result<Option<RunLease>, String> {
        let key = format!("workflow/run/{run_id}");
        let Some(client) = self.inner.clone() else {
            return Ok(Some(RunLease {
                key: key.clone(),
                lock_id: self.instance_id.clone(),
                fencing_token: 1,
            }));
        };
        let holder = self.instance_id.clone();
        let key2 = key.clone();
        tokio::task::spawn_blocking(move || {
            // Non-blocking try-lock with a 60s lease; the scheduler re-leases on
            // each tick, so a crashed holder's lease lapses and another replica
            // reclaims the run.
            match client.try_lock(&key2, Some(&holder), Some(60_000), None) {
                Ok(v) => {
                    let out = &v["result"]["output"];
                    let acquired = out
                        .get("acquired")
                        .and_then(|a| a.as_bool())
                        .unwrap_or(true);
                    if !acquired {
                        return Ok(None);
                    }
                    let token = out
                        .get("fencing_token")
                        .and_then(|t| t.as_u64())
                        .unwrap_or(0);
                    let lock_id = out
                        .get("lock_id")
                        .and_then(|l| l.as_str())
                        .unwrap_or(&holder)
                        .to_string();
                    Ok(Some(RunLease {
                        key: key2,
                        lock_id,
                        fencing_token: token,
                    }))
                }
                Err(e) => Err(format!("{e:?}")),
            }
        })
        .await
        .map_err(|e| e.to_string())?
        .map(|opt| {
            opt.map(|mut lease| {
                lease.key = key;
                lease
            })
        })
    }

    /// Release a previously-held run lease (best-effort).
    pub async fn release_run(&self, lease: &RunLease) {
        let Some(client) = self.inner.clone() else {
            return;
        };
        let (key, lock_id, token) = (
            lease.key.clone(),
            lease.lock_id.clone(),
            lease.fencing_token,
        );
        let _ =
            tokio::task::spawn_blocking(move || client.lock_release(&key, &lock_id, token)).await;
    }
}
