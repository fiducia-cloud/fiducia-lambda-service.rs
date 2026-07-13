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
//! pool so it never stalls the async runtime. An absent node URL is an explicit
//! single-process mode; once a node is configured, credentials and every
//! authority decision fail closed.

use std::{sync::Arc, time::Duration};

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
    service_address: String,
}

impl Coordinator {
    pub fn new(
        base_url: Option<&str>,
        internal_secret: Option<&str>,
        org_id: &str,
        service_address: Option<&str>,
        instance_id: impl Into<String>,
    ) -> Result<Self, String> {
        let (inner, service_address) = match base_url {
            None => (None, String::new()),
            Some(base_url) => {
                let secret = internal_secret
                    .map(str::trim)
                    .filter(|secret| !secret.is_empty())
                    .ok_or_else(|| {
                        "fiducia-node coordination requires an internal secret".to_string()
                    })?;
                let org = org_id.trim();
                if org.is_empty()
                    || org.len() > 128
                    || org
                        .chars()
                        .any(|character| character.is_whitespace() || character.is_control())
                {
                    return Err("fiducia-node coordination requires a valid org scope".to_string());
                }
                let mut client = FiduciaClient::internal(base_url.trim(), secret, org);
                client.request_timeout = Some(Duration::from_secs(5));
                let service_address = service_address
                    .map(str::trim)
                    .filter(|address| !address.is_empty())
                    .ok_or_else(|| {
                        "fiducia-node coordination requires a service address".to_string()
                    })?
                    .to_string();
                (Some(Arc::new(client)), service_address)
            }
        };
        Ok(Coordinator {
            inner,
            instance_id: instance_id.into(),
            service_address,
        })
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Register this process before serving. A configured node is authoritative,
    /// so transport, authentication, and response-contract errors abort startup.
    pub async fn register_service(&self) -> Result<(), String> {
        let Some(client) = self.inner.clone() else {
            return Ok(());
        };
        let instance = self.instance_id.clone();
        let address = self.service_address.clone();
        let response = tokio::task::spawn_blocking(move || {
            client.service_register("fiducia-lambda-service", &instance, &address, 30_000, None)
        })
        .await
        .map_err(|error| format!("service registration task failed: {error}"))?
        .map_err(|error| format!("service registration request failed: {error:?}"))?;
        let registered = response
            .pointer("/result/output/registered")
            .and_then(|value| value.as_bool())
            .ok_or_else(|| "service registration response omitted registered".to_string())?;
        if !registered {
            return Err("fiducia-node rejected service registration".to_string());
        }
        let instance = response
            .pointer("/result/output/instance")
            .cloned()
            .ok_or_else(|| "service registration response omitted instance".to_string())?;
        serde_json::from_value::<types::ServiceInstance>(instance)
            .map_err(|error| format!("invalid service registration response: {error}"))?;
        tracing::info!(instance = %self.instance_id, "registered with fiducia-node");
        Ok(())
    }

    /// Claim an idempotency key for a workflow start. Returns:
    ///   * `Ok(true)`  — freshly claimed, caller should do the work;
    ///   * `Ok(false)` — already claimed/completed, caller should treat as dup;
    ///   * `Err`       — coordination unavailable or malformed; caller must stop.
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
                Ok(v) => parse_idempotency_claim(&v),
                Err(e) => Err(format!("{e:?}")),
            }
        })
        .await
        .map_err(|e| e.to_string())?
    }

    /// Try to acquire the exclusive lease that authorizes advancing a run. The
    /// returned fencing token stamps any external mutation the run performs.
    /// With no coordinator, an explicit single-process mode grants a synthetic
    /// lease. A configured coordinator never falls back to this path.
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
                Ok(v) => parse_run_lease(&v, key2),
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

    /// Release a previously-held run lease and surface every configured-node
    /// failure instead of silently discarding it.
    pub async fn release_run(&self, lease: &RunLease) -> Result<(), String> {
        let Some(client) = self.inner.clone() else {
            return Ok(());
        };
        let (key, lock_id, token) = (
            lease.key.clone(),
            lease.lock_id.clone(),
            lease.fencing_token,
        );
        tokio::task::spawn_blocking(move || client.lock_release(&key, &lock_id, token))
            .await
            .map_err(|error| format!("run lease release task failed: {error}"))?
            .map_err(|error| format!("run lease release request failed: {error:?}"))?;
        Ok(())
    }
}

fn parse_idempotency_claim(response: &serde_json::Value) -> Result<bool, String> {
    response
        .pointer("/result/output/claimed")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| "idempotency response omitted result.output.claimed".to_string())
}

fn parse_run_lease(response: &serde_json::Value, key: String) -> Result<Option<RunLease>, String> {
    let output = response
        .pointer("/result/output")
        .ok_or_else(|| "run lease response omitted result.output".to_string())?;
    let acquired = output
        .get("acquired")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| "run lease response omitted acquired".to_string())?;
    if !acquired {
        return Ok(None);
    }
    let fencing_token = output
        .get("fencing_token")
        .and_then(|value| value.as_u64())
        .filter(|token| *token > 0)
        .ok_or_else(|| "acquired run lease omitted a positive fencing token".to_string())?;
    let lock_id = output
        .get("lock_id")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "acquired run lease omitted lock_id".to_string())?
        .to_string();
    Ok(Some(RunLease {
        key,
        lock_id,
        fencing_token,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    fn serve_once(
        status: &'static str,
        body: &'static str,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let count = stream.read(&mut buffer).unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4)
                .unwrap();
            let content_length = String::from_utf8_lossy(&request[..header_end])
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let count = stream.read(&mut buffer).unwrap();
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..count]);
            }
            request_tx
                .send(String::from_utf8(request).unwrap())
                .unwrap();
            write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });
        (format!("http://{address}"), request_rx, server)
    }

    #[test]
    fn configured_coordinator_requires_secret_and_valid_org() {
        assert!(Coordinator::new(
            Some("http://node"),
            None,
            "fiducia-lambda-service",
            Some("http://lambda:8083"),
            "i"
        )
        .is_err());
        assert!(Coordinator::new(
            Some("http://node"),
            Some("secret"),
            "bad org",
            Some("http://lambda:8083"),
            "i"
        )
        .is_err());
        assert!(Coordinator::new(
            Some("http://node"),
            Some("secret"),
            "fiducia-lambda-service",
            None,
            "i"
        )
        .is_err());
        assert!(Coordinator::new(None, None, "fiducia-lambda-service", None, "i").is_ok());
    }

    #[tokio::test]
    async fn registration_sends_normalized_internal_headers() {
        const BODY: &str = r#"{"committed":true,"result":{"output":{"registered":true,"instance":{"instance_id":"instance-1","address":"http://lambda-service:8083","lease_expires_ms":123,"metadata":{}}}}}"#;
        let (url, request_rx, server) = serve_once("200 OK", BODY);
        let coordinator = Coordinator::new(
            Some(&url),
            Some("  node-secret  "),
            "  fiducia-lambda-service  ",
            Some("  http://lambda-service:8083  "),
            "instance-1",
        )
        .unwrap();
        coordinator.register_service().await.unwrap();
        let request = request_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .to_ascii_lowercase();
        assert!(request.starts_with("put /v1/services/fiducia-lambda-service/instances/instance-1"));
        assert!(request.contains("x-fiducia-internal-auth: node-secret\r\n"));
        assert!(request.contains("x-fiducia-org-id: fiducia-lambda-service\r\n"));
        assert!(request.contains(r#""address":"http://lambda-service:8083""#));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn registration_propagates_node_auth_failure() {
        let (url, _request_rx, server) =
            serve_once("401 Unauthorized", r#"{"error":"unauthorized"}"#);
        let coordinator = Coordinator::new(
            Some(&url),
            Some("wrong-secret"),
            "fiducia-lambda-service",
            Some("http://lambda-service:8083"),
            "instance-1",
        )
        .unwrap();
        assert!(coordinator.register_service().await.is_err());
        server.join().unwrap();
    }

    #[test]
    fn authority_envelopes_are_parsed_strictly() {
        assert_eq!(
            parse_idempotency_claim(&json!({"result":{"output":{"claimed":true}}})),
            Ok(true)
        );
        assert_eq!(
            parse_idempotency_claim(&json!({"result":{"output":{"claimed":false}}})),
            Ok(false)
        );
        assert!(parse_idempotency_claim(&json!({"claimed":true})).is_err());

        assert!(parse_run_lease(
            &json!({"result":{"output":{"acquired":false}}}),
            "workflow/run/1".to_string()
        )
        .unwrap()
        .is_none());
        assert!(parse_run_lease(
            &json!({"result":{"output":{"acquired":true,"fencing_token":0,"lock_id":"l"}}}),
            "workflow/run/1".to_string()
        )
        .is_err());
        let lease = parse_run_lease(
            &json!({"result":{"output":{"acquired":true,"fencing_token":9,"lock_id":"lock-1"}}}),
            "workflow/run/1".to_string(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(lease.fencing_token, 9);
        assert_eq!(lease.lock_id, "lock-1");
    }
}
